"""Warm DuckDB query server for the CDC query benchmark (Deployment
duckdb-warm-cdc, deployed by deploy-duckdb-cdc.sh, driven by
cdc-pipeline/querybench/duckdb-warm.sh).

A copy of throughput-benchmark/engine-deploy/duckdb_warm_server.py (same
settings, cache parity, timing and ZERO_SCAN guard), changed only where the CDC
tables need it:
  - the client sends FULLY rendered SQL: four tables per arm, each an
    iceberg_scan() of its own pinned metadata.json, so the server resolves no
    ${TABLE}, has no /pin, and needs no table names in its environment;
  - a finished job returns every result row (`rows`, up to MAX_RESULT_ROWS,
    fields |-joined, NULL as "NULL", like StarRocks' mysql -N -B), so the
    summary can compare all rows across arms, not only the first;
  - /health reports `started_at`, so the client can tell that the server was
    restarted under a query (an OOM kill on uncompacted equality deletes:
    the container restarts and its memory caches are gone).
DuckDB 2.0 (2026-11-02; DUCKDB_IMAGE, the source-built image, or the PyPI
wheel): extensions that are STATICALLY_LINKED (the image links iceberg, httpfs,
aws, avro) or already installed (the image's cache_httpfs) are LOADed without
an INSTALL, so the pod needs no extension download; profile_query reads
EXPLAIN (ANALYZE, FORMAT JSON) (2.0 draws the text plan in boxes, "Table Scan",
which the 1.5 regex no longer matched); results stream, so execute and fetch
stay under one try and one timer (attempt). Python >= 3.11.
Iceberg v3 (ICEBERG_FORMAT_VERSION=3): a query of an arm in
SKIP_PUFFIN_VERIFICATION_ARMS (default: paimon on v3, none on v2) runs with
iceberg_unsafe_skip_puffin_verification = true, every other with false: Paimon's
Iceberg export writes its deletion-vector index files as bare blobs, not Puffin
containers, which DuckDB otherwise rejects. The setting is global, which is safe
here because one query runs at a time (the lock). The job and /health report it.
Everything below this paragraph is the throughput server's own description.

Original: Warm DuckDB query server: the DuckDB side of a warm-vs-warm comparison with
StarRocks. Runs in a long-lived pod.

One process and ONE connection for the pod's life, like a StarRocks FE/BE that is
already up: extensions, the S3 secret, Parquet footers and the cache_httpfs block
cache all survive from one query to the next. A query is timed inside this
process around execute + fetch, which is what StarRocks' audit log reports
(server-side, planning included). Start-up, the secret refresh and resolving
${TABLE} are outside the timer, as they are on StarRocks.

Cache parity with the StarRocks BEs (3 x datacache, checked 2026-10-13); both
engines run with their cache ON, as both defaults:
  disk    3 x 150 GiB = 450 GiB  ->  CACHE_MODE=on_disk (default): cache_httpfs
                                     on-disk cache, CACHE_DISK_BYTES, LRU, on a
                                     gp3-fast volume (850 MB/s)
  memory  3 x 7.34 GB = 22.03 GB ->  its read-through memory cache, CACHE_MEM_BYTES
          (12.5 GB = 3 x ~4.2 GB until 2026-10-23)
CACHE_MODE=in_mem keeps only a CACHE_MEM_BYTES memory cache (every miss reads S3).
DuckDB's own external file cache is left on (its default): it also keeps file
data in the buffer pool, within memory_limit, which StarRocks has no match for.
The cache volume must not be default gp3: at its 125 MB/s baseline cache_httpfs
served every hit from the disk, never S3, and the snapshot S3 pass ran for hours
at 130 MB/s and 0.3 cores (2026-10-17). StarRocks' BEs sit on default gp3, but
enable_datacache_io_adaptor sends reads back to S3 when the disk saturates;
cache_httpfs has no such thing, hence the faster volume on DuckDB's side.
DuckDB does not count the cache_httpfs memory against memory_limit, so
DUCKDB_MEMORY_LIMIT + CACHE_MEM_BYTES must stay under the container limit.

One query at a time (a lock), so a suite cannot overlap another on this pod.
HTTP, stdlib only; every call returns JSON:
  GET  /health            version, settings, busy, cache size
  POST /query             {suite, arm, name, sql, runs, warmup, timeout_s,
                           allow_zero_rows, leading_col_is_label} -> {id}
  GET  /job/<id>          progress, then the result
  POST /query {..., profile: true}
                          once, untimed, cache bypassed: scan_bytes / scan_rows
                          (profile_query)
  POST /clear_cache       empties the block cache (a cold start on demand)
Queries come back by polling /job, because a snapshot query can run for half an
hour and a port-forward drops idle connections long before that.
"""
import json
import re
import os
import statistics
import subprocess
import threading
import time
import traceback
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

ENV = os.environ
REGION = ENV["AWS_REGION"]
POD = ENV.get("HOSTNAME", "duckdb-warm-cdc")
MAX_RESULT_ROWS = int(ENV.get("MAX_RESULT_ROWS") or 10000)
STARTED_AT = time.time()
CACHE_MODE = ENV.get("CACHE_MODE", "on_disk")
CACHE_DIR = ENV["CACHE_DIR"]
CACHE_DISK_BYTES = int(ENV["CACHE_DISK_BYTES"])
CACHE_MEM_BYTES = int(ENV["CACHE_MEM_BYTES"])
# cache_httpfs_cache_block_size, CACHE_BLOCK_BYTES: 256 KiB since 2026-10-25, the same
# as StarRocks' datacache_block_size (4.1.4 default, read from the BEs' /varz). Its own
# default, 512 KiB, made the narrow snapshot queries fetch 5.7x the bytes they need
# (S1: 87.4 vs 15.2 GB) and read them back at the cache disk's 850 MB/s: S1 100 s cached
# (29 s at 64 KiB, which was faster still but finer than StarRocks' blocks).
BLOCK = int(ENV.get("CACHE_BLOCK_BYTES") or 262144)
SECRET_TTL_S = 20 * 60                      # IRSA sessions last ~1h
ICEBERG_FORMAT_VERSION = int(ENV.get("ICEBERG_FORMAT_VERSION") or 3)
_skip = ENV.get("SKIP_PUFFIN_VERIFICATION_ARMS")
SKIP_PUFFIN_ARMS = set((_skip if _skip is not None else ("paimon" if ICEBERG_FORMAT_VERSION >= 3 else "")).split())
DUCKDB_MAJOR = int(duckdb.__version__.split(".")[0])

con = duckdb.connect()
lock = threading.Lock()                     # one query at a time
jobs = {}
secret_at = 0.0


def run(cmd):
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def load_extensions():
    """LOAD httpfs, iceberg, cache_httpfs. INSTALL only what is neither linked
    into the build nor installed already: on the source-built image (DUCKDB_IMAGE)
    iceberg/httpfs/aws/avro are STATICALLY_LINKED and cache_httpfs is installed
    at build time, so nothing is fetched; on the pip fallback they download
    from extensions.duckdb.org / community as before. Returns {name: how}."""
    mode = {n: (m, inst) for n, m, inst in con.execute(
        "SELECT extension_name, install_mode, installed FROM duckdb_extensions()").fetchall()}
    how = {}
    for ext, repo in (("httpfs", ""), ("iceberg", ""), ("cache_httpfs", " FROM community")):
        m, inst = mode.get(ext, (None, False))
        if m == "STATICALLY_LINKED":
            how[ext] = "static"
        elif inst:
            how[ext] = "preinstalled"
        else:
            con.execute("INSTALL %s%s" % (ext, repo))
            how[ext] = "installed at start"
        con.execute("LOAD " + ext)
    return how


def init():
    if CACHE_MODE == "in_mem":
        cache = [
            "SET cache_httpfs_type='in_mem'",
            "SET cache_httpfs_cache_block_size=%d" % BLOCK,
            "SET cache_httpfs_max_in_mem_cache_block_count=%d" % (CACHE_MEM_BYTES // BLOCK),
            # The default 1 h expiry would drop a warm-up pass's blocks before a
            # long snapshot suite reused them; StarRocks' cache has no TTL.
            "SET cache_httpfs_in_mem_cache_block_timeout_millisec=%d" % (7 * 86400 * 1000),
        ]
        info = {"cache_mode": CACHE_MODE}
    elif CACHE_MODE == "on_disk":
        st = os.statvfs(CACHE_DIR)
        vol = st.f_frsize * st.f_blocks
        # cache_httpfs has no size cap, only a floor of free space below which it
        # evicts (LRU), so the cap is "volume minus the floor".
        floor = max(vol - CACHE_DISK_BYTES, 1 << 30)
        cache = [
            "SET cache_httpfs_type='on_disk'",
            "SET cache_httpfs_cache_block_size=%d" % BLOCK,
            "SET cache_httpfs_cache_directory='%s'" % CACHE_DIR,
            "SET cache_httpfs_min_disk_bytes_for_cache=%d" % floor,
            "SET cache_httpfs_evict_policy='lru_sp'",
            "SET cache_httpfs_disk_cache_reader_enable_memory_cache=true",
            "SET cache_httpfs_disk_cache_reader_mem_cache_block_count=%d" % (CACHE_MEM_BYTES // BLOCK),
        ]
        info = {"cache_mode": CACHE_MODE, "volume_bytes": vol, "free_floor_bytes": floor,
                "cache_disk_cap_bytes": CACHE_DISK_BYTES}
    else:
        raise SystemExit("CACHE_MODE must be in_mem|on_disk, got %r" % CACHE_MODE)
    for sql in [
        "SET memory_limit='%s'" % ENV["DUCKDB_MEMORY_LIMIT"],
        "SET threads=%s" % ENV["DUCKDB_THREADS"],
        "SET temp_directory='%s'" % ENV["SPILL_DIR"],
        "SET max_temp_directory_size='%s'" % ENV["DUCKDB_MAX_TEMP_SIZE"],
    ]:
        con.execute(sql)
    info["extensions"] = load_extensions()
    for sql in cache + [                           # before the first file access, or they do not take effect
        # StarRocks caches Parquet footers and object metadata too.
        "SET parquet_metadata_cache=true",
        "SET enable_http_metadata_cache=true",
        # enable_external_file_cache is left at DuckDB's default (on since 1.3):
        # remote file data is also kept in the buffer pool, within memory_limit
        # (82.08 GiB), a memory cache in front of cache_httpfs's CACHE_MEM_BYTES.
        # From 2026-10-20 to 2026-10-23 it was off, so that DuckDB held no more
        # data in memory than StarRocks' datacache memory (then 3 x ~4.2 GB); runs since then
        # can hold more, and are not comparable with those on that point.
    ]:
        con.execute(sql)
    return info


def refresh_secret(force=False):
    """IRSA -> static S3 secret; DuckDB's credential_chain cannot use IRSA here."""
    global secret_at
    if not force and time.time() - secret_at < SECRET_TTL_S:
        return
    with open(ENV["AWS_WEB_IDENTITY_TOKEN_FILE"]) as f:
        tok = f.read().strip()
    out = run(["aws", "sts", "assume-role-with-web-identity", "--role-arn", ENV["AWS_ROLE_ARN"],
               "--role-session-name", "duckdb-warm", "--web-identity-token", tok,
               "--query", "Credentials.[AccessKeyId,SecretAccessKey,SessionToken]", "--output", "text"])
    ak, sk, st = out.split()
    con.execute("CREATE OR REPLACE SECRET irsa (TYPE s3, KEY_ID '%s', SECRET '%s', SESSION_TOKEN '%s', REGION '%s')"
                % (ak, sk, st, REGION))
    secret_at = time.time()


def checked_sql(q):
    """The client renders every placeholder (lib/querybench.sh qb_render); one
    left here would bind as an identifier or a string and return a wrong answer."""
    sql = q["sql"]
    if "${" in sql:
        raise RuntimeError("unresolved placeholder in the SQL")
    return sql


def row_text(r):
    return "|".join("NULL" if v is None else str(v) for v in r)


def set_puffin(cur, arm):
    """iceberg_unsafe_skip_puffin_verification for this query's arm (see the
    module docstring). Set only when it matters (v3, or the arm is listed), so a
    1.x server, which has no such setting, still runs v2. Returns the value."""
    want = arm in SKIP_PUFFIN_ARMS
    if want or ICEBERG_FORMAT_VERSION >= 3:
        cur.execute("SET iceberg_unsafe_skip_puffin_verification = %s" % ("true" if want else "false"))
    return want


def attempt(sql, timeout_s, arm=None):
    """One timed execution. Returns (status, ms, rows). A timer interrupts it.

    On its OWN cursor: a DuckDB connection is not safe to share between threads.
    Sharing `con` with the /health thread (2026-10-18) let one thread fetch the
    other's result: a COUNT(*) came back as the cache size (245,022,069,168) or
    as no row at all (ZERO_SCAN). A cursor shares the database (settings,
    secrets, the cache) but has its own result."""
    cur = con.cursor()
    set_puffin(cur, arm)                    # untimed, like the secret refresh
    timer = threading.Timer(timeout_s, cur.interrupt)
    t0 = time.perf_counter()
    timer.start()
    try:
        # 2.0 streams results: an error can surface in fetchall(), so execute
        # and fetch share this try and this timer.
        rows = cur.execute(sql).fetchall()
        return "OK", int((time.perf_counter() - t0) * 1000), rows
    except duckdb.InterruptException:
        return "TIMEOUT", None, None
    finally:
        timer.cancel()
        cur.close()


def run_query(job, q):
    """One query: warm-up passes, timed passes, median, and the ZERO_SCAN guard
    (an empty scan is never reported as a win)."""
    job.update(state="running", started=time.time(), times=[],
               skip_puffin_verification=q.get("arm") in SKIP_PUFFIN_ARMS)
    status, rows, table = "OK", None, None
    try:
        sql = checked_sql(q)
        for i in range(q["warmup"] + q["runs"]):
            refresh_secret()
            status, ms, out = attempt(sql, q["timeout_s"], q.get("arm"))
            if status != "OK":
                break
            if i >= q["warmup"]:
                job["times"].append(ms)
                rows = out
    except Exception as e:                  # a failed query is a RESULT, not a crash
        status, job["error"] = "ERROR", "%s: %s" % (type(e).__name__, str(e)[:400])
    first = row_text(rows[0]) if rows else ""
    if status == "OK" and not q.get("allow_zero_rows"):
        v = first.split("|", 1)[0]
        if not rows or (not q.get("leading_col_is_label") and v in ("0", "NULL", "")):
            status = "ZERO_SCAN"
    med = int(statistics.median(job["times"])) if job["times"] else None
    if med is None and status == "OK":
        status = "ERROR"
    job.update(state="done", status=status, median_ms=med, result_rows=len(rows) if rows else 0,
               first_row=first[:200], rows=[row_text(r) for r in (rows or [])[:MAX_RESULT_ROWS]],
               finished=time.time())


_UNITS = {"bytes": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def profile_query(job, q):
    """The data a query scans, for the summary's "scanned GB (rows)" tables
    (StarRocks' audit log has ScanBytes / ScanRows). Run once, untimed, after an
    arm's timed passes (duckdb-warm.sh), as EXPLAIN ANALYZE with cache_httpfs
    bypassed (cache_httpfs_type='noop'): the HTTPFS "in:" counter then counts
    every byte the query reads from S3, where through the cache it counts only
    misses (0 when warm) and in whole cache blocks (CACHE_BLOCK_BYTES). The cache type is restored after,
    which keeps the disk cache. DuckDB 1.5.6's JSON profile has no usable
    counter for this: its total_bytes_read and rows_scanned do not count
    Parquet reads, and custom_profiling_settings crashes (2026-10-24).

    DuckDB >= 2.0: EXPLAIN (ANALYZE, FORMAT JSON) instead. The text plan is now
    drawn in boxes ("Table Scan"), which the regex below no longer matched
    (scan_rows came back None), while the JSON profile's io.total_bytes_read now
    counts Parquet bytes (checked on 2.0.0 over local and HTTP files,
    2026-11-02; over S3 with cache_httpfs 'noop' it is assumed to be the same
    byte count as 1.5's HTTPFS "in:" line, not yet compared on a real run).
    scan_rows = the sum of the TABLE_SCAN nodes' output rows (intermediate_rows,
    after pushed-down filters), the same quantity as before.

    scan_rows: the rows the TABLE_SCAN operators return, i.e. AFTER the filters
    DuckDB pushes into the scan. StarRocks' ScanRows counts rows read BEFORE its
    filters, so the two row counts are not the same quantity; the bytes are."""
    job.update(state="running", started=time.time())
    sql = checked_sql(q)
    refresh_secret()
    cur = con.cursor()
    timer = threading.Timer(q["timeout_s"], cur.interrupt)
    try:
        cur.execute("SET cache_httpfs_type='noop'")
        set_puffin(cur, q.get("arm"))
        timer.start()
        t0 = time.perf_counter()
        explain = "EXPLAIN (ANALYZE, FORMAT JSON) " if DUCKDB_MAJOR >= 2 else "EXPLAIN ANALYZE "
        txt = "\n".join(r[1] for r in cur.execute(explain + sql).fetchall())
        ms = int((time.perf_counter() - t0) * 1000)
    except duckdb.InterruptException:
        job.update(state="done", status="TIMEOUT", finished=time.time()); return
    finally:
        timer.cancel()
        try:
            cur.execute("SET cache_httpfs_type='%s'" % CACHE_MODE)
        finally:
            cur.close()
    if DUCKDB_MAJOR >= 2:
        scan_bytes, rows, why = profile_json(txt)
    else:
        m = re.search(r"\bin:\s*([\d.]+)\s*(bytes|KiB|MiB|GiB|TiB)", txt)
        scan_bytes = int(float(m.group(1)) * _UNITS[m.group(2)]) if m else None
        rows = [int(r.replace(",", "")) for r in re.findall(r"TABLE_SCAN[\s\S]*?([\d,]+) rows", txt)]
        why = "no HTTPFS 'in:' line in EXPLAIN ANALYZE"
    job.update(state="done", status="OK" if scan_bytes is not None else "ERROR",
               scan_bytes=scan_bytes, scan_rows=sum(rows) if rows else None, profile_ms=ms,
               finished=time.time(),
               **({} if scan_bytes is not None else {"error": why}))


def profile_json(txt):
    """(bytes read, [TABLE_SCAN output rows], error) from a 2.0 JSON profile."""
    try:
        d = json.loads(txt)
    except ValueError as e:
        return None, [], "EXPLAIN (ANALYZE, FORMAT JSON) is not JSON: %s" % e
    rows, todo = [], list(d.get("operator") or d.get("children") or [])
    while todo:
        n = todo.pop()
        todo += n.get("children") or []
        if str(n.get("type") or n.get("operator_type") or "").upper().replace(" ", "_") == "TABLE_SCAN":
            v = n.get("intermediate_rows", n.get("operator_cardinality"))
            if v is not None:
                rows.append(int(v))
    b = (d.get("io") or {}).get("total_bytes_read", d.get("total_bytes_read"))
    return (int(b) if b is not None else None), rows, "no io.total_bytes_read in the JSON profile"


def worker(job, q):
    with lock:
        try:
            (profile_query if q.get("profile") else run_query)(job, q)
        except Exception:
            job.update(state="done", status="ERROR", error=traceback.format_exc()[-400:])


def cache_bytes():
    """Disk cache size. Call only while holding `lock`, so it never runs next to
    a benchmark query; on its own cursor for the same reason as attempt()."""
    cur = con.cursor()
    try:
        return cur.execute("SELECT cache_httpfs_get_ondisk_data_cache_size()").fetchone()[0]
    except Exception:
        return None
    finally:
        cur.close()


class H(BaseHTTPRequestHandler):
    def reply(self, code, obj):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *a):
        pass

    def do_GET(self):
        if self.path.split("?")[0] == "/health":
            # Cheap by default: the harness calls /health before every request.
            # Measuring the disk cache walks its directory, slow once it fills,
            # and holding the query lock for it made queries submitted meanwhile
            # come back "busy" (6 of 12 refused, 2026-10-19). Only ?cache=1
            # measures it, under the lock and only when no query is running.
            busy, cb = lock.locked(), None
            if "cache=1" in self.path and lock.acquire(blocking=False):
                try:
                    busy, cb = False, cache_bytes()
                finally:
                    lock.release()
            self.reply(200, dict(INFO, busy=busy, cache_disk_bytes=cb))
        elif self.path.startswith("/job/"):
            j = jobs.get(self.path[5:])
            self.reply(200 if j else 404, j or {"error": "no such job"})
        else:
            self.reply(404, {"error": "unknown path"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        if self.path == "/query":
            # Wait briefly for a /health?cache=1 or clear_cache to finish rather
            # than refusing; a real query in progress still answers 409.
            if not lock.acquire(timeout=30):
                return self.reply(409, {"error": "busy: one query at a time"})
            lock.release()
            jid = uuid.uuid4().hex[:12]
            jobs[jid] = {"id": jid, "state": "queued", "name": body.get("name"), "arm": body.get("arm")}
            threading.Thread(target=worker, args=(jobs[jid], body), daemon=True).start()
            self.reply(202, {"id": jid})
        elif self.path == "/clear_cache":
            if not lock.acquire(blocking=False):
                return self.reply(409, {"error": "busy"})
            try:
                cur = con.cursor()
                try:
                    cur.execute("SELECT cache_httpfs_clear_cache()").fetchall()
                finally:
                    cur.close()
                self.reply(200, {"cache_disk_bytes": cache_bytes()})
            finally:
                lock.release()
        else:
            self.reply(404, {"error": "unknown path"})


if __name__ == "__main__":
    vol = init()
    refresh_secret(force=True)
    INFO = {"pod": POD, "duckdb": duckdb.__version__,
            "engine": con.execute("SELECT version()").fetchone()[0],
            "image": ENV.get("DUCKDB_IMAGE") or "pip",
            "iceberg_format_version": ICEBERG_FORMAT_VERSION,
            "skip_puffin_verification_arms": sorted(SKIP_PUFFIN_ARMS),
            "memory_limit": ENV["DUCKDB_MEMORY_LIMIT"], "threads": int(ENV["DUCKDB_THREADS"]),
            "cache_mem_bytes": CACHE_MEM_BYTES,
            "started_at": STARTED_AT, "max_result_rows": MAX_RESULT_ROWS, **vol}
    print("ready", json.dumps(INFO), flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
