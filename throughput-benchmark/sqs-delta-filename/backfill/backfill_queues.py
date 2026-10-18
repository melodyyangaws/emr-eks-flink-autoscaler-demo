#!/usr/bin/env python3
"""Backfill SQS landing queues with synthetic S3 ObjectCreated events.

S3 event notifications only announce NEW objects. To point the SQS-driven
Flink jobs (Paimon and Iceberg) at an existing landing prefix, this tool lists
the existing objects once and sends one S3-event-shaped message per object to
EVERY target queue, so each job sees every file.

Safety: S3 is only ever listed (ListObjectsV2). Nothing is deleted, copied or
modified. The only write is SendMessageBatch to the target queues.

See README.md for usage and cost.
"""

import argparse
import json
import os
import queue
import random
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

DEFAULT_QUEUES = ["cid-landing-jsongz-paimon", "cid-landing-jsongz-iceberg"]
DEFAULT_SUFFIX = ".json.gz"
# Levels below the prefix: year=/month=/day=/hour=/customerId=/productCode=.
# 4 = descend to hour=HH/ folders, then list each hour folder recursively.
DEFAULT_DISCOVER_DEPTH = 4
SQS_BATCH = 10
# SQS limit is 256 KiB for one message and for a whole SendMessageBatch.
MAX_BODY_BYTES = 256 * 1024
MAX_BATCH_BYTES = 256 * 1024
MAX_SEND_ATTEMPTS = 8
PROGRESS_SECONDS = 10.0
_SENTINEL = object()


# --------------------------------------------------------------------------
# Redaction: never print the real bucket name or account ID.
# --------------------------------------------------------------------------
class Redactor:
    _ACCOUNT_RE = re.compile(r"(?<!\d)\d{12}(?!\d)")

    def __init__(self, bucket=None, account_id=None):
        self.bucket = bucket
        self.account_id = account_id

    def __call__(self, text):
        text = str(text)
        if self.bucket:
            text = text.replace(self.bucket, "<bucket>")
        if self.account_id and len(self.account_id) >= 4:
            text = text.replace(self.account_id, "<account>")
        return self._ACCOUNT_RE.sub("<account>", text)


# --------------------------------------------------------------------------
# Message construction (pure functions, unit-tested offline).
# --------------------------------------------------------------------------
def encode_key(key):
    """URL-encode an object key the way S3 event notifications do.

    S3 form-encodes keys in events: spaces become '+', '=' becomes %3D, and
    '/' is kept. Java's URLDecoder.decode(key, UTF_8) reverses this.
    """
    return urllib.parse.quote_plus(key, safe="/")


def format_event_time(last_modified):
    """S3 event time format: 2026-10-09T06:21:03.123Z (UTC, millisecond)."""
    if isinstance(last_modified, str):
        return last_modified
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    utc = last_modified.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + "%03dZ" % (utc.microsecond // 1000)


def build_record(bucket, region, key, size, etag, last_modified):
    """One S3 ObjectCreated:Put record, exactly as S3 shapes it."""
    return {
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventTime": format_event_time(last_modified),
        "eventName": "ObjectCreated:Put",
        "userIdentity": {"principalId": "backfill"},
        "requestParameters": {"sourceIPAddress": "backfill"},
        "s3": {
            "s3SchemaVersion": "1.0",
            "configurationId": "backfill",
            "bucket": {"name": bucket, "arn": "arn:aws:s3:::" + bucket},
            "object": {
                "key": encode_key(key),
                "size": int(size),
                "eTag": (etag or "").strip('"'),
                "sequencer": "backfill",
            },
        },
    }


def _dumps(obj):
    return json.dumps(obj, separators=(",", ":"))


def build_body(records):
    """SQS message body for a list of records: {"Records":[...]}."""
    return _dumps({"Records": list(records)})


def build_event_body(bucket, region, key, size, etag, last_modified):
    """SQS message body with a single record (the default)."""
    return build_body([build_record(bucket, region, key, size, etag,
                                    last_modified)])


def record_for(obj, bucket, region):
    return build_record(bucket, region, obj["Key"], obj.get("Size", 0),
                        obj.get("ETag", ""), obj["LastModified"])


_BODY_OVERHEAD = len(build_body([]))  # {"Records":[]}


def pack_records(records, per_message, max_bytes=MAX_BODY_BYTES):
    """Pack records into message bodies.

    At most `per_message` records per body, and each body's UTF-8 size stays
    <= max_bytes: a group that would exceed the cap is split. Returns a list
    of (body, n_records). A single record larger than max_bytes raises.
    """
    if per_message < 1:
        raise ValueError("per_message must be >= 1")
    out = []
    cur, cur_bytes = [], _BODY_OVERHEAD
    for rec in records:
        rb = len(_dumps(rec).encode("utf-8"))
        if rb + _BODY_OVERHEAD > max_bytes:
            raise ValueError("a single record is %d bytes, over the %d byte "
                             "message cap" % (rb + _BODY_OVERHEAD, max_bytes))
        add = rb + (1 if cur else 0)  # comma separator
        if cur and (len(cur) >= per_message or cur_bytes + add > max_bytes):
            out.append((build_body(cur), len(cur)))
            cur, cur_bytes = [], _BODY_OVERHEAD
            add = rb
        cur.append(rec)
        cur_bytes += add
    if cur:
        out.append((build_body(cur), len(cur)))
    return out


def normalize_prefix(prefix):
    """Validate --prefix. Refuse empty or bucket-root prefixes."""
    if prefix is None:
        raise ValueError("--prefix is required")
    p = prefix.strip()
    if p.startswith("s3://"):
        raise ValueError("--prefix is a key prefix, not an s3:// URI "
                         "(e.g. cid-landing-jsongz/)")
    p = p.lstrip("/")
    if p in ("", "/") or p.strip("/") == "":
        raise ValueError("refusing to run on an empty or '/' prefix "
                         "(that would backfill the whole bucket)")
    if not p.endswith("/"):
        # Without the slash, 'cid-landing-jsongz' would also match
        # 'cid-landing-jsongz-r3/...'.
        p += "/"
    return p


# --------------------------------------------------------------------------
# Stats and progress reporting.
# --------------------------------------------------------------------------
class Stats:
    """Counters. sent/failed/retried are per queue and counted in FILES
    (records); sent_msgs counts SQS messages."""

    def __init__(self, queue_names):
        self.lock = threading.Lock()
        self.listed = 0
        self.leaf_prefixes = 0
        self.sent = {q: 0 for q in queue_names}
        self.sent_msgs = {q: 0 for q in queue_names}
        self.failed = {q: 0 for q in queue_names}
        self.retried = {q: 0 for q in queue_names}
        self.batch_calls = 0
        self.start = time.monotonic()

    def add(self, field, n=1, q=None):
        with self.lock:
            if q is None:
                setattr(self, field, getattr(self, field) + n)
            else:
                getattr(self, field)[q] += n

    def line(self):
        with self.lock:
            el = max(time.monotonic() - self.start, 1e-9)
            total_sent = sum(self.sent.values())
            per_q = " ".join("%s=%d" % (q, n) for q, n in self.sent.items())
            failed = sum(self.failed.values())
            return ("[%6.0fs] listed=%d (%.0f/s) sent files: %s (%.0f/s) "
                    "msgs=%d failed=%d batch_calls=%d"
                    % (el, self.listed, self.listed / el, per_q or "-",
                       total_sent / el, sum(self.sent_msgs.values()), failed,
                       self.batch_calls))


def progress_loop(stats, stop, out, interval=PROGRESS_SECONDS):
    while not stop.wait(interval):
        out(stats.line())


# --------------------------------------------------------------------------
# Listing.
# --------------------------------------------------------------------------
def _matches(key, suffix):
    return key.endswith(suffix) and not key.endswith("/")


def discover_leaf_prefixes(s3, bucket, prefix, depth, workers, emit, stats,
                           stop):
    """Walk Delimiter='/' folders down `depth` levels in parallel.

    Objects that sit directly in a folder above the leaf level are emitted
    here (the leaf listing would not see them). Returns the leaf prefixes.
    """
    paginator = s3.get_paginator("list_objects_v2")

    def one_level(p):
        children = []
        for page in paginator.paginate(Bucket=bucket, Prefix=p, Delimiter="/"):
            for cp in page.get("CommonPrefixes", []) or []:
                children.append(cp["Prefix"])
            for obj in page.get("Contents", []) or []:
                if stop.is_set():
                    return children
                emit(obj)
        return children

    level = [prefix]
    leaves = []
    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="discover") as pool:
        for _ in range(depth):
            if stop.is_set() or not level:
                break
            nxt = []
            futures = {pool.submit(one_level, p): p for p in level}
            for f in as_completed(futures):
                kids = f.result()
                if kids:
                    nxt.extend(kids)
                # A folder with no sub-folders is fully listed already.
            level = nxt
        leaves = sorted(level) if level is not None else []
    stats.add("leaf_prefixes", len(leaves))
    return leaves


def list_leaves(s3, bucket, leaves, workers, emit, stop):
    """List each leaf prefix recursively (no delimiter) in parallel."""
    paginator = s3.get_paginator("list_objects_v2")

    def one_leaf(p):
        for page in paginator.paginate(Bucket=bucket, Prefix=p):
            for obj in page.get("Contents", []) or []:
                if stop.is_set():
                    return
                emit(obj)

    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="list") as pool:
        for f in as_completed([pool.submit(one_leaf, p) for p in leaves]):
            f.result()


def list_objects(s3, bucket, prefix, suffix, depth, workers, limit, out_q,
                 stats, stop):
    """Producer: put matching objects on out_q, then a sentinel."""
    lock = threading.Lock()
    count = [0]

    def emit(obj):
        key = obj["Key"]
        if not _matches(key, suffix):
            return
        with lock:
            if limit is not None and count[0] >= limit:
                stop.set()
                return
            count[0] += 1
            if limit is not None and count[0] >= limit:
                stop.set()
        stats.add("listed")
        out_q.put(obj)

    try:
        leaves = discover_leaf_prefixes(s3, bucket, prefix, depth, workers,
                                        emit, stats, stop)
        if not stop.is_set():
            list_leaves(s3, bucket, leaves, workers, emit, stop)
    finally:
        out_q.put(_SENTINEL)


# --------------------------------------------------------------------------
# Sending.
# --------------------------------------------------------------------------
def _is_retryable_client_error(exc):
    code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
    return code not in (
        "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist",
        "AccessDenied", "AccessDeniedException", "InvalidParameterValue",
        "AWS.SimpleQueueService.BatchRequestTooLong",
        "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
    )


def _backoff(attempt, sleep):
    sleep(min(20.0, 0.2 * (2 ** attempt)) * (0.5 + random.random()))


def send_batch(sqs, queue_name, queue_url, entries, stats, sleep=time.sleep,
               max_attempts=MAX_SEND_ATTEMPTS, log=print, weights=None):
    """Send <=10 entries; retry the failed ones with exponential backoff.

    `weights` maps entry Id -> number of files (records) in that message;
    default 1. Returns the number of FILES that were never delivered.
    """
    w = (lambda i: weights.get(i, 1)) if weights else (lambda i: 1)

    def files(es):
        return sum(w(e["Id"]) for e in es)

    def give_up(es, why):
        n = files(es)
        stats.add("failed", n, q=queue_name)
        log("send %s: giving up on %d messages (%d files)%s"
            % (queue_name, len(es), n, why))
        return n

    pending = list(entries)
    lost = 0
    attempt = 0
    while pending:
        attempt += 1
        try:
            stats.add("batch_calls")
            resp = sqs.send_message_batch(QueueUrl=queue_url, Entries=pending)
        except Exception as exc:  # botocore ClientError / connection errors
            if hasattr(exc, "response") and not _is_retryable_client_error(exc):
                return lost + give_up(pending, ": %s" % exc)
            if attempt >= max_attempts:
                return lost + give_up(pending, " after %d attempts: %s"
                                      % (attempt, exc))
            stats.add("retried", files(pending), q=queue_name)
            _backoff(attempt, sleep)
            continue
        ok_ids = {e["Id"] for e in resp.get("Successful", []) or []}
        failed = resp.get("Failed", []) or []
        stats.add("sent", sum(w(i) for i in ok_ids), q=queue_name)
        stats.add("sent_msgs", len(ok_ids), q=queue_name)
        # Sender faults (bad entry) will not succeed on retry.
        hard = [e for e in pending
                if any(f["Id"] == e["Id"] and f.get("SenderFault")
                       for f in failed)]
        for f in failed:
            if f.get("SenderFault"):
                log("send %s: entry %s rejected: %s %s" % (
                    queue_name, f["Id"], f.get("Code"), f.get("Message")))
        if hard:
            lost += files(hard)
            stats.add("failed", files(hard), q=queue_name)
        soft = {f["Id"] for f in failed if not f.get("SenderFault")}
        pending = [e for e in pending if e["Id"] in soft]
        if pending:
            if attempt >= max_attempts:
                return lost + give_up(pending, " after %d attempts" % attempt)
            stats.add("retried", files(pending), q=queue_name)
            _backoff(attempt, sleep)
    return lost


def batch_entries(bodies, max_entries=SQS_BATCH, max_bytes=MAX_BATCH_BYTES):
    """Group (body, n_records) into SendMessageBatch-sized lists.

    Each group has <= max_entries entries and a total body size <= max_bytes.
    Yields (entries, weights) with Ids "0".."9" unique within the group.
    """
    cur, cur_bytes = [], 0
    for body, n in bodies:
        b = len(body.encode("utf-8"))
        if cur and (len(cur) >= max_entries or cur_bytes + b > max_bytes):
            yield _entries(cur)
            cur, cur_bytes = [], 0
        cur.append((body, n))
        cur_bytes += b
    if cur:
        yield _entries(cur)


def _entries(group):
    entries, weights = [], {}
    for i, (body, n) in enumerate(group):
        entries.append({"Id": str(i), "MessageBody": body})
        weights[str(i)] = n
    return entries, weights


def send_all(sqs, queue_urls, bucket, region, in_q, workers, stats,
             log=print, records_per_message=1):
    """Consumer: pack objects into messages, batch them, fan out to every
    queue. Each queue gets the identical message sequence."""
    in_flight = threading.BoundedSemaphore(workers * 4)
    futures = []
    # Enough records for one full SendMessageBatch before packing.
    group_size = SQS_BATCH * records_per_message

    def submit(qname, entries, weights):
        in_flight.acquire()

        def run():
            try:
                return send_batch(sqs, qname, queue_urls[qname], entries,
                                  stats, log=log, weights=weights)
            finally:
                in_flight.release()
        futures.append(pool.submit(run))

    def flush(objs):
        recs = [record_for(o, bucket, region) for o in objs]
        bodies = pack_records(recs, records_per_message)
        for entries, weights in batch_entries(bodies):
            for qname in queue_urls:
                submit(qname, entries, weights)

    with ThreadPoolExecutor(max_workers=workers,
                            thread_name_prefix="send") as pool:
        group = []
        while True:
            item = in_q.get()
            if item is _SENTINEL:
                break
            group.append(item)
            if len(group) >= group_size:
                flush(group)
                group = []
                # Drop references to finished futures to bound memory.
                if len(futures) > 10000:
                    futures[:] = [f for f in futures if not f.done()]
        if group:
            flush(group)
        for f in futures:
            f.result()


def drain(in_q):
    """--count-only consumer: just consume until the sentinel."""
    while in_q.get() is not _SENTINEL:
        pass


# --------------------------------------------------------------------------
# CLI.
# --------------------------------------------------------------------------
def parse_args(argv=None):
    ap = argparse.ArgumentParser(
        description="Send one synthetic S3 ObjectCreated event per existing "
                    "landing object to every target SQS queue.")
    ap.add_argument("--prefix", required=True,
                    help="key prefix to backfill, e.g. cid-landing-jsongz/")
    ap.add_argument("--bucket", default=os.environ.get("BUCKET_NAME"),
                    help="bucket (default: $BUCKET_NAME)")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION",
                                                       "us-west-2"),
                    help="region (default: $AWS_REGION or us-west-2)")
    ap.add_argument("--queues", default=",".join(DEFAULT_QUEUES),
                    help="comma-separated queue names or URLs "
                         "(default: %(default)s)")
    ap.add_argument("--suffix", default=DEFAULT_SUFFIX,
                    help="only objects whose key ends with this "
                         "(default: %(default)s)")
    ap.add_argument("--list-workers", type=int, default=32)
    ap.add_argument("--send-workers", type=int, default=32)
    ap.add_argument("--discover-depth", type=int,
                    default=DEFAULT_DISCOVER_DEPTH,
                    help="folder levels to walk with Delimiter='/' before "
                         "listing leaves in parallel; 4=hour, 5=customerId "
                         "(default: %(default)s)")
    ap.add_argument("--records-per-message", type=int, default=1,
                    help="pack up to N files into one message's Records "
                         "array; bodies are split to stay <= 256 KiB "
                         "(default: %(default)s)")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N matching objects (small tests)")
    ap.add_argument("--count-only", action="store_true",
                    help="list and count; send nothing")
    ap.add_argument("--show-sample", action="store_true",
                    help="print the first message body (redacted)")
    ap.add_argument("--fake-objects", type=int, default=None,
                    help=argparse.SUPPRESS)  # offline self-test, no AWS calls
    args = ap.parse_args(argv)
    if args.limit is not None and args.limit <= 0:
        ap.error("--limit must be > 0")
    if args.records_per_message < 1:
        ap.error("--records-per-message must be >= 1")
    if args.list_workers <= 0 or args.send_workers <= 0:
        ap.error("worker counts must be > 0")
    if args.discover_depth < 0:
        ap.error("--discover-depth must be >= 0")
    try:
        args.prefix = normalize_prefix(args.prefix)
    except ValueError as e:
        ap.error(str(e))
    if not args.bucket:
        ap.error("--bucket or $BUCKET_NAME is required")
    args.queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    if not args.queues and not args.count_only:
        ap.error("--queues is empty")
    return args


def make_clients(args):
    if args.fake_objects is not None:
        from fake_aws import FakeS3, FakeSQS
        return (FakeS3(args.bucket, args.prefix, args.fake_objects),
                FakeSQS(region=args.region))
    import boto3
    from botocore.config import Config
    pool = max(args.list_workers, args.send_workers) + 8
    cfg = Config(region_name=args.region, max_pool_connections=pool,
                 retries={"max_attempts": 10, "mode": "adaptive"})
    session = boto3.session.Session(region_name=args.region)
    return session.client("s3", config=cfg), session.client("sqs", config=cfg)


def resolve_queue_urls(sqs, names):
    urls = {}
    for n in names:
        if n.startswith("https://"):
            urls[n.rstrip("/").rsplit("/", 1)[-1]] = n
        else:
            urls[n] = sqs.get_queue_url(QueueName=n)["QueueUrl"]
    return urls


def main(argv=None):
    args = parse_args(argv)
    redact = Redactor(args.bucket, os.environ.get("AWS_ACCOUNT_ID"))

    def log(msg):
        print(redact(msg), flush=True)

    s3, sqs = make_clients(args)
    mode = "COUNT-ONLY (no messages sent)" if args.count_only else "SEND"
    if args.fake_objects is not None:
        mode += " [FAKE AWS]"
    log("backfill: mode=%s bucket=%s region=%s prefix=%s suffix=%s "
        "limit=%s depth=%d records_per_message=%d list_workers=%d "
        "send_workers=%d"
        % (mode, args.bucket, args.region, args.prefix, args.suffix,
           args.limit, args.discover_depth, args.records_per_message,
           args.list_workers, args.send_workers))

    queue_urls = {}
    if not args.count_only:
        queue_urls = resolve_queue_urls(sqs, args.queues)
        for n, u in queue_urls.items():
            log("  queue %s -> %s" % (n, u))

    stats = Stats(list(queue_urls))
    stop_list = threading.Event()
    stop_progress = threading.Event()
    objs = queue.Queue(maxsize=20000)

    sample_q = objs
    if args.show_sample:
        # Tee the first object so its body can be printed.
        sample_q = queue.Queue(maxsize=20000)
        first = []

        def tee():
            while True:
                item = sample_q.get()
                if item is not _SENTINEL and not first:
                    first.append(item)
                    log("sample body (1 record): " + build_body(
                        [record_for(item, args.bucket, args.region)]))
                objs.put(item)
                if item is _SENTINEL:
                    return
        threading.Thread(target=tee, daemon=True, name="tee").start()

    progress = threading.Thread(target=progress_loop,
                                args=(stats, stop_progress, log),
                                daemon=True, name="progress")
    progress.start()

    lister = threading.Thread(
        target=list_objects, name="lister",
        args=(s3, args.bucket, args.prefix, args.suffix, args.discover_depth,
              args.list_workers, args.limit, sample_q, stats, stop_list))
    lister.start()
    try:
        if args.count_only:
            drain(objs)
        else:
            send_all(sqs, queue_urls, args.bucket, args.region, objs,
                     args.send_workers, stats, log=log,
                     records_per_message=args.records_per_message)
    except KeyboardInterrupt:
        stop_list.set()
        log("interrupted; partial results below. Purge the queues before "
            "re-running or files will be read twice.")
        raise
    finally:
        lister.join(timeout=5)
        stop_progress.set()

    elapsed = time.monotonic() - stats.start
    log("done: " + stats.line())
    log("summary: listed=%d leaf_prefixes=%d elapsed=%.1fs"
        % (stats.listed, stats.leaf_prefixes, elapsed))
    total_failed = sum(stats.failed.values())
    if not args.count_only:
        for q in queue_urls:
            log("  %s: sent_files=%d messages=%d failed_files=%d "
                "retried_files=%d" % (q, stats.sent[q], stats.sent_msgs[q],
                                      stats.failed[q], stats.retried[q]))
        log("  SendMessageBatch calls=%d (~$%.4f at $0.40/M requests)"
            % (stats.batch_calls, stats.batch_calls * 0.40 / 1e6))
    else:
        msgs = -(-stats.listed // args.records_per_message)
        # Lower bound: large packed messages may split batches further.
        calls = len(args.queues) * -(-msgs // SQS_BATCH)
        log("  a real run would make >=%d SendMessageBatch calls to %d "
            "queue(s) (~$%.4f)" % (calls, len(args.queues),
                                   calls * 0.40 / 1e6))
    return 1 if total_failed else 0


if __name__ == "__main__":
    sys.exit(main())
