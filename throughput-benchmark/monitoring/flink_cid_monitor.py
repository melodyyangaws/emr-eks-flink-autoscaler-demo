"""
Flink Throughput Monitor — captures Flink REST API metrics + Iceberg/Paimon table stats
from AWS Glue catalog, then compares Iceberg vs Paimon writer throughput.

Metric collection strategy (inspired by Iceberg Performance Tuning notebook):
  1. Flink vertex metrics from /jobs/{jid} response (read-records, write-records)
     with subtask aggregation fallback (/subtasks/metrics?agg=sum)
  2. Iceberg snapshot summary deltas (added-records, total-delete-files, etc.)
  3. Checkpoint statistics for throughput calculation
"""
import io
import re
import time
import json
import logging
import statistics
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

import boto3
import fastavro
import requests
from pyiceberg.catalog import load_catalog
from pyiceberg.table import StaticTable
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class TableSnapshot:
    name: str
    format_version: int = 0
    snapshot_count: int = 0
    total_records: int = 0
    total_data_files: int = 0
    total_size_bytes: int = 0
    total_delete_files: int = 0
    total_position_deletes: int = 0
    total_equality_deletes: int = 0
    added_delete_files: int = 0
    added_records: int = 0
    added_data_files: int = 0
    added_files_size: int = 0
    current_snapshot_id: Optional[int] = None


@dataclass
class FlinkMetricsSnapshot:
    timestamp: float
    records_in: int = 0
    records_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    num_checkpoints: int = 0
    failed_checkpoints: int = 0
    last_checkpoint_duration_ms: int = 0
    last_checkpoint_size: int = 0
    is_backpressured: bool = False
    uptime_ms: int = 0
    # Sum of the source subtasks' numRecordsOutPerSecond: Flink's own rate meter
    # (a smoothed per-second rate), unaffected by the REST counter cache.
    records_out_per_sec: float = 0.0
    job_state: str = ""


@dataclass
class PipelineReport:
    label: str
    pipeline_type: str
    duration_seconds: float
    job_name: str = ""
    job_status: str = ""
    parallelism: int = 0
    start_time: str = ""
    end_time: str = ""
    flink_snapshots: list = field(default_factory=list)
    table_snapshots_start: dict = field(default_factory=dict)
    table_snapshots_end: dict = field(default_factory=dict)
    # Headline rows/s: commit_records_per_sec when the window saw a commit,
    # else meter_records_per_sec, else counter_records_per_sec (see generate_report).
    throughput_records_per_sec: float = 0.0
    throughput_source: str = ""
    # Rows the table committed / time between commits (the last commit before
    # the window is the anchor): exact, the same method as the write-ups.
    commit_records_per_sec: float = 0.0
    commits_in_window: int = 0
    # Median of the source rate meter over the window's RUNNING samples.
    meter_records_per_sec: float = 0.0
    # Source counter delta / time over the longest stretch without a restart.
    # The REST counters are cached and move in steps, so this is the least
    # reliable of the three; kept for comparison.
    counter_records_per_sec: float = 0.0
    restarts_in_window: int = 0
    throughput_bytes_per_sec: float = 0.0
    total_records_processed: int = 0
    total_bytes_processed: int = 0
    avg_checkpoint_duration_ms: float = 0.0
    checkpoint_count: int = 0
    total_new_snapshots: int = 0
    total_new_data_files: int = 0
    total_new_delete_files: int = 0
    total_new_position_deletes: int = 0
    total_new_equality_deletes: int = 0
    total_new_records_in_tables: int = 0
    total_new_bytes_in_tables: int = 0
    failed_checkpoints: int = 0
    errors: list = field(default_factory=list)


class FlinkCIDMonitor:

    def __init__(self, flink_url: str, catalog_config: dict, glue_client=None,
                 glue_database: str = "", tables: list = None):
        self.flink_url = flink_url.rstrip("/")
        self.catalog_config = catalog_config
        self.glue_client = glue_client
        self.glue_database = glue_database
        self.tables = tables or []
        self._catalog = None

    @property
    def catalog(self):
        if self.catalog_config.get("type") == "hadoop":
            return None
        if self._catalog is None:
            self._catalog = load_catalog(self.catalog_config.get("type", "glue"), **self.catalog_config)
        return self._catalog

    # ── Flink REST API ──────────────────────────────────────────────

    def _get(self, path: str, timeout: int = 10):
        try:
            r = requests.get(f"{self.flink_url}{path}", timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning("Flink API %s failed: %s", path, e)
            return None

    def get_jobs(self):
        data = self._get("/jobs")
        return data.get("jobs", []) if data else []

    def get_running_job(self):
        for j in self.get_jobs():
            if j.get("status") == "RUNNING":
                return j
        return None

    def get_job_details(self, job_id: str):
        return self._get(f"/jobs/{job_id}")

    def _operator_record_metrics(self, job_id: str, details: dict) -> dict:
        """{vertex id: [operator metric ids]} for the source's numRecordsOut and
        the table writer's numRecordsIn. Resolved once per job (operator names
        are fixed for the job's life); empty when nothing matches, in which
        case the caller falls back to vertex-level counters."""
        cache = getattr(self, "_op_metric_cache", {})
        if job_id in cache:
            return cache[job_id]
        # Rows: writer, not committer — IcebergFilesCommitter / "Global
        # Committer" count committables, not rows. Bytes: Iceberg's committed
        # data-file bytes. Paimon's committer counter (sink.numBytesOut) also
        # counts compaction rewrites, so its bytes come from APPEND commits in
        # the table metadata instead (_paimon_append_stats).
        pattern = re.compile(r"^(Source__.*\.numRecordsOut(PerSecond)?|(IcebergStreamWriter|Writer).*\.numRecordsIn"
                             r"|.*\.committedDataFilesByteCount)$")
        found = {}
        for vertex in details.get("vertices", []):
            ids = [m["id"] for m in self._get(f"/jobs/{job_id}/vertices/{vertex['id']}/subtasks/metrics") or []
                   if pattern.match(m["id"])]
            if ids:
                found[vertex["id"]] = ids
        # Only cache once both ends resolved; operator metrics can lag job start.
        has_src = any(i.startswith("Source__") for ids in found.values() for i in ids)
        has_sink = any(i.endswith("numRecordsIn") for ids in found.values() for i in ids)
        if has_src and has_sink:
            cache[job_id] = found
            self._op_metric_cache = cache
            log.info("Operator record metrics for %s: %s", job_id, found)
            return found
        return {}

    def get_vertex_metrics(self, job_id: str, details: dict = None):
        """Get aggregated record/byte counts from Flink job.

        Strategy:
          1. Subtask aggregation API (real-time counters, updates every poll)
          2. Fallback: vertex metrics from /jobs/{jid} (stale, only updates
             at checkpoint boundaries — causes zero deltas in summary)
        """
        if details is None:
            details = self.get_job_details(job_id)
        if not details:
            return {}

        totals = {"records_in": 0, "records_out": 0, "bytes_in": 0, "bytes_out": 0, "rate_out": 0.0}

        # Records come from OPERATOR-scoped metrics, not vertex-level ones. A
        # vertex's numRecordsIn/Out only counts records crossing a vertex
        # boundary: Iceberg chains source -> Correlate -> Calc ->
        # IcebergStreamWriter into ONE vertex, so its vertex numRecordsOut is
        # the writer's committables (~3/s) and its numRecordsIn is 0. Paimon's
        # bucket shuffle happens to split the writer into its own vertex, which
        # is why only Iceberg read wrong. records_out = rows the source emitted,
        # records_in = rows the table writer received.
        op_ids = self._operator_record_metrics(job_id, details)
        for vid, ids in op_ids.items():
            data = self._get(
                f"/jobs/{job_id}/vertices/{vid}/subtasks/metrics"
                f"?get={','.join(quote(i, safe='') for i in ids)}&agg=sum"
            )
            for m in data or []:
                raw = float(m.get("sum", m.get("value", "0")))
                val = int(raw)
                if m["id"].endswith("PerSecond"):
                    totals["rate_out"] += raw
                elif m["id"].startswith("Source__"):
                    totals["records_out"] += val
                elif "Byte" in m["id"]:  # Iceberg committedDataFilesByteCount
                    totals["bytes_out"] += val
                else:
                    totals["records_in"] += val

        # Bytes: committed Parquet bytes when the committer exposes them (both
        # sinks do). Vertex numBytesIn/Out are only network bytes between
        # vertices — in a chained job that is the committables, a few KB/s.
        has_committed_bytes = any("Byte" in i for ids in op_ids.values() for i in ids)
        for vertex in ([] if has_committed_bytes else details.get("vertices", [])):
            vid = vertex["id"]
            get = "numBytesIn,numBytesOut" if op_ids else "numRecordsIn,numRecordsOut,numBytesIn,numBytesOut"
            data = self._get(f"/jobs/{job_id}/vertices/{vid}/subtasks/metrics?get={get}&agg=sum")
            for m in data or []:
                mid = m["id"]
                val = int(float(m.get("sum", m.get("value", "0"))))
                if "RecordsIn" in mid:
                    totals["records_in"] += val
                elif "RecordsOut" in mid:
                    totals["records_out"] += val
                elif "BytesIn" in mid:
                    totals["bytes_in"] += val
                elif "BytesOut" in mid:
                    totals["bytes_out"] += val

        # Fallback: vertex metrics from job details (checkpoint-cached, may be stale)
        if totals["records_in"] == 0 and totals["records_out"] == 0:
            for vertex in details.get("vertices", []):
                m = vertex.get("metrics", {})
                totals["records_in"] += m.get("read-records", 0)
                totals["records_out"] += m.get("write-records", 0)
                totals["bytes_in"] += m.get("read-bytes", 0)
                totals["bytes_out"] += m.get("write-bytes", 0)

        return totals

    def get_checkpoint_stats(self, job_id: str):
        data = self._get(f"/jobs/{job_id}/checkpoints")
        if not data:
            return {}
        counts = data.get("counts", {})
        latest = data.get("latest", {}).get("completed")
        # Flink 1.20 uses end_to_end_duration; older versions use duration
        duration = (latest.get("end_to_end_duration") or latest.get("duration") or 0) if latest else 0
        return {
            "count": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "latest_duration_ms": duration,
            "latest_size": latest.get("state_size", 0) if latest else 0,
        }

    def _check_backpressure(self, details: dict) -> bool:
        """Check backpressure using already-fetched job details."""
        if not details:
            return False
        for vertex in details.get("vertices", []):
            vid = vertex["id"]
            bp_data = self._get(f"/jobs/{details['jid']}/vertices/{vid}/backpressure")
            if bp_data and bp_data.get("backpressureLevel") == "HIGH":
                return True
        return False

    def capture_flink_snapshot(self, job_id: str) -> FlinkMetricsSnapshot:
        """Capture all Flink metrics in a single pass — one job details call reused."""
        details = self.get_job_details(job_id)
        vertex_metrics = self.get_vertex_metrics(job_id, details=details)
        cp_stats = self.get_checkpoint_stats(job_id)
        bp = self._check_backpressure(details)
        uptime = details.get("duration", 0) if details else 0
        return FlinkMetricsSnapshot(
            timestamp=time.time(),
            records_in=vertex_metrics.get("records_in", 0),
            records_out=vertex_metrics.get("records_out", 0),
            bytes_in=vertex_metrics.get("bytes_in", 0),
            bytes_out=vertex_metrics.get("bytes_out", 0),
            num_checkpoints=cp_stats.get("count", 0),
            failed_checkpoints=cp_stats.get("failed", 0),
            last_checkpoint_duration_ms=cp_stats.get("latest_duration_ms", 0),
            last_checkpoint_size=cp_stats.get("latest_size", 0),
            is_backpressured=bp,
            uptime_ms=uptime,
            records_out_per_sec=vertex_metrics.get("rate_out", 0.0),
            job_state=(details or {}).get("state", ""),
        )

    # ── Iceberg / Glue Table Stats ──────────────────────────────────

    def capture_table_snapshot(self, table_name: str, since_ts: float = None) -> TableSnapshot:
        """Capture table stats — dispatches by catalog type.

        Paimon (hadoop): scan native S3 warehouse directory for file counts/sizes.
        Iceberg (glue):  read snapshot summary from Glue catalog.
        """
        if self.catalog_config.get("type") == "hadoop":
            return self._capture_table_snapshot_paimon_s3(table_name, since_ts=since_ts)
        return self._capture_table_snapshot_iceberg(table_name)

    def _capture_table_snapshot_iceberg(self, table_name: str) -> TableSnapshot:
        """Capture native Iceberg table stats from Glue catalog snapshot summary."""
        ts = TableSnapshot(name=table_name)
        try:
            tbl = self.catalog.load_table(f"{self.glue_database}.{table_name}")
            snapshots = list(tbl.metadata.snapshots) if tbl.metadata.snapshots else []
            ts.snapshot_count = len(snapshots)
            if snapshots:
                current = snapshots[-1]
                ts.current_snapshot_id = current.snapshot_id
                s = current.summary or {}
                log.info("Iceberg snapshot summary for %s.%s (snap_id=%s): %s | deletes: %s",
                         self.glue_database, table_name, current.snapshot_id,
                         {k: s.get(k) for k in ["total-records", "total-data-files",
                          "total-files-size", "total-delete-files"]},
                         {k: s.get(k, "0") for k in ["total-delete-files",
                          "added-delete-files", "total-position-deletes",
                          "total-equality-deletes"]})

                # Table format version as reported by the metadata
                ts.format_version = getattr(tbl.metadata, "format_version", 0)

                ts.total_records = int(s.get("total-records") or 0)
                ts.total_data_files = int(s.get("total-data-files") or 0)
                ts.total_size_bytes = int(s.get("total-files-size") or 0)
                ts.total_delete_files = int(s.get("total-delete-files") or 0)
                ts.total_position_deletes = int(s.get("total-position-deletes") or 0)
                ts.total_equality_deletes = int(s.get("total-equality-deletes") or 0)
                ts.added_delete_files = int(s.get("added-delete-files") or 0)
                ts.added_records = int(s.get("added-records") or 0)
                ts.added_data_files = int(s.get("added-data-files") or 0)
                ts.added_files_size = int(s.get("added-files-size") or 0)

                # Compute total bytes: if total-files-size is missing,
                # sum added-files-size across all snapshots (avoids inspect API
                # which fails on equality deletes)
                if ts.total_size_bytes == 0 and ts.total_data_files > 0:
                    total_bytes = 0
                    for snap in snapshots:
                        ss = snap.summary or {}
                        total_bytes += int(ss.get("added-files-size") or 0)
                    ts.total_size_bytes = total_bytes
                    log.info("Computed total bytes from added-files-size for %s.%s: %d",
                             self.glue_database, table_name, total_bytes)
            else:
                log.warning("No snapshots found for %s.%s", self.glue_database, table_name)

        except Exception as e:
            log.info("pyiceberg failed for %s.%s (%s), trying Glue API fallback",
                     self.glue_database, table_name, e)
            ts = self._capture_table_snapshot_glue(table_name)
        return ts

    def _paimon_s3(self, table_name: str):
        """(s3 client, bucket, table key prefix) for a Paimon table."""
        warehouse = self.catalog_config["warehouse"].rstrip("/")
        path = warehouse.replace("s3://", "").replace("s3a://", "")
        bucket, _, prefix = path.partition("/")
        s3 = boto3.client("s3", region_name=self.catalog_config.get("s3.region", "us-west-2"))
        return s3, bucket, f"{prefix}/{self.glue_database}.db/{table_name}".lstrip("/")

    @staticmethod
    def _read_avro(s3, bucket: str, key: str) -> list:
        # Paimon manifests and manifest lists: Avro, zstd by default.
        resp = s3.get_object(Bucket=bucket, Key=key)
        return list(fastavro.reader(io.BytesIO(resp["Body"].read())))

    def _capture_table_snapshot_paimon_s3(self, table_name: str, since_ts: float = None) -> TableSnapshot:
        """Table stats from Paimon's own metadata, not an S3 listing.

        Layout: <warehouse>/<db>.db/<table>/{snapshot/,manifest/,dt=*/bucket-0/}.
        A raw listing of the data dirs also sees compaction inputs that are
        not yet expired and files written ahead of their commit, so it read
        about 30% high on bytes and counted compaction output as new files.

          total_records    latest snapshot's totalRecordCount
          total_data_files LIVE files: sum of (_NUM_ADDED_FILES -
                           _NUM_DELETED_FILES) over the latest snapshot's base
                           and delta manifest lists, so compaction nets out
          snapshot_count   snapshot-N files still retained
        Bytes are not a live total here: per-window bytes come from APPEND
        commits only, in _paimon_append_stats.
        """
        ts = TableSnapshot(name=table_name)
        s3, bucket, table_prefix = self._paimon_s3(table_name)
        try:
            resp = s3.get_object(Bucket=bucket, Key=f"{table_prefix}/snapshot/LATEST")
            ts.current_snapshot_id = int(resp["Body"].read().decode("utf-8").strip())
            snap = json.loads(s3.get_object(
                Bucket=bucket, Key=f"{table_prefix}/snapshot/snapshot-{ts.current_snapshot_id}"
            )["Body"].read())
            ts.total_records = int(snap.get("totalRecordCount", 0))
            for ml in (snap.get("baseManifestList"), snap.get("deltaManifestList")):
                if ml:
                    for m in self._read_avro(s3, bucket, f"{table_prefix}/manifest/{ml}"):
                        ts.total_data_files += m["_NUM_ADDED_FILES"] - m["_NUM_DELETED_FILES"]
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_prefix}/snapshot/snapshot-"):
                ts.snapshot_count += len(page.get("Contents", []))
            log.info("Paimon metadata %s.%s: snapshot_id=%s live_files=%d records=%d snapshots=%d",
                     self.glue_database, table_name, ts.current_snapshot_id,
                     ts.total_data_files, ts.total_records, ts.snapshot_count)
        except Exception as e:
            log.warning("Paimon metadata read failed for %s.%s: %s",
                        self.glue_database, table_name, e)
        return ts

    def _paimon_commits(self, table_name: str, start_id: int, end_id: int) -> list:
        """[(commit time s, rows added)] for snapshots start_id..end_id, APPEND
        only (COMPACT rewrites rows already counted). start_id is the anchor:
        the last commit before the window."""
        out = []
        s3, bucket, table_prefix = self._paimon_s3(table_name)
        for sid in range(start_id, end_id + 1):
            try:
                snap = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{table_prefix}/snapshot/snapshot-{sid}")["Body"].read())
            except Exception as e:
                log.warning("Paimon snapshot-%d of %s unreadable, skipped: %s", sid, table_name, e)
                continue
            if snap.get("commitKind") == "APPEND" or sid == start_id:
                out.append((snap["timeMillis"] / 1000.0, int(snap.get("deltaRecordCount") or 0)))
        return out

    def _iceberg_commits(self, table_name: str) -> list:
        """[(commit time s, rows added)] for every snapshot in the current metadata."""
        try:
            tbl = self.catalog.load_table(f"{self.glue_database}.{table_name}")
            return [(sn.timestamp_ms / 1000.0, int((sn.summary or {}).get("added-records") or 0))
                    for sn in (tbl.metadata.snapshots or [])]
        except Exception as e:
            log.warning("Iceberg commit history unreadable for %s.%s: %s", self.glue_database, table_name, e)
            return []

    @staticmethod
    def _counter_rate(snaps: list):
        """(rows/s, rows, bytes, seconds, restarts) from the source counters over
        the longest stretch of RUNNING samples with no counter drop. Non-RUNNING
        samples are left out; each drop (counters reset) is one restart."""
        run = [x for x in snaps if x.job_state in ("", "RUNNING")]
        segments, cur, restarts = [], run[:1], 0
        for prev, x in zip(run, run[1:]):
            if x.records_out < prev.records_out:
                restarts += 1
                segments.append(cur)
                cur = [x]
            else:
                cur.append(x)
        segments.append(cur)
        seg = max(segments, key=lambda g: g[-1].timestamp - g[0].timestamp if len(g) >= 2 else -1)
        if len(seg) < 2 or seg[-1].timestamp <= seg[0].timestamp:
            return 0.0, 0, 0, 0.0, restarts
        dt = seg[-1].timestamp - seg[0].timestamp
        rows = seg[-1].records_out - seg[0].records_out
        return rows / dt, rows, seg[-1].bytes_out - seg[0].bytes_out, dt, restarts

    @staticmethod
    def _commit_rate(commits: list, t0: float, t1: float):
        """(rows/s, commits) from commits in (t0, t1], over the time since the
        last commit at or before t0. (0, n) when there is no anchor or commit."""
        commits = sorted(commits)
        anchor = [c for c in commits if c[0] <= t0]
        inside = [c for c in commits if t0 < c[0] <= t1]
        if not anchor or not inside:
            return 0.0, len(inside)
        span = inside[-1][0] - anchor[-1][0]
        return (sum(r for _, r in inside) / span if span > 0 else 0.0), len(inside)

    def _paimon_append_stats(self, table_name: str, start_id: int, end_id: int) -> dict:
        """Files, rows and bytes added by APPEND commits in (start_id, end_id].

        COMPACT commits are skipped: they rewrite rows already counted, and
        Paimon's committer byte counter (sink.numBytesOut) includes them.
        A snapshot already expired is logged and skipped (1 h retention against
        a 120 s window, so this should not happen).
        """
        out = {"commits": 0, "files": 0, "records": 0, "bytes": 0}
        s3, bucket, table_prefix = self._paimon_s3(table_name)
        for sid in range(start_id + 1, end_id + 1):
            try:
                snap = json.loads(s3.get_object(
                    Bucket=bucket, Key=f"{table_prefix}/snapshot/snapshot-{sid}")["Body"].read())
            except Exception as e:
                log.warning("Paimon snapshot-%d of %s unreadable, skipped: %s", sid, table_name, e)
                continue
            if snap.get("commitKind") != "APPEND" or not snap.get("deltaManifestList"):
                continue
            out["commits"] += 1
            out["records"] += int(snap.get("deltaRecordCount") or 0)
            for m in self._read_avro(s3, bucket, f"{table_prefix}/manifest/{snap['deltaManifestList']}"):
                for e in self._read_avro(s3, bucket, f"{table_prefix}/manifest/{m['_FILE_NAME']}"):
                    if e["_KIND"] == 0:  # 0 = ADD, 1 = DELETE
                        out["files"] += 1
                        out["bytes"] += e["_FILE"]["_FILE_SIZE"]
        return out

    def _load_table_from_s3(self, table_name: str):
        """Load Iceberg table from metadata files written by Paimon or Iceberg.

        Two-tier metadata location strategy:
          1. Glue table Parameters.metadata_location (Paimon hive-catalog storage)
          2. S3 version-hint.text fallback (pure Hadoop catalog layout)

        Paimon's IcebergHiveMetadataCommitter with storage=hive-catalog writes
        metadata files to S3 and registers metadata_location in Glue — it does
        NOT write version-hint.text (that's a Hadoop catalog convention).
        """
        region = self.catalog_config.get("s3.region", "us-west-2")
        metadata_location = None

        # Strategy 1: Read metadata_location from Glue table properties
        try:
            if not self.glue_client:
                self.glue_client = boto3.client("glue", region_name=region)
            resp = self.glue_client.get_table(
                DatabaseName=self.glue_database, Name=table_name)
            params = resp.get("Table", {}).get("Parameters", {})
            metadata_location = params.get("metadata_location")
            if metadata_location:
                log.info("Glue metadata_location for %s.%s: %s",
                         self.glue_database, table_name, metadata_location)
        except Exception as e:
            log.debug("Glue metadata_location lookup failed for %s: %s", table_name, e)

        # Strategy 2: Fall back to version-hint.text (Hadoop catalog layout)
        if not metadata_location:
            warehouse = self.catalog_config["warehouse"].rstrip("/")
            path = warehouse.replace("s3://", "")
            bucket = path.split("/", 1)[0]
            prefix = path.split("/", 1)[1] if "/" in path else ""
            metadata_prefix = f"{prefix}/{self.glue_database}/{table_name}/metadata"

            s3 = boto3.client("s3", region_name=region)
            try:
                resp = s3.get_object(Bucket=bucket, Key=f"{metadata_prefix}/version-hint.text")
                version = resp["Body"].read().decode("utf-8").strip()
                metadata_location = f"s3://{bucket}/{metadata_prefix}/v{version}.metadata.json"
            except Exception:
                raise FileNotFoundError(
                    f"No metadata for {self.glue_database}.{table_name}: "
                    f"no Glue metadata_location and no version-hint.text")

        return StaticTable.from_metadata(metadata_location, {"s3.region": region})

    def _scan_table_files(self, tbl, ts: TableSnapshot) -> TableSnapshot:
        """Read Iceberg manifest entries for file sizes via inspect API.

        Uses tbl.inspect.data_files() which reads manifest entries directly
        without applying deletes — avoids pyiceberg's 'equality deletes not
        supported' error that scan().plan_files() triggers on Flink CDC MoR tables.
        """
        try:
            df = tbl.inspect.data_files()
            total_size = 0
            total_records = 0
            total_files = 0
            if "file_size_in_bytes" in df.column_names:
                sizes = df.column("file_size_in_bytes").to_pylist()
                total_size = sum(s for s in sizes if s is not None)
                total_files = len(sizes)
            if "record_count" in df.column_names:
                counts = df.column("record_count").to_pylist()
                total_records = sum(c for c in counts if c is not None)

            ts.total_size_bytes = total_size
            if total_records > 0 and ts.total_records == 0:
                ts.total_records = total_records
            if total_files > 0 and ts.total_data_files == 0:
                ts.total_data_files = total_files
            log.info("Inspect data_files for %s: records=%d files=%d size=%d",
                     ts.name, total_records, total_files, total_size)
        except Exception as e:
            log.warning("Inspect data_files failed for %s: %s", ts.name, e)
        return ts

    def _capture_table_snapshot_glue(self, table_name: str) -> TableSnapshot:
        """Fallback for non-Iceberg tables (e.g. Paimon) using boto3 Glue API."""
        ts = TableSnapshot(name=table_name)
        try:
            if not self.glue_client:
                region = self.catalog_config.get("region_name",
                         self.catalog_config.get("s3.region", "us-west-2"))
                self.glue_client = boto3.client("glue", region_name=region)

            resp = self.glue_client.get_table(
                DatabaseName=self.glue_database, Name=table_name
            )
            tbl = resp.get("Table", {})
            params = tbl.get("Parameters", {})

            ts.total_records = int(params.get("numRows") or params.get("recordCount") or 0)
            ts.total_data_files = int(params.get("numFiles") or 0)
            ts.total_size_bytes = int(params.get("totalSize") or 0)
            ts.snapshot_count = int(params.get("numSnapshots") or
                                   params.get("snapshot.num-retained.max") or 0)
            ts.current_snapshot_id = int(params.get("snapshot.id") or 0) or None

            log.info("Glue fallback for %s.%s: records=%d files=%d size=%d",
                     self.glue_database, table_name,
                     ts.total_records, ts.total_data_files, ts.total_size_bytes)
        except Exception as e:
            log.warning("Glue fallback also failed for %s.%s: %s",
                        self.glue_database, table_name, e)
        return ts

    def capture_all_table_snapshots(self, since_ts: float = None) -> dict[str, TableSnapshot]:
        return {t: self.capture_table_snapshot(t, since_ts=since_ts) for t in self.tables}

    # ── Monitoring Session ──────────────────────────────────────────

    def generate_report(self, label: str, monitor_duration: int = 70,
                        poll_interval: int = 10) -> PipelineReport:
        log.info("═" * 60)
        log.info("Starting monitoring: %s (duration=%ds)", label, monitor_duration)
        log.info("═" * 60)

        report = PipelineReport(
            label=label, pipeline_type=label, duration_seconds=monitor_duration,
            start_time=datetime.utcnow().isoformat() + "Z",
        )

        job = self.get_running_job()
        if not job:
            report.errors.append("No running Flink job found")
            log.error("No running Flink job found at %s", self.flink_url)
            return report

        job_id = job["id"]
        details = self.get_job_details(job_id)
        report.job_name = details.get("name", "unknown") if details else "unknown"
        report.job_status = job.get("status", "unknown")
        if details:
            for v in details.get("vertices", []):
                report.parallelism = max(report.parallelism, v.get("parallelism", 0))

        log.info("Job: %s [%s] parallelism=%d", report.job_name, job_id, report.parallelism)

        table_start = self.capture_all_table_snapshots()
        report.table_snapshots_start = table_start
        monitor_start_ts = time.time()

        flink_snapshots = []
        start = time.time()
        while time.time() - start < monitor_duration:
            snap = self.capture_flink_snapshot(job_id)
            flink_snapshots.append(snap)
            elapsed = int(time.time() - start)
            log.info(
                "[%3ds/%ds] records_in=%d records_out=%d checkpoints=%d bp=%s",
                elapsed, monitor_duration, snap.records_in, snap.records_out,
                snap.num_checkpoints, snap.is_backpressured,
            )
            remaining = poll_interval - ((time.time() - start) % poll_interval)
            if time.time() - start + remaining < monitor_duration:
                time.sleep(remaining)
            else:
                break

        report.flink_snapshots = flink_snapshots
        report.end_time = datetime.utcnow().isoformat() + "Z"

        # Reset cached catalog to force fresh metadata from Glue for end capture
        self._catalog = None
        table_end = self.capture_all_table_snapshots(since_ts=monitor_start_ts)
        report.table_snapshots_end = table_end

        sample_dt = report.duration_seconds
        valid = [s for s in flink_snapshots if s.records_out > 0 or s.num_checkpoints > 0]
        if len(flink_snapshots) >= 2:
            # A restart resets the task counters, so a drop in records_out splits
            # the window. Differencing across it gave a negative row count
            # (Iceberg, 2026-10-18 12:49Z). The counter rate comes from the
            # longest stretch without one; the restart is reported.
            (report.counter_records_per_sec, report.total_records_processed,
             report.total_bytes_processed, dt, report.restarts_in_window) = self._counter_rate(flink_snapshots)
            if dt > 0:
                report.throughput_bytes_per_sec = report.total_bytes_processed / dt
                sample_dt = dt
            if report.restarts_in_window:
                report.errors.append(f"job restarted {report.restarts_in_window}x in the window "
                                     "(counters reset); counter rate from the longest clean stretch")
        rates = [s.records_out_per_sec for s in flink_snapshots
                 if s.records_out_per_sec > 0 and s.job_state in ("", "RUNNING")]
        if rates:
            report.meter_records_per_sec = statistics.median(rates)

        cp_durations = [s.last_checkpoint_duration_ms for s in flink_snapshots
                        if s.last_checkpoint_duration_ms > 0]
        if cp_durations:
            report.avg_checkpoint_duration_ms = statistics.mean(cp_durations)
        # Track failed checkpoints (critical for diagnosing commit failures)
        if flink_snapshots:
            last_snap = valid[-1] if valid else flink_snapshots[-1]
            report.failed_checkpoints = last_snap.failed_checkpoints
            if report.failed_checkpoints > 0:
                log.warning("⚠ %d failed checkpoints detected!", report.failed_checkpoints)
        # Checkpoint count: show DELTA (new during window), not absolute total
        if len(flink_snapshots) >= 2:
            first_cp = (valid[0] if valid else flink_snapshots[0]).num_checkpoints
            last_cp = (valid[-1] if valid else flink_snapshots[-1]).num_checkpoints
            report.checkpoint_count = max(0, last_cp - first_cp)

        # Table-level deltas: use max(0, delta) because snapshot expiration
        # can remove old snapshots between start/end, causing negative raw deltas
        for t in self.tables:
            s, e = table_start.get(t), table_end.get(t)
            if s and e:
                # Paimon: snapshot IDs are monotonically increasing (1,2,3,...) but
                # snapshot_count is capped by retention (e.g. always 5) → use ID delta.
                # Iceberg: snapshot IDs are RANDOM 64-bit ints → use snapshot_count delta.
                is_paimon = self.catalog_config.get("type") == "hadoop"
                if is_paimon and s.current_snapshot_id is not None and e.current_snapshot_id is not None:
                    report.total_new_snapshots += max(0, e.current_snapshot_id - s.current_snapshot_id)
                else:
                    report.total_new_snapshots += max(0, e.snapshot_count - s.snapshot_count)
                # Live-file delta on both formats: files added minus files
                # compaction removed. Iceberg's Flink sink never compacts, so
                # there it equals files added.
                report.total_new_data_files += max(0, e.total_data_files - s.total_data_files)
                report.total_new_delete_files += max(0, e.total_delete_files - s.total_delete_files)
                report.total_new_position_deletes += max(0, e.total_position_deletes - s.total_position_deletes)
                report.total_new_equality_deletes += max(0, e.total_equality_deletes - s.total_equality_deletes)
                report.total_new_records_in_tables += max(0, e.total_records - s.total_records)
                # Paimon: bytes of files added by APPEND commits in the window,
                # read from each commit's delta manifests. Iceberg: live-size
                # delta from the snapshot summary (no compaction to subtract).
                if is_paimon and s.current_snapshot_id is not None and e.current_snapshot_id is not None:
                    st = self._paimon_append_stats(t, s.current_snapshot_id, e.current_snapshot_id)
                    report.total_new_bytes_in_tables += st["bytes"]
                    report.total_bytes_processed = st["bytes"]
                    if sample_dt > 0:  # same divisor as rec/s and Iceberg's bytes/s
                        report.throughput_bytes_per_sec = st["bytes"] / sample_dt
                    log.info("Paimon %s APPEND commits in window: %d, +%d files, +%d rows, +%d bytes",
                             t, st["commits"], st["files"], st["records"], st["bytes"])
                else:
                    report.total_new_bytes_in_tables += max(0, e.total_size_bytes - s.total_size_bytes)
                log.info("Table %s (v%d): records %d→%d files %d→%d deletes %d→%d "
                         "pos_del %d→%d eq_del %d→%d snapshots %d→%d snap_id %s→%s",
                         t, e.format_version, s.total_records, e.total_records,
                         s.total_data_files, e.total_data_files,
                         s.total_delete_files, e.total_delete_files,
                         s.total_position_deletes, e.total_position_deletes,
                         s.total_equality_deletes, e.total_equality_deletes,
                         s.snapshot_count, e.snapshot_count,
                         s.current_snapshot_id, e.current_snapshot_id)

        # Commit-based rows/s: the headline. Rows each commit added / time
        # between commits, from the table's own history, so no counter caching,
        # no restart artefacts (replayed rows are not committed twice).
        window_end = time.time()
        for t in self.tables:
            s, e = table_start.get(t), table_end.get(t)
            if self.catalog_config.get("type") == "hadoop":
                commits = (self._paimon_commits(t, s.current_snapshot_id, e.current_snapshot_id)
                           if s and e and s.current_snapshot_id is not None and e.current_snapshot_id is not None
                           else [])
            else:
                commits = self._iceberg_commits(t)
            rate, n = self._commit_rate(commits, monitor_start_ts, window_end)
            report.commit_records_per_sec += rate
            report.commits_in_window += n
        if report.commit_records_per_sec > 0:
            report.throughput_records_per_sec, report.throughput_source = report.commit_records_per_sec, "commits"
        elif report.meter_records_per_sec > 0:
            report.throughput_records_per_sec, report.throughput_source = report.meter_records_per_sec, "rate meter"
        else:
            report.throughput_records_per_sec, report.throughput_source = report.counter_records_per_sec, "counter"
        log.info("Throughput %s: %.0f rows/s (%s) | restarts %d",
                 label, report.throughput_records_per_sec, report.throughput_source, report.restarts_in_window)
        log.info("Monitoring complete: %s", label)
        return report

    # ── Comparison ──────────────────────────────────────────────────

    @staticmethod
    def compare_reports(report_a: PipelineReport, report_b: PipelineReport,
                        labels: list[str] = None) -> str:
        la = labels[0] if labels and len(labels) > 0 else report_a.label
        lb = labels[1] if labels and len(labels) > 1 else report_b.label

        def fmt_bytes(b):
            if b >= 1 << 30:
                return f"{b / (1 << 30):.2f} GB"
            if b >= 1 << 20:
                return f"{b / (1 << 20):.2f} MB"
            if b >= 1 << 10:
                return f"{b / (1 << 10):.2f} KB"
            return f"{b} B"

        def delta_pct(a_val, b_val):
            if a_val == 0:
                return "N/A"
            pct = ((b_val - a_val) / a_val) * 100
            sign = "+" if pct >= 0 else ""
            return f"{sign}{pct:.1f}%"

        rows = [
            ["Job Name", report_a.job_name, report_b.job_name, ""],
            ["Status", report_a.job_status, report_b.job_status, ""],
            ["Parallelism", report_a.parallelism, report_b.parallelism, ""],
            ["Duration (s)", f"{report_a.duration_seconds:.0f}", f"{report_b.duration_seconds:.0f}", ""],
            ["─" * 30, "─" * 15, "─" * 15, "─" * 10],
            ["Throughput (rows/s)",
             f"{report_a.throughput_records_per_sec:,.0f} ({report_a.throughput_source})",
             f"{report_b.throughput_records_per_sec:,.0f} ({report_b.throughput_source})",
             delta_pct(report_a.throughput_records_per_sec, report_b.throughput_records_per_sec)],
            ["Restarts in window",
             report_a.restarts_in_window, report_b.restarts_in_window, ""],
            ["Records Processed (counter)",
             f"{report_a.total_records_processed:,}",
             f"{report_b.total_records_processed:,}",
             delta_pct(report_a.total_records_processed, report_b.total_records_processed)],
            ["Committed bytes/s (Parquet)",
             fmt_bytes(report_a.throughput_bytes_per_sec),
             fmt_bytes(report_b.throughput_bytes_per_sec),
             delta_pct(report_a.throughput_bytes_per_sec, report_b.throughput_bytes_per_sec)],
            ["Committed Bytes (Parquet)",
             fmt_bytes(report_a.total_bytes_processed),
             fmt_bytes(report_b.total_bytes_processed),
             delta_pct(report_a.total_bytes_processed, report_b.total_bytes_processed)],
            ["─" * 30, "─" * 15, "─" * 15, "─" * 10],
            ["Checkpoints",
             report_a.checkpoint_count, report_b.checkpoint_count, ""],
            ["Failed Checkpoints",
             report_a.failed_checkpoints, report_b.failed_checkpoints, ""],
            ["Avg Checkpoint (ms)",
             f"{report_a.avg_checkpoint_duration_ms:,.0f}",
             f"{report_b.avg_checkpoint_duration_ms:,.0f}",
             delta_pct(report_a.avg_checkpoint_duration_ms, report_b.avg_checkpoint_duration_ms)],
            ["─" * 30, "─" * 15, "─" * 15, "─" * 10],
            ["New Table Snapshots",
             report_a.total_new_snapshots, report_b.total_new_snapshots, ""],
            ["New Data Files",
             report_a.total_new_data_files, report_b.total_new_data_files, ""],
            ["New Delete Files",
             report_a.total_new_delete_files, report_b.total_new_delete_files, ""],
            ["  ↳ Position Deletes (Δ)",
             f"{report_a.total_new_position_deletes:,}", f"{report_b.total_new_position_deletes:,}", ""],
            ["  ↳ Equality Deletes (Δ)",
             f"{report_a.total_new_equality_deletes:,}", f"{report_b.total_new_equality_deletes:,}", ""],
            ["New Records in Tables",
             f"{report_a.total_new_records_in_tables:,}",
             f"{report_b.total_new_records_in_tables:,}",
             delta_pct(report_a.total_new_records_in_tables, report_b.total_new_records_in_tables)],
            ["New Bytes in Tables",
             fmt_bytes(report_a.total_new_bytes_in_tables),
             fmt_bytes(report_b.total_new_bytes_in_tables),
             delta_pct(report_a.total_new_bytes_in_tables, report_b.total_new_bytes_in_tables)],
        ]

        if report_a.errors or report_b.errors:
            rows.append(["─" * 30, "─" * 15, "─" * 15, "─" * 10])
            rows.append(["Errors",
                         "; ".join(report_a.errors) or "None",
                         "; ".join(report_b.errors) or "None", ""])

        headers = ["Metric", la, lb, "Delta"]
        table = tabulate(rows, headers=headers, tablefmt="grid", stralign="right")

        output = [
            "",
            "=" * 80,
            f"  Throughput Comparison: {la} vs {lb}",
            f"  Time: {report_a.start_time} → {report_a.end_time}",
            "=" * 80,
            table,
            "=" * 80,
        ]
        result = "\n".join(output)
        # Log each line so the table appears in container logs (kubectl logs / k9s)
        for line in result.split("\n"):
            log.info(line)
        return result

    def to_json(self, report: PipelineReport) -> str:
        d = {
            "label": report.label,
            "pipeline_type": report.pipeline_type,
            "job_name": report.job_name,
            "job_status": report.job_status,
            "parallelism": report.parallelism,
            "duration_seconds": report.duration_seconds,
            "start_time": report.start_time,
            "end_time": report.end_time,
            "throughput_records_per_sec": round(report.throughput_records_per_sec, 2),
            "throughput_source": report.throughput_source,
            "commit_records_per_sec": round(report.commit_records_per_sec, 2),
            "commits_in_window": report.commits_in_window,
            "meter_records_per_sec": round(report.meter_records_per_sec, 2),
            "counter_records_per_sec": round(report.counter_records_per_sec, 2),
            "restarts_in_window": report.restarts_in_window,
            "throughput_bytes_per_sec": round(report.throughput_bytes_per_sec, 2),
            "total_records_processed": report.total_records_processed,
            "total_bytes_processed": report.total_bytes_processed,
            "avg_checkpoint_duration_ms": round(report.avg_checkpoint_duration_ms, 2),
            "checkpoint_count": report.checkpoint_count,
            "total_new_snapshots": report.total_new_snapshots,
            "total_new_data_files": report.total_new_data_files,
            "total_new_delete_files": report.total_new_delete_files,
            "total_new_position_deletes": report.total_new_position_deletes,
            "total_new_equality_deletes": report.total_new_equality_deletes,
            "total_new_records_in_tables": report.total_new_records_in_tables,
            "total_new_bytes_in_tables": report.total_new_bytes_in_tables,
            "errors": report.errors,
            "table_details": {},
        }
        for t in self.tables:
            s = report.table_snapshots_start.get(t)
            e = report.table_snapshots_end.get(t)
            if s and e:
                d["table_details"][t] = {
                    "format_version": e.format_version,
                    "snapshots_delta": e.snapshot_count - s.snapshot_count,
                    "records_delta": e.total_records - s.total_records,
                    "data_files_delta": e.total_data_files - s.total_data_files,
                    "delete_files_delta": e.total_delete_files - s.total_delete_files,
                    "position_deletes_delta": e.total_position_deletes - s.total_position_deletes,
                    "equality_deletes_delta": e.total_equality_deletes - s.total_equality_deletes,
                    "added_delete_files": e.added_delete_files,
                    "bytes_delta": e.total_size_bytes - s.total_size_bytes,
                }
        return json.dumps(d, indent=2)
