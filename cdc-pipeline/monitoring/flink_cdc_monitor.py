"""
Flink CDC Monitor — captures Flink REST API metrics + Iceberg/Paimon table stats
from AWS Glue catalog, then compares Iceberg vs Paimon CDC pipeline performance.

Metric collection strategy (inspired by Iceberg Performance Tuning notebook):
  1. Flink vertex metrics from /jobs/{jid} response (read-records, write-records)
     with subtask aggregation fallback (/subtasks/metrics?agg=sum)
  2. Iceberg snapshot summary deltas (added-records, total-delete-files, etc.)
  3. Checkpoint statistics for throughput calculation
"""
import time
import json
import logging
import statistics
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

import boto3
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
    bytes_added_in_window: int = 0


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
    throughput_records_per_sec: float = 0.0
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


class FlinkCDCMonitor:

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

        totals = {"records_in": 0, "records_out": 0, "bytes_in": 0, "bytes_out": 0}

        # Primary: subtask aggregation API — real-time counters
        for vertex in details.get("vertices", []):
            vid = vertex["id"]
            data = self._get(
                f"/jobs/{job_id}/vertices/{vid}/subtasks/metrics"
                f"?get=numRecordsIn,numRecordsOut,numBytesIn,numBytesOut&agg=sum"
            )
            if data:
                for m in data:
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
                          "total-file-size-in-bytes", "total-delete-files"]},
                         {k: s.get(k, "0") for k in ["total-delete-files",
                          "added-delete-files", "total-position-deletes",
                          "total-equality-deletes"]})

                # V3 format version from table metadata
                ts.format_version = getattr(tbl.metadata, "format_version", 0)

                ts.total_records = int(s.get("total-records") or 0)
                ts.total_data_files = int(s.get("total-data-files") or 0)
                ts.total_size_bytes = int(s.get("total-file-size-in-bytes") or 0)
                ts.total_delete_files = int(s.get("total-delete-files") or 0)
                ts.total_position_deletes = int(s.get("total-position-deletes") or 0)
                ts.total_equality_deletes = int(s.get("total-equality-deletes") or 0)
                ts.added_delete_files = int(s.get("added-delete-files") or 0)
                ts.added_records = int(s.get("added-records") or 0)
                ts.added_data_files = int(s.get("added-data-files") or 0)
                ts.added_files_size = int(s.get("added-files-size") or 0)

                # Compute total bytes: if total-file-size-in-bytes is missing,
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

    def _capture_table_snapshot_paimon_s3(self, table_name: str, since_ts: float = None) -> TableSnapshot:
        """Scan Paimon's native S3 warehouse directory for table stats.

        Paimon directory layout:
          <warehouse>/<db>.db/<table>/
            snapshot/  → snapshot-1, snapshot-2, ..., LATEST
            bucket-0/  → data-*.parquet
            bucket-1/  → data-*.parquet
            ...

        If since_ts is set, also computes bytes_added_in_window by filtering
        S3 objects with LastModified >= since_ts (for accurate delta bytes).
        """
        ts = TableSnapshot(name=table_name)
        warehouse = self.catalog_config["warehouse"].rstrip("/")
        region = self.catalog_config.get("s3.region", "us-west-2")
        import json

        path = warehouse.replace("s3://", "")
        bucket = path.split("/", 1)[0]
        prefix = path.split("/", 1)[1] if "/" in path else ""
        table_prefix = f"{prefix}/{self.glue_database}.db/{table_name}"

        s3 = boto3.client("s3", region_name=region)

        try:
            # 1. Read LATEST marker to get current snapshot id
            try:
                resp = s3.get_object(Bucket=bucket, Key=f"{table_prefix}/snapshot/LATEST")
                ts.current_snapshot_id = int(resp["Body"].read().decode("utf-8").strip())
            except Exception:
                pass

            # 2. Read latest snapshot JSON for record counts
            #    Paimon snapshot files contain: totalRecordCount, deltaRecordCount, id, etc.
            if ts.current_snapshot_id is not None:
                try:
                    snap_key = f"{table_prefix}/snapshot/snapshot-{ts.current_snapshot_id}"
                    snap_resp = s3.get_object(Bucket=bucket, Key=snap_key)
                    snap_data = json.loads(snap_resp["Body"].read().decode("utf-8"))
                    ts.total_records = int(snap_data.get("totalRecordCount", 0))
                    log.debug("Paimon snapshot-%d for %s: totalRecordCount=%d",
                              ts.current_snapshot_id, table_name, ts.total_records)
                except Exception as e:
                    log.warning("Failed to read Paimon snapshot JSON for %s: %s", table_name, e)

            # 3. Count snapshot files (snapshot-N, skip EARLIEST/LATEST markers)
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_prefix}/snapshot/snapshot-"):
                ts.snapshot_count += sum(1 for _ in page.get("Contents", []))

            # 4. Count data files + total size across all bucket-N/ directories
            total_files = 0
            total_size = 0
            added_in_window = 0
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_prefix}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if "/bucket-" in key and key.endswith(".parquet"):
                        total_files += 1
                        total_size += obj["Size"]
                        if since_ts and obj["LastModified"].timestamp() >= since_ts:
                            added_in_window += obj["Size"]
            ts.total_data_files = total_files
            ts.total_size_bytes = total_size
            ts.bytes_added_in_window = added_in_window

            log.info("Paimon S3 scan %s.%s: snapshots=%d files=%d size=%d records=%d snapshot_id=%s added_bytes=%d",
                     self.glue_database, table_name,
                     ts.snapshot_count, ts.total_data_files, ts.total_size_bytes, ts.total_records,
                     ts.current_snapshot_id, ts.bytes_added_in_window)
        except Exception as e:
            log.warning("Paimon S3 scan failed for %s.%s: %s",
                        self.glue_database, table_name, e)
        return ts

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

        if len(flink_snapshots) >= 2:
            # Use last VALID snapshot to handle mid-monitoring job restarts
            # (when job crashes, records/checkpoints drop to 0)
            valid = [s for s in flink_snapshots if s.records_out > 0 or s.num_checkpoints > 0]
            first = valid[0] if valid else flink_snapshots[0]
            last = valid[-1] if valid else flink_snapshots[-1]
            dt = last.timestamp - first.timestamp
            if dt > 0:
                report.total_records_processed = last.records_out - first.records_out
                report.total_bytes_processed = last.bytes_out - first.bytes_out
                report.throughput_records_per_sec = report.total_records_processed / dt
                report.throughput_bytes_per_sec = report.total_bytes_processed / dt

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
                report.total_new_data_files += max(0, e.total_data_files - s.total_data_files)
                report.total_new_delete_files += max(0, e.total_delete_files - s.total_delete_files)
                report.total_new_position_deletes += max(0, e.total_position_deletes - s.total_position_deletes)
                report.total_new_equality_deletes += max(0, e.total_equality_deletes - s.total_equality_deletes)
                report.total_new_records_in_tables += max(0, e.total_records - s.total_records)
                # Paimon: estimate new-data bytes from record delta × avg record size.
                # bytes_added_in_window (S3 LastModified) is inflated by compaction
                # output files that rewrite existing data with a fresh timestamp.
                # Iceberg: use total_size_bytes delta from snapshot summary
                new_records = max(0, e.total_records - s.total_records)
                if is_paimon and new_records > 0 and e.total_records > 0:
                    avg_bytes_per_record = e.total_size_bytes / e.total_records
                    report.total_new_bytes_in_tables += int(new_records * avg_bytes_per_record)
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
            ["Records Processed",
             f"{report_a.total_records_processed:,}",
             f"{report_b.total_records_processed:,}",
             delta_pct(report_a.total_records_processed, report_b.total_records_processed)],
            ["Throughput (rec/s)",
             f"{report_a.throughput_records_per_sec:,.1f}",
             f"{report_b.throughput_records_per_sec:,.1f}",
             delta_pct(report_a.throughput_records_per_sec, report_b.throughput_records_per_sec)],
            ["Throughput (bytes/s)",
             fmt_bytes(report_a.throughput_bytes_per_sec),
             fmt_bytes(report_b.throughput_bytes_per_sec),
             delta_pct(report_a.throughput_bytes_per_sec, report_b.throughput_bytes_per_sec)],
            ["Bytes Processed",
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
            f"  CDC Pipeline Comparison: {la} vs {lb}",
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
