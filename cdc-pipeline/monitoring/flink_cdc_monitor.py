"""
Flink CDC Monitor — captures Flink REST API metrics + Iceberg/Paimon table stats
from AWS Glue catalog, then compares Iceberg vs Paimon CDC pipeline performance.
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
from tabulate import tabulate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class TableSnapshot:
    name: str
    snapshot_count: int = 0
    total_records: int = 0
    total_data_files: int = 0
    total_size_bytes: int = 0
    current_snapshot_id: Optional[int] = None


@dataclass
class FlinkMetricsSnapshot:
    timestamp: float
    records_in: int = 0
    records_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0
    num_checkpoints: int = 0
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
    total_new_records_in_tables: int = 0
    total_new_bytes_in_tables: int = 0
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
        if self._catalog is None:
            self._catalog = load_catalog("glue", **self.catalog_config)
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

    def get_job_metrics(self, job_id: str, metrics: list[str]):
        metric_str = ",".join(metrics)
        data = self._get(f"/jobs/{job_id}/metrics?get={metric_str}")
        if not data:
            return {}
        return {m["id"]: m.get("value", "0") for m in data}

    def get_vertex_metrics(self, job_id: str):
        details = self.get_job_details(job_id)
        if not details:
            return {}
        totals = {"records_in": 0, "records_out": 0, "bytes_in": 0, "bytes_out": 0}
        for vertex in details.get("vertices", []):
            vid = vertex["id"]
            metrics = self._get(
                f"/jobs/{job_id}/vertices/{vid}/metrics"
                f"?get=numRecordsIn,numRecordsOut,numBytesIn,numBytesOut"
            )
            if metrics:
                for m in metrics:
                    key = m["id"]
                    val = int(float(m.get("value", "0")))
                    if "RecordsIn" in key:
                        totals["records_in"] += val
                    elif "RecordsOut" in key:
                        totals["records_out"] += val
                    elif "BytesIn" in key:
                        totals["bytes_in"] += val
                    elif "BytesOut" in key:
                        totals["bytes_out"] += val
        return totals

    def get_checkpoint_stats(self, job_id: str):
        data = self._get(f"/jobs/{job_id}/checkpoints")
        if not data:
            return {}
        counts = data.get("counts", {})
        latest = data.get("latest", {}).get("completed")
        return {
            "count": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "latest_duration_ms": latest.get("duration", 0) if latest else 0,
            "latest_size": latest.get("state_size", 0) if latest else 0,
        }

    def get_backpressure(self, job_id: str):
        details = self.get_job_details(job_id)
        if not details:
            return False
        for vertex in details.get("vertices", []):
            vid = vertex["id"]
            bp_data = self._get(f"/jobs/{job_id}/vertices/{vid}/backpressure")
            if bp_data and bp_data.get("backpressureLevel") == "HIGH":
                return True
        return False

    def capture_flink_snapshot(self, job_id: str) -> FlinkMetricsSnapshot:
        vertex_metrics = self.get_vertex_metrics(job_id)
        cp_stats = self.get_checkpoint_stats(job_id)
        bp = self.get_backpressure(job_id)
        details = self.get_job_details(job_id)
        uptime = details.get("duration", 0) if details else 0
        return FlinkMetricsSnapshot(
            timestamp=time.time(),
            records_in=vertex_metrics.get("records_in", 0),
            records_out=vertex_metrics.get("records_out", 0),
            bytes_in=vertex_metrics.get("bytes_in", 0),
            bytes_out=vertex_metrics.get("bytes_out", 0),
            num_checkpoints=cp_stats.get("count", 0),
            last_checkpoint_duration_ms=cp_stats.get("latest_duration_ms", 0),
            last_checkpoint_size=cp_stats.get("latest_size", 0),
            is_backpressured=bp,
            uptime_ms=uptime,
        )

    # ── Iceberg / Glue Table Stats ──────────────────────────────────

    def capture_table_snapshot(self, table_name: str) -> TableSnapshot:
        ts = TableSnapshot(name=table_name)
        try:
            tbl = self.catalog.load_table(f"{self.glue_database}.{table_name}")
            snapshots = list(tbl.metadata.snapshots)
            ts.snapshot_count = len(snapshots)
            if snapshots:
                current = snapshots[-1]
                ts.current_snapshot_id = current.snapshot_id
                summary = current.summary or {}
                ts.total_records = int(summary.get("total-records", 0))
                ts.total_data_files = int(summary.get("total-data-files", 0))
                ts.total_size_bytes = int(summary.get("total-file-size-in-bytes", 0))
        except Exception as e:
            log.info("pyiceberg failed for %s.%s (%s), trying Glue API fallback",
                     self.glue_database, table_name, e)
            ts = self._capture_table_snapshot_glue(table_name)
        return ts

    def _capture_table_snapshot_glue(self, table_name: str) -> TableSnapshot:
        """Fallback for non-Iceberg tables (e.g. Paimon) using boto3 Glue API"""
        ts = TableSnapshot(name=table_name)
        try:
            if not self.glue_client:
                region = self.catalog_config.get("region_name", "us-west-2")
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

    def capture_all_table_snapshots(self) -> dict[str, TableSnapshot]:
        return {t: self.capture_table_snapshot(t) for t in self.tables}

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

        table_end = self.capture_all_table_snapshots()
        report.table_snapshots_end = table_end

        if len(flink_snapshots) >= 2:
            first, last = flink_snapshots[0], flink_snapshots[-1]
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
        if flink_snapshots:
            report.checkpoint_count = flink_snapshots[-1].num_checkpoints

        for t in self.tables:
            s, e = table_start.get(t), table_end.get(t)
            if s and e:
                report.total_new_snapshots += e.snapshot_count - s.snapshot_count
                report.total_new_data_files += e.total_data_files - s.total_data_files
                report.total_new_records_in_tables += e.total_records - s.total_records
                report.total_new_bytes_in_tables += e.total_size_bytes - s.total_size_bytes

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
            ["Avg Checkpoint (ms)",
             f"{report_a.avg_checkpoint_duration_ms:,.0f}",
             f"{report_b.avg_checkpoint_duration_ms:,.0f}",
             delta_pct(report_a.avg_checkpoint_duration_ms, report_b.avg_checkpoint_duration_ms)],
            ["─" * 30, "─" * 15, "─" * 15, "─" * 10],
            ["New Table Snapshots",
             report_a.total_new_snapshots, report_b.total_new_snapshots, ""],
            ["New Data Files",
             report_a.total_new_data_files, report_b.total_new_data_files, ""],
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
        print(result)
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
                    "snapshots_delta": e.snapshot_count - s.snapshot_count,
                    "records_delta": e.total_records - s.total_records,
                    "data_files_delta": e.total_data_files - s.total_data_files,
                    "bytes_delta": e.total_size_bytes - s.total_size_bytes,
                }
        return json.dumps(d, indent=2)
