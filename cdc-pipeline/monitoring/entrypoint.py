"""
Monitoring entrypoint — runs FlinkCDCMonitor for Paimon and Iceberg pipelines,
generates periodic reports and optional comparisons.

Runs as a long-lived pod in the emr-flink namespace.
"""
import os
import sys
import time
import json
import logging
from datetime import datetime

from flink_cdc_monitor import FlinkCDCMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("monitor-entrypoint")

PAIMON_FLINK_URL = os.environ.get("PAIMON_FLINK_URL", "http://flink-cdc-paimonv3-rest:8081")
ICEBERG_FLINK_URL = os.environ.get("ICEBERG_FLINK_URL", "http://flink-cdc-icebergv3-rest:8081")
AWS_REGION = os.environ.get("AWS_REGION", "${AWS_REGION}")
PAIMON_GLUE_DB = os.environ.get("PAIMON_GLUE_DB", "flink_paimonv3_db")
ICEBERG_GLUE_DB = os.environ.get("ICEBERG_GLUE_DB", "flink_icebergv3_db")
PAIMON_WAREHOUSE = os.environ.get("PAIMON_WAREHOUSE", "s3://emr-on-eks-test-021732063925-us-west-2/paimonv3-warehouse")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://emr-on-eks-test-021732063925-us-west-2/icebergv3-warehouse")
CDC_TABLES = os.environ.get("CDC_TABLES", "customers,products,orders,order_items").split(",")
MONITOR_DURATION = int(os.environ.get("MONITOR_DURATION", "120"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))
REPEAT_INTERVAL = int(os.environ.get("REPEAT_INTERVAL", "300"))
REPORT_OUTPUT_DIR = os.environ.get("REPORT_OUTPUT_DIR", "/tmp/reports")
RUN_MODE = os.environ.get("RUN_MODE", "continuous")


def build_monitor(flink_url, glue_db, warehouse, tables, catalog_type="glue"):
    """Build a FlinkCDCMonitor with the appropriate pyiceberg catalog.

    catalog_type:
      - "glue"   : Iceberg tables registered natively in Glue (table_type=ICEBERG)
      - "hadoop" : Read Iceberg metadata directly from S3 (for Paimon tables)
    """
    if catalog_type == "hadoop":
        catalog_config = {
            "type": "hadoop",
            "warehouse": warehouse,
            "s3.region": AWS_REGION,
        }
    else:
        catalog_config = {
        "type": "glue",
        "warehouse": warehouse,
        "region_name": AWS_REGION,
        "s3.region": AWS_REGION,
        }
    return FlinkCDCMonitor(
        flink_url=flink_url,
        catalog_config=catalog_config,
        glue_database=glue_db,
        tables=tables,
    )


def save_report(monitor, report, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{report.label}_{ts}.json"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        f.write(monitor.to_json(report))
    log.info("Report saved to %s", filepath)
    return filepath


def run_single_cycle():
    log.info("=" * 70)
    log.info("Starting monitoring cycle at %s", datetime.utcnow().isoformat())
    log.info("=" * 70)

    paimon_monitor = build_monitor(
        PAIMON_FLINK_URL, PAIMON_GLUE_DB, PAIMON_WAREHOUSE, CDC_TABLES, catalog_type="hadoop")
    iceberg_monitor = build_monitor(
        ICEBERG_FLINK_URL, ICEBERG_GLUE_DB, ICEBERG_WAREHOUSE, CDC_TABLES, catalog_type="glue")

    paimon_report = paimon_monitor.generate_report(
        label="paimon-cdc",
        monitor_duration=MONITOR_DURATION,
        poll_interval=POLL_INTERVAL,
    )
    save_report(paimon_monitor, paimon_report, REPORT_OUTPUT_DIR)

    iceberg_report = iceberg_monitor.generate_report(
        label="iceberg-cdc",
        monitor_duration=MONITOR_DURATION,
        poll_interval=POLL_INTERVAL,
    )
    save_report(iceberg_monitor, iceberg_report, REPORT_OUTPUT_DIR)

    log.info("")
    log.info("=" * 70)
    log.info("  PAIMON vs ICEBERG COMPARISON")
    log.info("=" * 70)
    FlinkCDCMonitor.compare_reports(
        paimon_report, iceberg_report,
        labels=["Paimon CDC", "Iceberg CDC"],
    )

    return paimon_report, iceberg_report


def main():
    log.info("Flink CDC Monitor starting")
    log.info("  Paimon  : %s  db=%s", PAIMON_FLINK_URL, PAIMON_GLUE_DB)
    log.info("  Iceberg : %s  db=%s", ICEBERG_FLINK_URL, ICEBERG_GLUE_DB)
    log.info("  Tables  : %s", CDC_TABLES)
    log.info("  Mode    : %s", RUN_MODE)
    log.info("  Duration: %ds  Poll: %ds  Repeat: %ds", MONITOR_DURATION, POLL_INTERVAL, REPEAT_INTERVAL)

    if RUN_MODE == "once":
        run_single_cycle()
        log.info("Single run complete. Exiting.")
        return

    while True:
        try:
            run_single_cycle()
        except Exception:
            log.exception("Monitoring cycle failed")
        log.info("Next cycle in %d seconds...", REPEAT_INTERVAL)
        time.sleep(REPEAT_INTERVAL)


if __name__ == "__main__":
    main()
