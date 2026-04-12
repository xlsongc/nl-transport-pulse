"""DAG: Backfill historical train data from rijdendetreinen.nl.

Manual-trigger only. Pass config to control what to load:
  {"services_months": ["2026-01", "2026-02"], "disruptions_years": ["2025"]}

Pipeline: Download CSV → GCS (archive) → BigQuery raw.
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

logger = logging.getLogger(__name__)

default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Chunk size: upload NDJSON in batches to avoid memory issues
CHUNK_SIZE = 200_000


def _upload_ndjson_chunked(records, bucket, gcs_prefix, chunk_size=CHUNK_SIZE):
    """Upload records as NDJSON to GCS, chunked if large."""
    from gcs_utils import upload_json_to_gcs

    if len(records) <= chunk_size:
        uri = upload_json_to_gcs(records, bucket, f"{gcs_prefix}/data.json")
        return [uri]

    uris = []
    for i in range(0, len(records), chunk_size):
        chunk = records[i : i + chunk_size]
        part = i // chunk_size
        uri = upload_json_to_gcs(chunk, bucket, f"{gcs_prefix}/part_{part:03d}.json")
        uris.append(uri)
        logger.info("Uploaded chunk %d (%d records) to %s", part, len(chunk), uri)
    return uris


def _backfill_single_month(month: str) -> None:
    """Backfill services for a single month. Runs as its own task to isolate memory."""
    import time
    from ingest_rdt import download_services
    from bq_utils import load_json_to_bq

    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.rdt_services"

    t_start = time.monotonic()
    logger.info("=== Backfilling services for %s ===", month)
    logger.info("[config] table=%s, bucket=%s", table_id, bucket)

    records = download_services(month)
    if not records:
        logger.warning("No records for %s, skipping", month)
        return

    gcs_prefix = f"rdt/services/{month}"
    uris = _upload_ndjson_chunked(records, bucket, gcs_prefix)

    for i, uri in enumerate(uris):
        load_json_to_bq(
            gcs_uri=uri,
            table_id=table_id,
            service_date=month,
            partition_field="_year_month",
            skip_delete=(i > 0),
        )

    total_sec = time.monotonic() - t_start
    logger.info(
        "=== %s complete — %d records, %d chunks, %.0fs total ===",
        month, len(records), len(uris), total_sec,
    )


def _backfill_disruptions(**context):
    import re
    import time
    from ingest_rdt import download_disruptions
    from bq_utils import load_json_to_bq

    raw = context["params"].get("disruptions_years", "")
    years = [y.strip() for y in raw.split(",") if re.fullmatch(r"\d{4}", y.strip())] if raw else []

    logger.info("[params] disruptions_years raw=%r → parsed=%s", raw, years)
    if not years:
        logger.info("No valid years to backfill, skipping disruptions")
        return

    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.rdt_disruptions"

    for year in years:
        t_start = time.monotonic()
        logger.info("=== Backfilling disruptions for %s ===", year)
        records = download_disruptions(year)
        if not records:
            logger.warning("No records for %s, skipping", year)
            continue

        gcs_prefix = f"rdt/disruptions/{year}"
        uris = _upload_ndjson_chunked(records, bucket, gcs_prefix)

        for uri in uris:
            load_json_to_bq(
                gcs_uri=uri,
                table_id=table_id,
                service_date=year,
                partition_field="_year",
            )

        total_sec = time.monotonic() - t_start
        logger.info(
            "=== %s complete — %d records, %.0fs total ===",
            year, len(records), total_sec,
        )


def _parse_months(**context) -> list[dict]:
    """Parse services_months param into list of dicts for dynamic task mapping."""
    import re
    raw = context["params"].get("services_months", "")
    all_tokens = [m.strip() for m in raw.split(",") if m.strip()] if raw else []
    months = [m for m in all_tokens if re.fullmatch(r"\d{4}-\d{2}", m)]
    skipped = [m for m in all_tokens if not re.fullmatch(r"\d{4}-\d{2}", m)]

    logger.info("[params] services_months raw=%r", raw)
    if skipped:
        logger.warning("[params] Skipped invalid month tokens: %s", skipped)
    logger.info("[params] Will backfill %d months: %s", len(months), months)

    return [{"month": m} for m in months]


def _backfill_month_task(month: str, **context) -> None:
    """Wrapper for dynamic task mapping — calls _backfill_single_month."""
    _backfill_single_month(month)


with DAG(
    dag_id="dag_rdt_backfill",
    default_args=default_args,
    description="Backfill historical train data from rijdendetreinen.nl",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["backfill", "historical", "rdt"],
    params={
        "services_months": Param(
            default="none",
            type="string",
            description="Comma-separated months, e.g. 2025-04,2025-05,2025-06. Use 'none' to skip.",
        ),
        "disruptions_years": Param(
            default="none",
            type="string",
            description="Comma-separated years, e.g. 2024,2025. Use 'none' to skip.",
        ),
    },
) as dag:
    backfill_disruptions = PythonOperator(
        task_id="backfill_disruptions",
        python_callable=_backfill_disruptions,
    )

    parse_months = PythonOperator(
        task_id="parse_months",
        python_callable=_parse_months,
    )

    # Dynamic task mapping: one task per month, each in its own process.
    # Memory is released between months — no more OOM on multi-month backfills.
    # max_active_tis_per_dag=1 ensures sequential execution.
    backfill_services = PythonOperator.partial(
        task_id="backfill_service_month",
        python_callable=_backfill_month_task,
        max_active_tis_per_dag=1,
    ).expand(op_kwargs=parse_months.output)

    parse_months >> backfill_services
