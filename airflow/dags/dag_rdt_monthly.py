"""DAG: Monthly RDT services ingestion.

Runs on the 5th of each month, pulls the previous month's complete
services archive from rijdendetreinen.nl. This ensures ongoing data
freshness without manual backfill.

RDT typically publishes monthly files within the first few days of the
following month, so the 5th gives a safe buffer.
"""
import os
import sys
import logging
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

logger = logging.getLogger(__name__)

CHUNK_SIZE = 200_000

default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _upload_ndjson_chunked(records, bucket, gcs_prefix):
    """Upload records in NDJSON chunks to GCS."""
    from gcs_utils import upload_json_to_gcs

    uris = []
    chunk = []
    part = 0
    count = 0

    for record in records:
        chunk.append(record)
        count += 1
        if len(chunk) >= CHUNK_SIZE:
            uri = upload_json_to_gcs(chunk, bucket, f"{gcs_prefix}/part_{part:03d}.json")
            uris.append(uri)
            chunk = []
            part += 1

    if chunk:
        uri = upload_json_to_gcs(chunk, bucket, f"{gcs_prefix}/part_{part:03d}.json")
        uris.append(uri)

    return uris, count


def _ingest_previous_month(**context):
    """Download and load the previous month's RDT services data."""
    from ingest_rdt import download_services
    from bq_utils import load_json_to_bq

    # Calculate previous month from execution_date
    exec_date = context["execution_date"]
    first_of_month = exec_date.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    month = last_month.strftime("%Y-%m")

    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.rdt_services"

    t_start = time.monotonic()
    logger.info("=== Monthly RDT ingest for %s ===", month)
    logger.info("[config] table=%s, bucket=%s", table_id, bucket)

    records = download_services(month)
    gcs_prefix = f"rdt/services/{month}"
    uris, num_records = _upload_ndjson_chunked(records, bucket, gcs_prefix)

    if not uris:
        logger.warning("No records for %s — file may not be published yet", month)
        return

    for i, uri in enumerate(uris):
        load_json_to_bq(
            gcs_uri=uri,
            table_id=table_id,
            service_date=month,
            partition_field="_year_month",
            skip_delete=(i > 0),
            use_existing_schema=True,
        )

    total_sec = time.monotonic() - t_start
    logger.info(
        "=== %s complete — %d records, %d chunks, %.0fs total ===",
        month, num_records, len(uris), total_sec,
    )


with DAG(
    dag_id="dag_rdt_monthly",
    default_args=default_args,
    description="Monthly RDT services ingestion — auto-pull previous month's archive",
    schedule_interval="0 6 5 * *",  # 5th of each month at 06:00 UTC
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["rdt", "ingestion", "monthly"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_previous_month",
        python_callable=_ingest_previous_month,
    )
