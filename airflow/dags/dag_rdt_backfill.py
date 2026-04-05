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


def _backfill_services(**context):
    from ingest_rdt import download_services
    from bq_utils import load_json_to_bq

    conf = context["dag_run"].conf or {}
    months = conf.get("services_months", [])
    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.rdt_services"

    for month in months:
        logger.info("=== Backfilling services for %s ===", month)
        records = download_services(month)
        if not records:
            logger.warning("No records for %s, skipping", month)
            continue

        gcs_prefix = f"rdt/services/{month}"
        uris = _upload_ndjson_chunked(records, bucket, gcs_prefix)

        for i, uri in enumerate(uris):
            load_json_to_bq(
                gcs_uri=uri,
                table_id=table_id,
                service_date=month,
                partition_field="_year_month",
                skip_delete=(i > 0),  # Only delete on first chunk
            )
        logger.info("Loaded %d records for %s to BQ", len(records), month)


def _backfill_disruptions(**context):
    from ingest_rdt import download_disruptions
    from bq_utils import load_json_to_bq

    conf = context["dag_run"].conf or {}
    years = conf.get("disruptions_years", [])
    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.rdt_disruptions"

    for year in years:
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
        logger.info("Loaded %d records for %s to BQ", len(records), year)


with DAG(
    dag_id="dag_rdt_backfill",
    default_args=default_args,
    description="Backfill historical train data from rijdendetreinen.nl",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["backfill", "historical", "rdt"],
) as dag:
    backfill_services = PythonOperator(
        task_id="backfill_services",
        python_callable=_backfill_services,
    )

    backfill_disruptions = PythonOperator(
        task_id="backfill_disruptions",
        python_callable=_backfill_disruptions,
    )

    # Independent — can run in parallel
    backfill_services
    backfill_disruptions
