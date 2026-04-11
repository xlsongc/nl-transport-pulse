"""DAG: Ingest KNMI daily weather data.

Schedule: Daily 07:00 UTC (after midnight, weather data for yesterday is available).
Also supports manual trigger with custom date range for historical backfill.

Pipeline: KNMI API → JSON → GCS → BigQuery (partition-scoped overwrite).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_weather(**context):
    from ingest_knmi import extract_weather
    from gcs_utils import upload_json_to_gcs
    from bq_utils import load_json_to_bq

    # Use param override for backfill, otherwise use execution date
    start_override = context["params"].get("start_date", "")
    end_override = context["params"].get("end_date", "")

    if start_override and start_override != "none" and end_override and end_override != "none":
        # Backfill mode: use provided date range (YYYYMMDD)
        start_date = start_override.replace("-", "")
        end_date = end_override.replace("-", "")
        service_date_label = f"{start_override}_to_{end_override}"
    else:
        # Daily mode: fetch yesterday's weather
        service_date = context["ds"]  # YYYY-MM-DD
        start_date = service_date.replace("-", "")
        end_date = start_date
        service_date_label = service_date

    bucket = os.environ["GCS_BUCKET_NAME"]
    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    table_id = f"{project}.{dataset}.knmi_weather"

    records = extract_weather(start_date, end_date)
    if not records:
        return

    gcs_path = f"raw/knmi/weather/dt={service_date_label}/weather.json"
    gcs_uri = upload_json_to_gcs(records, bucket, gcs_path)

    # For daily runs, partition-scoped overwrite on service_date.
    # For backfill, skip_delete since we load a range at once.
    is_backfill = start_override and start_override != "none"
    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=table_id,
        service_date=service_date_label,
        partition_field="service_date",
        skip_delete=is_backfill,
    )


with DAG(
    dag_id="dag_knmi_ingest",
    default_args=default_args,
    description="Ingest KNMI daily weather data for corridor stations",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "knmi", "weather"],
    params={
        "start_date": Param(
            default="none",
            type="string",
            description="Backfill start date YYYY-MM-DD (use 'none' for daily mode)",
        ),
        "end_date": Param(
            default="none",
            type="string",
            description="Backfill end date YYYY-MM-DD (use 'none' for daily mode)",
        ),
    },
) as dag:
    ingest_weather = PythonOperator(
        task_id="ingest_weather",
        python_callable=_ingest_weather,
    )
