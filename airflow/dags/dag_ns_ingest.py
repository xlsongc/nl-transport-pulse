"""DAG: Ingest NS disruptions and departures daily.

Schedule: 06:00 UTC daily.
service_date = execution_date (= yesterday in Airflow convention).

Pipeline: NS API → JSON → GCS → BigQuery (partition-scoped overwrite).
"""
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import os
import sys

# Add scripts to path
sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))


default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Stations to pull departures for — MVP corridors
MVP_STATIONS = [
    "ASD", "RTD", "UT", "GVC", "EHV",
    "AH", "SDM", "DT", "LEDN", "ASA",
    "BRN", "HTN", "DB", "ED",
]


def _extract_and_upload_disruptions(**context):
    from ingest_ns import extract_disruptions
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]  # execution_date as YYYY-MM-DD
    api_key = os.environ["NS_API_KEY"]
    base_url = os.environ["NS_API_BASE_URL"]
    bucket = os.environ["GCS_BUCKET_NAME"]

    records = extract_disruptions(api_key, base_url, service_date)
    if records:
        gcs_uri = upload_json_to_gcs(
            data=records,
            bucket_name=bucket,
            blob_path=f"raw/ns/disruptions/dt={service_date}/disruptions.json",
        )
        context["ti"].xcom_push(key="disruptions_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="disruptions_gcs_uri", value=None)


def _extract_and_upload_departures(**context):
    from ingest_ns import extract_departures
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]
    api_key = os.environ["NS_API_KEY"]
    base_url = os.environ["NS_API_BASE_URL"]
    bucket = os.environ["GCS_BUCKET_NAME"]

    all_departures = []
    for station_code in MVP_STATIONS:
        departures = extract_departures(api_key, base_url, station_code, service_date)
        all_departures.extend(departures)
        time.sleep(1)  # rate-limit protection

    if all_departures:
        gcs_uri = upload_json_to_gcs(
            data=all_departures,
            bucket_name=bucket,
            blob_path=f"raw/ns/departures/dt={service_date}/departures.json",
        )
        context["ti"].xcom_push(key="departures_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="departures_gcs_uri", value=None)


def _load_disruptions_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_disruptions", key="disruptions_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ns_disruptions",
        service_date=service_date,
    )


def _load_departures_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_departures", key="departures_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ns_departures",
        service_date=service_date,
    )


with DAG(
    dag_id="dag_ns_ingest",
    default_args=default_args,
    description="Ingest NS disruptions and departures daily",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=True,
    max_active_runs=1,
    tags=["ingestion", "ns"],
) as dag:
    extract_disruptions_task = PythonOperator(
        task_id="extract_disruptions",
        python_callable=_extract_and_upload_disruptions,
    )

    extract_departures_task = PythonOperator(
        task_id="extract_departures",
        python_callable=_extract_and_upload_departures,
    )

    load_disruptions_task = PythonOperator(
        task_id="load_disruptions_to_bq",
        python_callable=_load_disruptions_to_bq,
    )

    load_departures_task = PythonOperator(
        task_id="load_departures_to_bq",
        python_callable=_load_departures_to_bq,
    )

    extract_disruptions_task >> load_disruptions_task
    extract_departures_task >> load_departures_task
