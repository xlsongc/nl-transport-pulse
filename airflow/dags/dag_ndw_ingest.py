"""DAG: Ingest NDW traffic observations daily.

Schedule: 07:00 UTC daily.
service_date = execution_date (= yesterday).

Pipeline: NDW → JSON → GCS → BigQuery (partition-scoped overwrite).
Only ingests measurement points from station_ndw_mapping seed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import csv
import os
import sys

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))


default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _load_mapped_location_ids() -> list:
    """Read NDW location IDs from the station_ndw_mapping seed CSV."""
    seed_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "dbt", "seeds", "station_ndw_mapping.csv",
    )
    location_ids = set()
    with open(seed_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_ids.add(row["ndw_location_id"])
    return list(location_ids)


def _extract_and_upload_traffic(**context):
    from ingest_ndw import extract_traffic
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]
    bucket = os.environ["GCS_BUCKET_NAME"]
    location_ids = _load_mapped_location_ids()

    records = extract_traffic(service_date=service_date, location_ids=location_ids)

    if records:
        gcs_uri = upload_json_to_gcs(
            data=records,
            bucket_name=bucket,
            blob_path=f"raw/ndw/traffic_flow/dt={service_date}/traffic.json",
        )
        context["ti"].xcom_push(key="traffic_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="traffic_gcs_uri", value=None)


def _load_traffic_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_traffic", key="traffic_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ndw_traffic_flow",
        service_date=service_date,
    )


with DAG(
    dag_id="dag_ndw_ingest",
    default_args=default_args,
    description="Ingest NDW 15-min traffic observations daily",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=True,
    max_active_runs=3,
    tags=["ingestion", "ndw"],
) as dag:
    extract_traffic_task = PythonOperator(
        task_id="extract_traffic",
        python_callable=_extract_and_upload_traffic,
    )

    load_traffic_task = PythonOperator(
        task_id="load_traffic_to_bq",
        python_callable=_load_traffic_to_bq,
    )

    extract_traffic_task >> load_traffic_task
