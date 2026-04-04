"""DAG: Run dbt transformations and alert checks daily.

Schedule: 08:00 UTC daily (after NS and NDW ingestion complete).
Runs dbt deps, seed, run, test, snapshot, source freshness, then alert checks.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import sys

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

DBT_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dbt")

default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _check_alerts(**context):
    from alert_checker import check_reliability_alerts, send_slack_alert, write_alert_history

    service_date = context["ds"]
    project_id = os.environ["GCP_PROJECT_ID"]
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    on_time_threshold = float(os.environ.get("ALERT_ON_TIME_THRESHOLD", "80"))
    deviation_threshold = float(os.environ.get("ALERT_DEVIATION_THRESHOLD", "10"))

    alerts = check_reliability_alerts(
        service_date=service_date,
        on_time_threshold=on_time_threshold,
        deviation_threshold=deviation_threshold,
        project_id=project_id,
    )

    if webhook_url and not webhook_url.endswith("XXX/YYY/ZZZ"):
        for alert in alerts:
            if alert["fired"]:
                send_slack_alert(
                    webhook_url=webhook_url,
                    service_date=service_date,
                    corridor=alert["corridor"],
                    pct_on_time=alert["metric_value"],
                    avg_7d=on_time_threshold,
                )

    write_alert_history(alerts, project_id=project_id)


with DAG(
    dag_id="dag_dbt_transform",
    default_args=default_args,
    description="Run dbt transformations and alert checks",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["transformation", "dbt", "alerts"],
) as dag:
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir {DBT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir {DBT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_DIR}",
    )

    check_alerts_task = PythonOperator(
        task_id="check_alerts",
        python_callable=_check_alerts,
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_snapshot >> dbt_test >> dbt_freshness >> check_alerts_task
