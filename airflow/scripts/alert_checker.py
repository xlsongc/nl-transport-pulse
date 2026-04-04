"""Post-dbt alert detection and Slack notification."""
from __future__ import annotations

import json
import uuid
from datetime import datetime

import requests
from google.cloud import bigquery


def check_reliability_alerts(
    service_date: str,
    on_time_threshold: float = 80.0,
    deviation_threshold: float = 10.0,
    project_id: str | None = None,
    dataset: str = "core_nl_transport",
) -> list[dict]:
    """Check for corridors below reliability threshold on the given service_date.

    Returns a list of alert records (one per corridor that triggered).
    """
    client = bigquery.Client(project=project_id)

    sql = f"""
    with latest as (
        select
            corridor_id,
            corridor_name,
            pct_on_time
        from `{project_id}.{dataset}.dm_multimodal_daily`
        where service_date = '{service_date}'
    ),
    rolling as (
        select
            corridor_id,
            avg(pct_on_time) as avg_7d_pct_on_time
        from `{project_id}.{dataset}.dm_multimodal_daily`
        where service_date between date_sub('{service_date}', interval 7 day)
              and '{service_date}'
        group by corridor_id
    )
    select
        l.corridor_id,
        l.corridor_name,
        l.pct_on_time,
        r.avg_7d_pct_on_time
    from latest l
    left join rolling r on l.corridor_id = r.corridor_id
    where l.pct_on_time < {on_time_threshold}
      and (r.avg_7d_pct_on_time - l.pct_on_time) > {deviation_threshold}
    """

    results = client.query(sql).result()
    alerts = []
    for row in results:
        alerts.append(
            {
                "alert_id": str(uuid.uuid4()),
                "check_ts": datetime.utcnow().isoformat(),
                "service_date": service_date,
                "alert_type": "reliability",
                "corridor": row.corridor_name,
                "metric_value": row.pct_on_time,
                "threshold": on_time_threshold,
                "fired": True,
            }
        )
    return alerts


def send_slack_alert(
    webhook_url: str,
    service_date: str,
    corridor: str,
    pct_on_time: float,
    avg_7d: float,
    disruption_count: int = 0,
) -> None:
    """Send a formatted alert message to Slack."""
    text = (
        f"Alert: NL Transport Pulse\n"
        f"Service date: {service_date}\n"
        f"Corridor: {corridor}\n"
        f"On-time: {pct_on_time:.0f}% (7-day avg: {avg_7d:.0f}%)\n"
        f"Active disruptions: {disruption_count}"
    )
    requests.post(
        webhook_url,
        data=json.dumps({"text": text}),
        headers={"Content-Type": "application/json"},
    )


def write_alert_history(
    alerts: list[dict],
    project_id: str,
    dataset: str = "core_nl_transport",
) -> None:
    """Write alert records to BigQuery alert_history table."""
    if not alerts:
        return
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.alert_history"
    errors = client.insert_rows_json(table_id, alerts)
    if errors:
        raise RuntimeError(f"Failed to insert alert history: {errors}")
