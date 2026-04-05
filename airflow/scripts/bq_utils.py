"""Helpers for loading data from GCS into BigQuery with partition-scoped overwrite."""
from __future__ import annotations

from google.cloud import bigquery


def load_json_to_bq(
    gcs_uri: str,
    table_id: str,
    service_date: str,
    partition_field: str = "_service_date",
) -> None:
    """Load a JSON file from GCS into a BigQuery table.

    Implements partition-scoped overwrite:
    1. Delete all rows for the given service_date
    2. Load new data from GCS
    """
    client = bigquery.Client()

    # Step 1: Delete existing partition data
    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE {partition_field} = '{service_date}'
    """
    try:
        client.query(delete_sql).result()
    except Exception:
        # Table may not exist yet on first run — that's OK
        pass

    # Step 2: Load from GCS
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


def load_csv_to_bq(
    gcs_uri: str,
    table_id: str,
    service_date: str,
    schema: list[bigquery.SchemaField] | None = None,
) -> None:
    """Load a CSV file from GCS into a BigQuery table.

    Same partition-scoped overwrite as load_json_to_bq.
    """
    client = bigquery.Client()

    # Step 1: Delete existing partition data
    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE service_date = '{service_date}'
    """
    try:
        client.query(delete_sql).result()
    except Exception:
        pass

    # Step 2: Load from GCS
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="service_date",
        ),
    )
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
