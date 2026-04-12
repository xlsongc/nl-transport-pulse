"""Helpers for loading data from GCS into BigQuery with partition-scoped overwrite."""
from __future__ import annotations

import logging
import time

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_json_to_bq(
    gcs_uri: str,
    table_id: str,
    service_date: str,
    partition_field: str = "_service_date",
    skip_delete: bool = False,
) -> None:
    """Load a JSON file from GCS into a BigQuery table.

    Implements partition-scoped overwrite:
    1. Delete all rows for the given service_date (unless skip_delete=True)
    2. Load new data from GCS

    Use skip_delete=True when loading multiple chunks for the same partition
    (delete once before the first chunk, then append the rest).
    """
    client = bigquery.Client()

    if not skip_delete:
        delete_sql = f"""
        DELETE FROM `{table_id}`
        WHERE {partition_field} = '{service_date}'
        """
        logger.info("[bq] DELETE %s WHERE %s = '%s'", table_id, partition_field, service_date)
        try:
            result = client.query(delete_sql).result()
            logger.info("[bq] DELETE complete — %d rows removed", result.num_dml_affected or 0)
        except Exception as exc:
            # Table may not exist yet on first run — that's OK
            logger.info("[bq] DELETE skipped (table may not exist): %s", exc)

    # Load from GCS
    logger.info("[bq] LOAD %s → %s", gcs_uri, table_id)
    t0 = time.monotonic()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    elapsed = time.monotonic() - t0
    logger.info(
        "[bq] LOAD complete — job %s, %d rows loaded in %.1fs",
        load_job.job_id, load_job.output_rows or 0, elapsed,
    )


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
