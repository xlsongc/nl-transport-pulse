"""Helpers for uploading data to Google Cloud Storage."""
import json
import logging
import time

from google.cloud import storage

logger = logging.getLogger(__name__)


def upload_json_to_gcs(
    data: list[dict], bucket_name: str, blob_path: str
) -> str:
    """Upload a list of dicts as a JSON file to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    logger.info("[gcs] Uploading %d records to gs://%s/%s", len(data), bucket_name, blob_path)
    t0 = time.monotonic()
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    ndjson = "\n".join(json.dumps(row) for row in data)
    size_mb = len(ndjson.encode()) / (1024 * 1024)
    blob.upload_from_string(ndjson, content_type="application/json")
    elapsed = time.monotonic() - t0
    uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("[gcs] Upload complete — %.1f MB in %.1fs → %s", size_mb, elapsed, uri)
    return uri


def upload_csv_to_gcs(
    csv_content: str, bucket_name: str, blob_path: str
) -> str:
    """Upload a CSV string to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_content, content_type="text/csv")
    return f"gs://{bucket_name}/{blob_path}"
