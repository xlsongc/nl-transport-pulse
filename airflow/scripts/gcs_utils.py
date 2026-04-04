"""Helpers for uploading data to Google Cloud Storage."""
import json
from google.cloud import storage


def upload_json_to_gcs(
    data: list[dict], bucket_name: str, blob_path: str
) -> str:
    """Upload a list of dicts as a JSON file to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    ndjson = "\n".join(json.dumps(row) for row in data)
    blob.upload_from_string(ndjson, content_type="application/json")
    return f"gs://{bucket_name}/{blob_path}"


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
