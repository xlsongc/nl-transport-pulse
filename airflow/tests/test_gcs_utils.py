from unittest.mock import MagicMock, patch
import json


def test_upload_json_to_gcs():
    """upload_json_to_gcs should upload serialized JSON to the correct GCS path."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    with patch("scripts.gcs_utils.storage.Client", return_value=mock_client):
        from scripts.gcs_utils import upload_json_to_gcs

        data = [{"id": "1", "value": "test"}]
        result = upload_json_to_gcs(
            data=data,
            bucket_name="test-bucket",
            blob_path="raw/ns/disruptions/dt=2026-03-26/disruptions.json",
        )

        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with(
            "raw/ns/disruptions/dt=2026-03-26/disruptions.json"
        )
        mock_blob.upload_from_string.assert_called_once()
        uploaded_data = [
            json.loads(line)
            for line in mock_blob.upload_from_string.call_args[0][0].splitlines()
        ]
        assert uploaded_data == data
        assert result == "gs://test-bucket/raw/ns/disruptions/dt=2026-03-26/disruptions.json"


def test_upload_csv_to_gcs():
    """upload_csv_to_gcs should upload CSV string to GCS."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    with patch("scripts.gcs_utils.storage.Client", return_value=mock_client):
        from scripts.gcs_utils import upload_csv_to_gcs

        csv_content = "col1,col2\nval1,val2\n"
        result = upload_csv_to_gcs(
            csv_content=csv_content,
            bucket_name="test-bucket",
            blob_path="raw/ndw/traffic_flow/dt=2026-03-26/traffic.csv",
        )

        mock_blob.upload_from_string.assert_called_once_with(
            csv_content, content_type="text/csv"
        )
        assert result == "gs://test-bucket/raw/ndw/traffic_flow/dt=2026-03-26/traffic.csv"
