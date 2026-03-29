from unittest.mock import MagicMock, patch
import scripts.bq_utils  # ensure module is imported before patching
from scripts.bq_utils import load_json_to_bq


def test_load_json_to_bq_deletes_partition_first():
    """load_json_to_bq should delete the target partition then load from GCS."""
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_client.load_table_from_uri.return_value = mock_job
    mock_client.query.return_value.result.return_value = None

    with patch("scripts.bq_utils.bigquery.Client", return_value=mock_client):
        load_json_to_bq(
            gcs_uri="gs://bucket/raw/ns/disruptions/dt=2026-03-26/disruptions.json",
            table_id="project.raw_nl_transport.ns_disruptions",
            service_date="2026-03-26",
        )

        # Should delete partition first
        delete_call = mock_client.query.call_args_list[0]
        assert "2026-03-26" in delete_call[0][0]
        assert "DELETE" in delete_call[0][0]

        # Then load from GCS
        mock_client.load_table_from_uri.assert_called_once()
