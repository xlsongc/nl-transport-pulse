from unittest.mock import MagicMock, patch
import json


def test_check_reliability_alerts_detects_below_threshold():
    """Should return alert records for corridors below threshold."""
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        MagicMock(
            corridor_id="1",
            corridor_name="Amsterdam-Rotterdam",
            pct_on_time=62.0,
            avg_7d_pct_on_time=87.0,
        )
    ]
    mock_client.query.return_value = mock_query_job

    with patch("scripts.alert_checker.bigquery.Client", return_value=mock_client):
        from scripts.alert_checker import check_reliability_alerts

        alerts = check_reliability_alerts(
            service_date="2026-03-26",
            on_time_threshold=80.0,
            deviation_threshold=10.0,
        )

        assert len(alerts) == 1
        assert alerts[0]["corridor"] == "Amsterdam-Rotterdam"
        assert alerts[0]["metric_value"] == 62.0
        assert alerts[0]["fired"] is True


def test_send_slack_alert_posts_to_webhook():
    """Should POST formatted message to Slack webhook URL."""
    mock_response = MagicMock()
    mock_response.status_code = 200

    with patch("scripts.alert_checker.requests.post", return_value=mock_response) as mock_post:
        from scripts.alert_checker import send_slack_alert

        send_slack_alert(
            webhook_url="https://hooks.slack.com/test",
            service_date="2026-03-26",
            corridor="Amsterdam-Rotterdam",
            pct_on_time=62.0,
            avg_7d=87.0,
            disruption_count=2,
        )

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        body = json.loads(call_kwargs[1]["data"])
        assert "Amsterdam-Rotterdam" in body["text"]
        assert "62" in body["text"]
