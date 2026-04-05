"""Tests for source-native raw NS API extraction."""
from unittest.mock import patch, MagicMock


SAMPLE_DEPARTURES_RESPONSE = {
    "payload": {
        "departures": [
            {
                "direction": "Rotterdam Centraal",
                "name": "NS 1234",
                "plannedDateTime": "2026-03-26T08:00:00+0100",
                "actualDateTime": "2026-03-26T08:05:00+0100",
                "trainCategory": "Intercity",
                "cancelled": False,
                "departureStatus": "INCOMING",
                "product": {"number": "1234", "operatorName": "NS"},
                "routeStations": [
                    {"uicCode": "8400530", "mediumName": "Rotterdam C."},
                ],
            }
        ]
    }
}

SAMPLE_DISRUPTIONS_V3_RESPONSE = [
    {
        "id": "12345",
        "type": "DISRUPTION",
        "title": "Amsterdam - Utrecht",
        "isActive": True,
        "start": "2026-04-01T08:00:00+0200",
        "end": "2026-04-01T12:00:00+0200",
        "publicationSections": [
            {
                "section": {
                    "stations": [
                        {"stationCode": "ASD", "name": "Amsterdam Centraal"},
                        {"stationCode": "UT", "name": "Utrecht Centraal"},
                    ]
                }
            }
        ],
        "timespans": [{"cause": {"label": "seinstoring"}}],
    }
]


def test_extract_departures_preserves_raw_and_adds_metadata():
    """Should return raw API records with metadata, no transformation."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DEPARTURES_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_departures

        records = extract_departures("key", "https://api", "ASD", "2026-03-26")

    assert len(records) == 1
    r = records[0]
    # Original API scalar fields preserved as-is
    assert r["direction"] == "Rotterdam Centraal"
    assert r["plannedDateTime"] == "2026-03-26T08:00:00+0100"
    # Nested fields are JSON-stringified for BQ compatibility
    import json
    assert json.loads(r["product"])["operatorName"] == "NS"
    assert json.loads(r["routeStations"])[0]["mediumName"] == "Rotterdam C."
    # Metadata injected
    assert r["_station_code"] == "ASD"
    assert r["_service_date"] == "2026-03-26"
    assert r["_source"] == "ns_api_v2"
    assert "_ingested_at" in r
    # No transformation happened
    assert "delay_minutes" not in r
    assert "station_code" not in r  # only _station_code


def test_extract_disruptions_preserves_raw_v3_and_adds_metadata():
    """v3 API returns bare list — should preserve structure and add metadata."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DISRUPTIONS_V3_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_disruptions

        records = extract_disruptions("key", "https://api", "2026-04-01")

    assert len(records) == 1
    r = records[0]
    # Scalar fields preserved
    assert r["id"] == "12345"
    assert r["type"] == "DISRUPTION"
    # Nested fields are JSON-stringified for BQ compatibility
    import json
    pub = json.loads(r["publicationSections"])
    assert pub[0]["section"]["stations"][0]["stationCode"] == "ASD"
    ts = json.loads(r["timespans"])
    assert ts[0]["cause"]["label"] == "seinstoring"
    # Metadata
    assert r["_service_date"] == "2026-04-01"
    assert r["_source"] == "ns_api_v3"
    # No transformation
    assert "disruption_id" not in r
    assert "affected_station_codes" not in r
    assert "duration_minutes" not in r
