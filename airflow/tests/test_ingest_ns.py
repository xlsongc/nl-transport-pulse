from unittest.mock import patch, MagicMock


SAMPLE_DISRUPTIONS_RESPONSE = {
    "payload": [
        {
            "id": "disruption-001",
            "title": "Geen treinen Amsterdam - Utrecht",
            "isActive": False,
            "start": "2026-03-26T06:00:00+0100",
            "end": "2026-03-26T10:30:00+0100",
            "timespans": [
                {
                    "cause": {"label": "seinstoring"},
                    "stations": [
                        {"stationCode": "ASD", "name": "Amsterdam Centraal"},
                        {"stationCode": "UT", "name": "Utrecht Centraal"},
                    ],
                }
            ],
        }
    ]
}


SAMPLE_DISRUPTIONS_LIVE_SHAPE_RESPONSE = {
    "payload": [
        {
            "id": "6063711",
            "title": "Dordrecht - Roosendaal.",
            "isActive": True,
            "start": "2026-03-30T18:43:00+0200",
            "end": "2026-03-30T23:59:00+0200",
            "timespans": [
                {
                    "cause": {"label": "defect spoor"},
                    "start": "2026-03-30T18:43:00+0200",
                    "end": "2026-03-30T23:59:00+0200",
                }
            ],
            "publicationSections": [
                {
                    "section": {
                        "stations": [
                            {"stationCode": "DD"},
                            {"stationCode": "RSD"},
                        ]
                    },
                    "consequence": {
                        "section": {
                            "stations": [
                                {"stationCode": "LAGEV"},
                            ]
                        }
                    },
                }
            ],
        }
    ]
}


def test_extract_disruptions_parses_response():
    """extract_disruptions should return flat list of disruption records."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DISRUPTIONS_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_disruptions

        result = extract_disruptions(
            api_key="test-key",
            base_url="https://test.api.ns.nl",
            service_date="2026-03-26",
        )

        assert len(result) == 1
        row = result[0]
        assert row["disruption_id"] == "disruption-001"
        assert row["service_date"] == "2026-03-26"
        assert row["title"] == "Geen treinen Amsterdam - Utrecht"
        assert "ASD" in row["affected_station_codes"]
        assert row["cause"] == "seinstoring"
        assert row["duration_minutes"] > 0


def test_extract_disruptions_supports_live_publication_sections_shape():
    """Live NS disruptions place affected stations under publicationSections."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DISRUPTIONS_LIVE_SHAPE_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_disruptions

        result = extract_disruptions(
            api_key="test-key",
            base_url="https://test.api.ns.nl",
            service_date="2026-03-30",
        )

        assert len(result) == 1
        row = result[0]
        assert row["cause"] == "defect spoor"
        assert row["affected_station_codes"] == ["DD", "RSD", "LAGEV"]
        assert row["stations_affected_count"] == 3


SAMPLE_DEPARTURES_RESPONSE = {
    "payload": {
        "departures": [
            {
                "direction": "Rotterdam Centraal",
                "plannedDateTime": "2026-03-26T08:00:00+0100",
                "actualDateTime": "2026-03-26T08:05:00+0100",
                "trainCategory": "Intercity",
                "routeStations": [
                    {"uicCode": "8400530", "mediumName": "Rotterdam C."},
                ],
            }
        ]
    }
}


def test_extract_departures_calculates_delay():
    """extract_departures should calculate delay_minutes from planned vs actual."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DEPARTURES_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_departures

        result = extract_departures(
            api_key="test-key",
            base_url="https://test.api.ns.nl",
            station_code="ASD",
            service_date="2026-03-26",
        )

        assert len(result) == 1
        row = result[0]
        assert row["station_code"] == "ASD"
        assert row["delay_minutes"] == 5.0
        assert row["direction"] == "Rotterdam Centraal"
        assert row["service_date"] == "2026-03-26"
