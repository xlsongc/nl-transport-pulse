from unittest.mock import patch


SAMPLE_NDW_RECORDS = [
    {
        "location_id": "NDW-001",
        "measurement_ts": "2026-03-26T08:00:00+01:00",
        "service_date": "2026-03-26",
        "avg_speed_kmh": 95.5,
        "vehicle_count": 42,
    },
    {
        "location_id": "NDW-001",
        "measurement_ts": "2026-03-26T08:15:00+01:00",
        "service_date": "2026-03-26",
        "avg_speed_kmh": 88.2,
        "vehicle_count": 55,
    },
]


def test_extract_traffic_returns_15min_records():
    """extract_traffic should return 15-min interval records for specified locations."""
    with patch(
        "scripts.ingest_ndw._fetch_raw_traffic_data",
        return_value=SAMPLE_NDW_RECORDS,
    ):
        from scripts.ingest_ndw import extract_traffic

        result = extract_traffic(
            service_date="2026-03-26",
            location_ids=["NDW-001"],
        )

        assert len(result) == 2
        assert result[0]["location_id"] == "NDW-001"
        assert result[0]["service_date"] == "2026-03-26"
        assert result[0]["avg_speed_kmh"] == 95.5
        assert result[0]["vehicle_count"] == 42


def test_extract_traffic_filters_by_location_ids():
    """extract_traffic should only return records for requested locations."""
    all_records = SAMPLE_NDW_RECORDS + [
        {
            "location_id": "NDW-999",
            "measurement_ts": "2026-03-26T08:00:00+01:00",
            "service_date": "2026-03-26",
            "avg_speed_kmh": 50.0,
            "vehicle_count": 10,
        },
    ]

    with patch(
        "scripts.ingest_ndw._fetch_raw_traffic_data",
        return_value=all_records,
    ):
        from scripts.ingest_ndw import extract_traffic

        result = extract_traffic(
            service_date="2026-03-26",
            location_ids=["NDW-001"],
        )

        assert all(r["location_id"] == "NDW-001" for r in result)
        assert len(result) == 2
