"""Tests for rijdendetreinen.nl historical data extraction."""
import csv
import gzip
import io
from unittest.mock import patch, MagicMock


SAMPLE_SERVICES_CSV = """Service:RDT-ID,Service:Date,Service:Type,Service:Company,Service:Train number,Service:Completely cancelled,Service:Partly cancelled,Service:Maximum delay,Stop:RDT-ID,Stop:Station code,Stop:Station name,Stop:Arrival time,Stop:Arrival delay,Stop:Arrival cancelled,Stop:Departure time,Stop:Departure delay,Stop:Departure cancelled,Stop:Platform change,Stop:Planned platform,Stop:Actual platform
17867877,2026-03-01,Intercity,NS,1234,false,false,120,162057586,UT,Utrecht Centraal,,,,2026-03-01T08:00:00+01:00,0,false,false,5,5
17867877,2026-03-01,Intercity,NS,1234,false,false,120,162057587,ASD,Amsterdam Centraal,2026-03-01T08:30:00+01:00,60,false,,,,false,,
"""

SAMPLE_DISRUPTIONS_CSV = """rdt_id,ns_lines,rdt_lines,rdt_lines_id,rdt_station_names,rdt_station_codes,cause_nl,cause_en,statistical_cause_nl,statistical_cause_en,cause_group,start_time,end_time,duration_minutes
57284,Den Haag - Rotterdam,Den Haag HS - Rotterdam Centraal,11,"Delft,Schiedam","DT, SDM",aanrijding,collision,aanrijding,collision,accidents,2025-01-01 04:16:38,2025-01-01 04:26:57,10
"""


def test_download_services_parses_csv():
    """Should parse services CSV.gz into structured records."""
    compressed = gzip.compress(SAMPLE_SERVICES_CSV.encode("utf-8"))
    mock_resp = MagicMock()
    mock_resp.__enter__.return_value = mock_resp
    mock_resp.__exit__.return_value = False
    mock_resp.raw = io.BytesIO(compressed)
    mock_resp.raise_for_status.return_value = None

    with patch("scripts.ingest_rdt.requests.get", return_value=mock_resp):
        from scripts.ingest_rdt import download_services

        records = list(download_services("2026-03"))

    assert len(records) == 2
    r0 = records[0]
    assert r0["service_rdt_id"] == "17867877"
    assert r0["service_date"] == "2026-03-01"
    assert r0["station_code"] == "UT"
    assert r0["train_number"] == "1234"
    assert r0["completely_cancelled"] is False
    assert r0["departure_delay"] == 0
    assert r0["arrival_time"] is None  # first stop has no arrival
    assert r0["_source"] == "rdt_archive"
    assert r0["_year_month"] == "2026-03"

    r1 = records[1]
    assert r1["station_code"] == "ASD"
    assert r1["arrival_delay"] == 60
    assert r1["departure_time"] is None  # last stop has no departure


def test_download_disruptions_parses_csv():
    """Should parse disruptions CSV into structured records."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.content = SAMPLE_DISRUPTIONS_CSV.encode("utf-8")

    with patch("scripts.ingest_rdt.requests.get", return_value=mock_resp):
        from scripts.ingest_rdt import download_disruptions

        records = download_disruptions("2025")

    assert len(records) == 1
    r = records[0]
    assert r["rdt_id"] == "57284"
    assert r["cause_en"] == "collision"
    assert r["cause_group"] == "accidents"
    assert r["duration_minutes"] == 10
    assert r["rdt_station_codes"] == "DT, SDM"
    assert r["_source"] == "rdt_archive"
    assert r["_year"] == "2025"
