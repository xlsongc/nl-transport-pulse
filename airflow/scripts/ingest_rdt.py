"""Rijdendetreinen.nl historical data ingestion — batch backfill.

Downloads monthly services CSV.gz and yearly disruptions CSV from
opendata.rijdendetreinen.nl, uploads to GCS as archive, and loads to BigQuery.

Data source: https://www.rijdendetreinen.nl/en/open-data
License: CC BY 4.0
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

RDT_SERVICES_URL = "https://opendata.rijdendetreinen.nl/public/services/services-{year_month}.csv.gz"
RDT_DISRUPTIONS_URL = "https://opendata.rijdendetreinen.nl/public/disruptions/disruptions-{year}.csv"

# Services CSV column names → clean snake_case
SERVICES_COLUMNS = [
    "service_rdt_id", "service_date", "service_type", "company", "train_number",
    "completely_cancelled", "partly_cancelled", "max_delay",
    "stop_rdt_id", "station_code", "station_name",
    "arrival_time", "arrival_delay", "arrival_cancelled",
    "departure_time", "departure_delay", "departure_cancelled",
    "platform_change", "planned_platform", "actual_platform",
]

# Disruptions CSV column names (already clean)
DISRUPTIONS_COLUMNS = [
    "rdt_id", "ns_lines", "rdt_lines", "rdt_lines_id",
    "rdt_station_names", "rdt_station_codes",
    "cause_nl", "cause_en", "statistical_cause_nl", "statistical_cause_en",
    "cause_group", "start_time", "end_time", "duration_minutes",
]


def _parse_bool(val: str) -> bool | None:
    """Parse 'true'/'false' strings to boolean."""
    if val.lower() == "true":
        return True
    if val.lower() == "false":
        return False
    return None


def _parse_int(val: str) -> int | None:
    """Parse string to int, return None for empty."""
    if val.strip() == "":
        return None
    return int(val)


def download_services(year_month: str) -> list[dict]:
    """Download and parse a monthly services CSV.gz.

    Args:
        year_month: Format 'YYYY-MM', e.g. '2026-03'

    Returns:
        List of dicts, one per stop (row in CSV).
    """
    url = RDT_SERVICES_URL.format(year_month=year_month)
    logger.info("Downloading %s", url)

    resp = requests.get(url, timeout=120)
    resp.raise_for_status()

    raw = gzip.decompress(resp.content)
    text = raw.decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)  # skip header

    ingested_at = datetime.now(timezone.utc).isoformat()
    records = []
    for row in reader:
        if len(row) != len(SERVICES_COLUMNS):
            continue
        record = {
            "service_rdt_id": row[0],
            "service_date": row[1],
            "service_type": row[2],
            "company": row[3],
            "train_number": row[4],
            "completely_cancelled": _parse_bool(row[5]),
            "partly_cancelled": _parse_bool(row[6]),
            "max_delay": _parse_int(row[7]),
            "stop_rdt_id": row[8],
            "station_code": row[9],
            "station_name": row[10],
            "arrival_time": row[11] or None,
            "arrival_delay": _parse_int(row[12]),
            "arrival_cancelled": _parse_bool(row[13]),
            "departure_time": row[14] or None,
            "departure_delay": _parse_int(row[15]),
            "departure_cancelled": _parse_bool(row[16]),
            "platform_change": _parse_bool(row[17]),
            "planned_platform": row[18] or None,
            "actual_platform": row[19] or None,
            # Metadata
            "_source": "rdt_archive",
            "_ingested_at": ingested_at,
            "_year_month": year_month,
        }
        records.append(record)

    logger.info("Parsed %d stop records for %s", len(records), year_month)
    return records


def download_disruptions(year: str) -> list[dict]:
    """Download and parse a yearly disruptions CSV.

    Args:
        year: Format 'YYYY', e.g. '2025'

    Returns:
        List of dicts, one per disruption.
    """
    url = RDT_DISRUPTIONS_URL.format(year=year)
    logger.info("Downloading %s", url)

    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    text = resp.content.decode("utf-8")
    reader = csv.reader(io.StringIO(text))
    header = next(reader)  # skip header

    ingested_at = datetime.now(timezone.utc).isoformat()
    records = []
    for row in reader:
        if len(row) != len(DISRUPTIONS_COLUMNS):
            continue
        record = {
            "rdt_id": row[0],
            "ns_lines": row[1],
            "rdt_lines": row[2],
            "rdt_lines_id": row[3],
            "rdt_station_names": row[4],
            "rdt_station_codes": row[5],
            "cause_nl": row[6],
            "cause_en": row[7],
            "statistical_cause_nl": row[8],
            "statistical_cause_en": row[9],
            "cause_group": row[10],
            "start_time": row[11] or None,
            "end_time": row[12] or None,
            "duration_minutes": _parse_int(row[13]),
            # Metadata
            "_source": "rdt_archive",
            "_ingested_at": ingested_at,
            "_year": year,
        }
        records.append(record)

    logger.info("Parsed %d disruption records for %s", len(records), year)
    return records
