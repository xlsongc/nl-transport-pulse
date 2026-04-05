"""NS API extraction logic — source-native raw.

Extract functions return API responses as-is, with only metadata fields
(_service_date, _station_code, _ingested_at) injected. All transformation
logic belongs in the dbt staging layer.

Complex nested fields (lists, dicts) are JSON-stringified so BigQuery
can load them via autodetect. The dbt staging layer parses them back
with JSON_EXTRACT / JSON_EXTRACT_ARRAY.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import requests


def _stringify_nested(record: dict) -> dict:
    """Convert nested lists/dicts to JSON strings for BQ compatibility."""
    out = {}
    for key, value in record.items():
        if isinstance(value, (list, dict)):
            out[key] = json.dumps(value)
        else:
            out[key] = value
    return out


def extract_disruptions(api_key: str, base_url: str, service_date: str) -> list[dict]:
    """Fetch all disruptions from NS API and return raw records.

    Each record is the original API object with _service_date and _ingested_at added.
    """
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    url = f"{base_url}/reisinformatie-api/api/v3/disruptions"
    params = {"isActive": "false"}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    data = response.json()
    raw = data if isinstance(data, list) else data.get("payload", [])

    ingested_at = datetime.now(timezone.utc).isoformat()
    results = []
    for record in raw:
        record["_service_date"] = service_date
        record["_ingested_at"] = ingested_at
        record["_source"] = "ns_api_v3"
        results.append(_stringify_nested(record))

    return results


def extract_departures(api_key: str, base_url: str, station_code: str, service_date: str) -> list[dict]:
    """Fetch departures for a station from NS API and return raw records.

    Each record is the original API object with _station_code, _service_date,
    and _ingested_at added.
    """
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    url = f"{base_url}/reisinformatie-api/api/v2/departures"
    params = {"station": station_code}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    raw = response.json().get("payload", {}).get("departures", [])

    ingested_at = datetime.now(timezone.utc).isoformat()
    results = []
    for record in raw:
        record["_station_code"] = station_code
        record["_service_date"] = service_date
        record["_ingested_at"] = ingested_at
        record["_source"] = "ns_api_v2"
        results.append(_stringify_nested(record))

    return results
