"""NS API extraction logic for disruptions and departures."""
from __future__ import annotations

import requests
from datetime import datetime


def extract_disruptions(
    api_key: str, base_url: str, service_date: str
) -> list[dict]:
    """Extract disruption events from NS API for a given service_date.

    Returns a flat list of disruption records ready for JSON serialization.
    """
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    url = f"{base_url}/reisinformatie-api/api/v3/disruptions"
    params = {"isActive": "false"}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    raw_disruptions = response.json().get("payload", [])
    records = []

    for d in raw_disruptions:
        start_str = d.get("start", "")
        end_str = d.get("end", "")

        start_dt = _parse_ns_datetime(start_str) if start_str else None
        end_dt = _parse_ns_datetime(end_str) if end_str else None

        duration_minutes = 0.0
        if start_dt and end_dt:
            duration_minutes = (end_dt - start_dt).total_seconds() / 60.0

        # Flatten affected stations from all timespans
        affected_stations = []
        cause = ""
        for ts in d.get("timespans", []):
            if not cause and ts.get("cause", {}).get("label"):
                cause = ts["cause"]["label"]
            for st in ts.get("stations", []):
                code = st.get("stationCode", "")
                if code and code not in affected_stations:
                    affected_stations.append(code)

        records.append(
            {
                "disruption_id": d.get("id", ""),
                "service_date": service_date,
                "title": d.get("title", ""),
                "is_active": d.get("isActive", False),
                "start_timestamp": start_str,
                "end_timestamp": end_str,
                "duration_minutes": duration_minutes,
                "cause": cause,
                "affected_station_codes": affected_stations,
                "stations_affected_count": len(affected_stations),
            }
        )

    return records


def extract_departures(
    api_key: str, base_url: str, station_code: str, service_date: str
) -> list[dict]:
    """Extract departure records from NS API for a given station and service_date.

    Returns flat list with delay_minutes computed.
    """
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    url = f"{base_url}/reisinformatie-api/api/v2/departures"
    params = {"station": station_code}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()

    raw_departures = response.json().get("payload", {}).get("departures", [])
    records = []

    for dep in raw_departures:
        planned_str = dep.get("plannedDateTime", "")
        actual_str = dep.get("actualDateTime", planned_str)

        planned_dt = _parse_ns_datetime(planned_str) if planned_str else None
        actual_dt = _parse_ns_datetime(actual_str) if actual_str else None

        delay_minutes = 0.0
        if planned_dt and actual_dt:
            delay_minutes = (actual_dt - planned_dt).total_seconds() / 60.0

        records.append(
            {
                "station_code": station_code,
                "service_date": service_date,
                "direction": dep.get("direction", ""),
                "planned_departure_ts": planned_str,
                "actual_departure_ts": actual_str,
                "delay_minutes": delay_minutes,
                "train_category": dep.get("trainCategory", ""),
            }
        )

    return records


def _parse_ns_datetime(dt_str: str) -> datetime:
    """Parse NS API datetime string (ISO 8601 with timezone offset).

    Python 3.9's fromisoformat does not support offsets without a colon
    separator (e.g. +0100). Normalise to +HH:MM before parsing.
    """
    # Insert colon into timezone offset if missing: +0100 -> +01:00
    import re
    normalised = re.sub(r"([+-])(\d{2})(\d{2})$", r"\1\2:\3", dt_str)
    return datetime.fromisoformat(normalised)
