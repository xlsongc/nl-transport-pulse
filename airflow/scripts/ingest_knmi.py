"""KNMI daily weather data ingestion.

Downloads daily weather observations from the Royal Netherlands
Meteorological Institute (KNMI) open API.

API: POST https://www.daggegevens.knmi.nl/klimatologie/daggegevens
Auth: None required.
Docs: https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script

Values are returned in 0.1 units:
  - Temperature (TG, TN, TX): 0.1 °C  → 92 means 9.2°C
  - Precipitation (RH): 0.1 mm  → -1 means <0.05mm, 0 means 0mm
  - Precipitation duration (DR): 0.1 hour  → 10 means 1.0 hour
  - Wind speed (FG, FHX): 0.1 m/s  → 59 means 5.9 m/s
  - Wind direction (DDVEC): degrees  → 199 means SSW
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

KNMI_URL = "https://www.daggegevens.knmi.nl/klimatologie/daggegevens"

# KNMI stations mapped to rail corridors
CORRIDOR_STATIONS = {
    "240": "amsterdam",       # Schiphol
    "344": "rotterdam",       # Rotterdam Airport
    "260": "utrecht",         # De Bilt
    "215": "den_haag",        # Voorschoten
    "370": "eindhoven",       # Eindhoven Airport
}


def extract_weather(start_date: str, end_date: str) -> list[dict]:
    """Fetch daily weather for corridor stations from KNMI.

    Args:
        start_date: YYYYMMDD format
        end_date: YYYYMMDD format

    Returns:
        List of dicts, one per station per day.
    """
    station_codes = ":".join(CORRIDOR_STATIONS.keys())

    params = {
        "start": start_date,
        "end": end_date,
        "stns": station_codes,
        "vars": "TEMP:PRCP:WIND",
        "fmt": "json",
    }

    logger.info("Fetching KNMI weather for %s to %s, stations: %s", start_date, end_date, station_codes)
    resp = requests.post(KNMI_URL, data=params, timeout=120)
    resp.raise_for_status()

    if not resp.text or resp.text.strip() in ("", "[]"):
        logger.warning("KNMI returned empty response for %s to %s", start_date, end_date)
        return []

    raw = resp.json()
    ingested_at = datetime.now(timezone.utc).isoformat()

    records = []
    for row in raw:
        station = str(row["station_code"])
        # Parse date from ISO format to YYYY-MM-DD
        date_str = row["date"][:10]

        records.append({
            "station_code": station,
            "station_region": CORRIDOR_STATIONS.get(station, "unknown"),
            "service_date": date_str,
            # Temperature (convert from 0.1°C to °C)
            "avg_temp_c": row.get("TG") / 10.0 if row.get("TG") is not None else None,
            "min_temp_c": row.get("TN") / 10.0 if row.get("TN") is not None else None,
            "max_temp_c": row.get("TX") / 10.0 if row.get("TX") is not None else None,
            # Precipitation (convert from 0.1mm to mm; -1 means trace <0.05mm)
            "precipitation_mm": max(row.get("RH", 0), 0) / 10.0 if row.get("RH") is not None else None,
            "precipitation_duration_h": row.get("DR") / 10.0 if row.get("DR") is not None else None,
            # Wind (convert from 0.1 m/s to m/s)
            "avg_wind_speed_ms": row.get("FG") / 10.0 if row.get("FG") is not None else None,
            "max_wind_gust_ms": row.get("FHX") / 10.0 if row.get("FHX") is not None else None,
            "wind_direction_deg": row.get("DDVEC"),
            # Metadata
            "_source": "knmi",
            "_ingested_at": ingested_at,
        })

    logger.info("Parsed %d weather records", len(records))
    return records
