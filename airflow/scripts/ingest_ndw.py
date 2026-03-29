"""NDW traffic data extraction logic.

Note: _fetch_raw_traffic_data is a placeholder that must be adapted
based on the actual NDW data format discovered in Task 5.
If NDW provides DATEX II XML, this function will parse XML.
If NDW provides CSV dumps, this function will parse CSV.
"""
from __future__ import annotations

import csv
import io
import requests


def extract_traffic(
    service_date: str,
    location_ids: list[str],
) -> list[dict]:
    """Extract 15-minute traffic observations for specified NDW locations.

    Args:
        service_date: The date to extract data for (YYYY-MM-DD).
        location_ids: List of NDW measurement point IDs to include
                      (from station_ndw_mapping seed).

    Returns:
        List of dicts with: location_id, measurement_ts, service_date,
        avg_speed_kmh, vehicle_count.
    """
    raw_records = _fetch_raw_traffic_data(service_date)
    location_set = set(location_ids)
    return [r for r in raw_records if r["location_id"] in location_set]


def _fetch_raw_traffic_data(service_date: str) -> list[dict]:
    """Fetch raw traffic data from NDW for a given service_date.

    TODO: Implement based on actual NDW data format (Task 5 findings).
    Options:
    - DATEX II XML: Download gzipped XML, parse with lxml
    - CSV: Download CSV, parse with csv module
    - REST API: HTTP GET with date parameter

    This function should return a list of dicts with standardized fields:
    location_id, measurement_ts, service_date, avg_speed_kmh, vehicle_count.
    """
    raise NotImplementedError(
        "Implement after confirming NDW data format in Task 5. "
        "See docs/superpowers/specs/ for expected output schema."
    )


def parse_ndw_csv(csv_content: str, service_date: str) -> list[dict]:
    """Parse NDW CSV data into standardized records.

    Use this if NDW provides CSV format.
    """
    reader = csv.DictReader(io.StringIO(csv_content))
    records = []
    for row in reader:
        records.append(
            {
                "location_id": row.get("measurementSiteId", row.get("location_id", "")),
                "measurement_ts": row.get("timestamp", row.get("measurement_ts", "")),
                "service_date": service_date,
                "avg_speed_kmh": float(row.get("averageSpeed", row.get("avg_speed_kmh", 0))),
                "vehicle_count": int(row.get("vehicleCount", row.get("vehicle_count", 0))),
            }
        )
    return records
