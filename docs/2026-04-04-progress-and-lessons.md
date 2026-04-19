# Progress Report & Lessons Learned — 2026-04-04

Updated: 2026-04-05

## Summary

Rail pipeline is now split into two verified paths:

- Live path: `NS API -> GCS -> BigQuery -> dbt -> Streamlit`
- Historical path: `rijdendetreinen.nl archive -> GCS -> BigQuery -> dbt`

NS live ingestion has been refactored to source-native raw storage. Historical rail backfill is implemented through a manual-trigger Airflow DAG with month/year config. Streamlit pages and alerting code are in place. NDW remains the main open gap and still depends on manual Dexter CSV import.

---

## Tasks Completed

| Task | Status | Notes |
|------|--------|-------|
| Task 12: End-to-end ingestion verification | Done | NS pipeline fully verified against live API + real BQ |
| Task 17: dbt fact tables + data mart | Done | fct_train_performance (incremental), fct_road_traffic, dm_multimodal_daily |
| Task 18: Alerting | Done | alert_checker.py + dag_dbt_transform.py + unit tests |
| Historical rail backfill | Done | `dag_rdt_backfill` + `ingest_rdt.py` + dbt staging/intermediate integration |
| Streamlit pages | Done | Network Overview + Corridor Explorer read from BigQuery models |

## Infrastructure Setup

- Terraform apply: GCS bucket, 3 BQ datasets, service account + key — all provisioned
- Service account key copied to `airflow/keys/gcp-credentials.json`
- `.env` placed in `airflow/` (standard: same directory as `docker-compose.yml`)
- Docker Compose: airflow-init, webserver (8080), scheduler, and Streamlit running

---

## Bugs Found & Fixed

### 1. Airflow init — user creation command broken

**Symptom:** `airflow users create` arguments parsed as separate shell commands (`--username: command not found`).

**Root cause:** YAML `>` folding with multiline `command` caused shell to treat each continuation line as an independent command.

**Fix:** Changed to `- -c` + `- |` block scalar, put all args on one line.

```yaml
# Before (broken)
command: >
  -c "
  airflow db init &&
  airflow users create
    --username airflow
    ...
  "

# After (working)
command:
  - -c
  - |
    airflow db init &&
    airflow users create --username airflow --password airflow --firstname Admin --lastname User --role Admin --email admin@example.com
```

### 2. NS API v3 disruptions — response format mismatch

**Symptom:** `AttributeError: 'list' object has no attribute 'get'`

**Root cause:** `extract_disruptions` assumed `{"payload": [...]}` wrapper, but v3 `/disruptions` endpoint returns a bare JSON array `[{...}, ...]`.

**Fix:** Auto-detect response shape:
```python
data = response.json()
raw_disruptions = data if isinstance(data, list) else data.get("payload", [])
```

### 3. Station codes — 4 codes not accepted by departures API

**Symptom:** `404 Not Found` for stations `LD`, `AMS`, `DRN`, `EDE`.

**Root cause:** Original station codes were informal abbreviations, not the codes recognized by the NS departures API. Verified by exact name match against `/v2/stations` endpoint.

**Correct mapping (same stations, different codes):**

| Station Name | Old Code | Correct Code |
|---|---|---|
| Leiden Centraal | LD | LEDN |
| Amsterdam Amstel | AMS | ASA |
| Driebergen-Zeist | DRN | DB |
| Ede-Wageningen | EDE | ED |

**Important lesson:** When fixing station codes, must update ALL references consistently:
- `airflow/dags/dag_ns_ingest.py` (MVP_STATIONS list)
- `dbt/seeds/station_corridor_mapping.csv`
- `dbt/seeds/station_ndw_mapping.csv`

Initially made the mistake of replacing stations with *different* stations (e.g., AMS -> AMF which is Amersfoort, not Amsterdam Amstel). The correct approach is: look up the exact station name in the stations API, then use the code it returns.

### 4. BQ load — no schema on first run

**Symptom:** `400 No schema specified on job or table`

**Root cause:** `load_json_to_bq` in `bq_utils.py` did not set `autodetect=True`. On first run the table doesn't exist yet, so BQ needs schema from somewhere.

**Fix:** Added `autodetect=True` to `LoadJobConfig`.

### 5. GCS JSON format — array vs NDJSON

**Symptom:** `Failed to parse JSON: No object found when new array is started`

**Root cause:** `upload_json_to_gcs` wrote `json.dumps(data)` which produces a JSON array `[{...}, {...}]`. BigQuery's `NEWLINE_DELIMITED_JSON` format expects one JSON object per line.

**Fix:**
```python
# Before
blob.upload_from_string(json.dumps(data), ...)

# After
ndjson = "\n".join(json.dumps(row) for row in data)
blob.upload_from_string(ndjson, ...)
```

### 6. dbt custom schema — dataset name concatenation

**Symptom:** `Access Denied: User does not have bigquery.datasets.create permission`

**Root cause:** dbt's default `generate_schema_name` macro concatenates `target.schema` + `custom_schema`, producing dataset names like `staging_nl_transport_raw_nl_transport` which don't exist. Terraform only created the 3 base datasets.

**Fix:** Added `dbt/macros/generate_schema_name.sql` override that uses the custom schema name directly:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
```

### 7. dbt departure_id uniqueness — duplicate records

**Symptom:** `unique_stg_ns_departures_departure_id` test failed with 207 duplicates.

**Root cause:** Two issues compounded:
1. NS live API returns current departures regardless of `service_date`, so catchup runs for different dates fetch identical data
2. Same station + same planned time can have multiple trains going different directions

**Fix:** Expanded `departure_id` to include `service_date` and `direction`:
```sql
concat(station_code, '_', service_date, '_', planned_departure_ts, '_', direction) as departure_id
```

### 8. DAG catchup — rate limiting

**Symptom:** `SSLEOFError: EOF occurred in violation of protocol` on API calls.

**Root cause:** `catchup=True` with `max_active_runs=3` launched 3+ concurrent DAG runs, each hitting NS API for 14 stations. API rate-limited by dropping SSL connections.

**Fix:**
- Set `max_active_runs=1`
- Added `time.sleep(1)` between station API calls

### 9. DAG backfill OOM on full-year archives

**Symptom:** `local_task_job_runner.py:234 INFO - Task exited with return code -9` (Docker container killed by OOM).

**Root cause:** Historical `services-YYYY.csv.gz` full-year archives (like 2022) compress to ~370MB but decompress to 5-7GB of CSV data. The ingestion script parsed all rows into a single in-memory Python list before starting the GCS upload, which required ~15GB of RAM.

**Fix:** Switched to a streaming architecture:
- `ingest_rdt.py` decompresses the data on-the-fly and `yields` rows via a generator.
- `dag_rdt_backfill.py` and `dag_rdt_monthly.py` consume the stream, batching records into chunks of 200,000 and immediately uploading each chunk to GCS to free memory.

---

## Verified Data State

| Layer | Count | Status |
|-------|-------|--------|
| GCS live raw files | NS daily snapshots present | OK |
| BQ ns live raw | NS disruptions + departures loaded | OK |
| BQ rdt_services raw | 1,915,136 March 2026 stop records | OK |
| BQ rdt_disruptions raw | 6,676 rows for 2025 | OK |
| dbt seeds | 3 (corridors, NDW mapping, holidays) | OK |
| dbt staging | NS live + RDT historical staging models working | OK |
| int_departures_combined | ~104K departure records from March 2026 history | OK |
| int_train_delays_daily | 1,100 daily aggregates for March 2026 | OK |
| fct_train_performance | Updated to use combined live/history rail data | OK |
| dm_multimodal_daily | Train-side metrics populated; road-side fields still pending NDW | Partial |
| dbt tests | Rail path tests pass; NDW-dependent checks remain blocked by missing data | OK |

---

## Remaining Work

| Task | Description | Blocked? |
|------|-------------|----------|
| Task 22 | Generate dbt docs | No |
| Task 23 | Final README + end-to-end verification | No |
| NDW data | Manual Dexter CSV download + import | User action needed |
| NDW pipeline verify | stg_ndw_traffic_flow, int_ndw_traffic_daily, fct_road_traffic, dm_multimodal_daily | Blocked on NDW data |
| Optional: weather integration | Add external explanatory dimension for rail analytics | No |

---

## Current Architecture Notes

- `dag_ns_ingest` is for live daily collection only. `catchup=False` is intentional because NS departures are not a true historical API.
- `dag_rdt_backfill` is a manual backfill DAG. It accepts `services_months` and `disruptions_years` in `dag_run.conf`.
- NS live raw is now source-native. Business logic moved downstream into dbt staging.
- Historical and live rail data are unified in `int_departures_combined`.
- NDW is still semi-manual by design because Dexter export requires captcha-protected user interaction.
