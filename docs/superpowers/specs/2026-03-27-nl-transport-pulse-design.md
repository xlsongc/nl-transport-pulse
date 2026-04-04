# NL Transport Pulse — Design Spec

## 1. Product Overview

**NL Transport Pulse** is a Dutch multimodal transport reliability dashboard with disruption impact analysis. It serves two audiences: (1) transport analysts and city planners monitoring network health, and (2) interview panels evaluating end-to-end data engineering skills.

The product combines steady-state reliability monitoring ("how is the network performing?") with event-driven disruption analysis ("when trains fail, what happens to road traffic?"). An alerting layer detects anomalies and notifies via Slack.

This is the final project for DataTalksClub DE Zoomcamp 2026, targeting interviews at Booking.com and Uber Amsterdam.

### 1.1 Unified Time Semantics

All pipeline logic uses **`service_date`** as the canonical time dimension. `service_date` always refers to the date being analyzed, not the date the DAG runs. For daily DAGs, `service_date = execution_date = yesterday` (Airflow convention). All raw tables, facts, marts, alerts, and dashboard filters are keyed or filtered by `service_date`.

---

## 2. Data Sources (Tiered Investment)

### 2.1 NS API — Nederlandse Spoorwegen (Deep)

- **What**: Train departures, delays, disruptions, station info
- **API**: https://www.ns.nl/en/travel-information/ns-api (free tier, API key required)
- **Format**: REST API → JSON
- **Raw granularity**: Individual departure records and disruption events
- **Ingestion**: Daily batch at 06:00 UTC — extract previous service_date's disruptions + departures per station
- **GCS path**: `gs://{bucket}/raw/ns/disruptions/dt=YYYY-MM-DD/` and `.../departures/...`
- **BQ load**: Partition-scoped overwrite (see Section 5)
- **Historical availability**: NS API may expose recent disruption history (weeks). Supplement with NS open data portal annual exports. Confirm during Week 1.

### 2.2 NDW — Road Traffic (Medium)

- **What**: Road traffic flow — vehicle counts, average speed per measurement point
- **Source**: NDW Open Data Portaal
- **Format**: REST API / CSV dumps
- **Raw granularity**: Traffic observations at 15-minute intervals. Scope limited to NDW measurement points mapped to MVP corridors only (~50-80 points across ~5 corridors), not nationwide.
- **Ingestion**: Daily batch at 07:00 UTC — download previous service_date's 15-minute observations for mapped measurement points
- **GCS path**: `gs://{bucket}/raw/ndw/traffic_flow/dt=YYYY-MM-DD/`
- **BQ load**: Partition-scoped overwrite (see Section 5)
- **Why 15-minute grain**: Required for disruption impact analysis (pre/during/post windows). Aggregated to daily in downstream marts for overview dashboards.
- **Historical availability**: NDW open data portal may offer bulk CSV downloads of past measurements. Confirm during Week 1.

### 2.3 OVapi GTFS — Bus/Tram/Metro (Light — Stretch)

- **What**: GTFS static feeds — stops, routes, trips, stop_times, calendar
- **Source**: NDOV / OVapi
- **Format**: GTFS zip containing CSV files
- **Raw granularity**: Schedule-level (one record per stop_time per trip)
- **Ingestion**: Quarterly (or manual trigger) — download full zip, extract CSVs
- **GCS path**: `gs://{bucket}/raw/ovapi/gtfs/version=YYYY-MM-DD/`
- **BQ load**: WRITE_TRUNCATE (full refresh per version). Previous versions preserved in GCS by version path.
- **Historical availability**: No backfill needed — GTFS static is a point-in-time snapshot of current schedules.

---

## 3. Architecture

```
Data Sources (NS API, NDW, OVapi GTFS)
        |  Python ingestion scripts
        |  (service_date = execution_date)
        v
Google Cloud Storage (Data Lake — raw/)
        |  Airflow orchestration
        v
BigQuery (raw_nl_transport dataset)
        |  dbt Core transformations
        v
BigQuery (staging → intermediate → core)
        |                          |
        v                          v
Streamlit Dashboard          Slack Alerts
(NL Transport Pulse)     (via Airflow task)
```

### 3.1 Airflow DAGs

| DAG | Schedule | service_date | Tasks |
|-----|----------|-------------|-------|
| `dag_ns_ingest` | Daily 06:00 UTC | execution_date (= yesterday) | `extract_disruptions` >> `extract_departures` >> `upload_to_gcs` >> `load_to_bq` |
| `dag_ndw_ingest` | Daily 07:00 UTC | execution_date (= yesterday) | `extract_traffic` >> `upload_to_gcs` >> `load_to_bq` |
| `dag_ovapi_ingest` | Quarterly / manual | N/A (versioned snapshot) | `download_gtfs` >> `extract_csvs` >> `upload_to_gcs` >> `load_to_bq` |
| `dag_dbt_transform` | Daily 08:00 UTC | execution_date (= yesterday) | `dbt_seed` >> `dbt_run` >> `dbt_test` >> `dbt_source_freshness` >> `check_alerts` |

### 3.2 Key Design Decisions

- **Batch over streaming**: Daily batch for reliability analysis; 15-minute raw grain for NDW enables sub-daily disruption impact analysis without requiring streaming infrastructure. Streaming (GTFS-RT) is an out-of-scope stretch goal.
- **GCS before BigQuery**: Raw data preservation, reprocessing capability, decoupling ingestion from transformation.
- **dbt Core over dbt Cloud**: Keeps the project fully reproducible — everything runs inside Docker Compose.
- **Idempotent ingestion**: Partition-scoped overwrite — re-running a DAG for the same service_date deletes and reloads that partition. See Section 5.
- **Airflow over Kestra/Bruin**: Booking.com and Uber both use Airflow heavily — maximizes resume relevance.
- **NDW corridor scoping**: Ingest only measurement points mapped to MVP corridors, not nationwide data. Controls data volume and cost while delivering the disruption impact story.

---

## 4. Canonical Grain & Keys

Every core table has an explicit grain, primary key, and time field. No ambiguity.

| Table | Grain | Primary Key | Time Field | Partition Key |
|-------|-------|-------------|------------|---------------|
| `raw.ns_disruptions` | Disruption event | `disruption_id` | `service_date` | `service_date` |
| `raw.ns_departures` | Individual departure | `departure_id` (station_code + planned_departure_ts) | `service_date` | `service_date` |
| `raw.ndw_traffic_flow` | Measurement point × 15-min interval | `location_id + measurement_ts` | `service_date` | `service_date` |
| `raw.gtfs_*` | Per GTFS spec (stop, route, trip, stop_time) | Per GTFS spec | `version_date` | N/A (WRITE_TRUNCATE) |
| `stg_ns_disruptions` | Disruption event | `disruption_id` | `service_date` | — (view) |
| `stg_ns_departures` | Individual departure | `station_code + planned_departure_ts` | `service_date` | — (view) |
| `stg_ndw_traffic_flow` | Measurement point × 15-min interval | `location_id + measurement_ts` | `service_date` | — (view) |
| `int_train_delays_daily` | Station × Day | `station_code + service_date` | `service_date` | — |
| `int_ndw_traffic_daily` | NDW point × Day | `location_id + service_date` | `service_date` | — |
| `int_disruption_road_impact` | Disruption × NDW point | `disruption_id + location_id` | `disruption_start_ts` | — |
| `fct_train_performance` | Station × Day | `station_code + service_date` | `service_date` | `service_date` |
| `fct_road_traffic` | NDW point × Day | `location_id + service_date` | `service_date` | `service_date` |
| `fct_transit_service` | Route × Day | `route_id + service_date` | `service_date` | — |
| `dm_multimodal_daily` | Corridor × Day | `corridor_id + service_date` | `service_date` | `service_date` |
| `dm_disruption_impact` | Disruption event | `disruption_id` | `disruption_start_ts` | — |
| `alert_history` | Alert check | `alert_id` (auto-generated) | `check_ts` | — |

---

## 5. Idempotency & Backfill Rules

### 5.1 Core Rule

All daily ingestion DAGs are idempotent. Re-running a DAG for the same `service_date` produces the same result as the first run:

- **Raw tables**: Partitioned by `service_date`. On re-run, delete the target `service_date` partition first, then reload. No duplicates possible.
- **dbt incremental models** (`fct_train_performance`): Use `unique_key` = `station_code + service_date`. Re-processing a date merges (upserts), not appends.
- **dbt snapshot tables** (`dim_routes`, `dim_stations`): Snapshots are idempotent by design — re-running with unchanged data produces no new rows.

### 5.2 Backfill Execution

Backfill is a date-range DAG run:

```bash
airflow dags backfill dag_ns_ingest --start-date 2026-03-01 --end-date 2026-03-26
```

Each execution processes one `service_date`. Partition-scoped overwrite ensures any partial or failed backfill can be safely re-run.

### 5.3 GTFS Versioning

GTFS loads use WRITE_TRUNCATE (full table replacement in BQ). Previous versions are preserved in GCS at `gs://{bucket}/raw/ovapi/gtfs/version=YYYY-MM-DD/`. To restore a previous version, re-run the DAG pointing at the desired GCS version path.

### 5.4 Summary

| Layer | Strategy | Re-run Behavior |
|-------|----------|----------------|
| GCS raw | Overwrite by path (dt=YYYY-MM-DD) | File replaced |
| BQ raw (NS, NDW) | Partition-scoped delete + reload | Partition replaced |
| BQ raw (GTFS) | WRITE_TRUNCATE | Full table replaced, old version in GCS |
| dbt incremental | Upsert via unique_key | Row merged |
| dbt snapshot | Check strategy | No-op if unchanged |
| alert_history | Append-only | Duplicate alert_id check in SQL |

---

## 6. dbt Model Architecture

### 6.1 Dimensional Modeling

Star schema with fact tables at daily grain, dimension tables for entities, and data marts for pre-joined consumption layers.

### 6.2 Staging Layer (materialized as views)

1:1 with source tables. Clean, type-cast, rename.

| Model | Source | Purpose |
|-------|--------|---------|
| `stg_ns_disruptions` | `raw.ns_disruptions` | Parsed disruption events with start/end timestamps, affected stations, cause, duration_minutes |
| `stg_ns_departures` | `raw.ns_departures` | Departure records with `delay_minutes` = actual - planned |
| `stg_ndw_traffic_flow` | `raw.ndw_traffic_flow` | Cleaned 15-minute measurement point readings (avg_speed_kmh, vehicle_count, location_id, measurement_ts) |
| `stg_gtfs_stops` | `raw.gtfs_stops` | Stop locations with lat/lon |
| `stg_gtfs_routes` | `raw.gtfs_routes` | Route definitions |
| `stg_gtfs_stop_times` | `raw.gtfs_stop_times` | Scheduled stop times |

### 6.3 Intermediate Layer

Business logic and aggregation.

| Model | Grain | Purpose |
|-------|-------|---------|
| `int_train_delays_daily` | Station × Day | Aggregate departure-level delays to daily stats: avg_delay_min, pct_on_time, max_delay_min, total_departures, disruption_count |
| `int_ndw_traffic_daily` | NDW point × Day | Aggregate 15-min readings to daily: avg_speed_kmh, total_vehicle_count, congestion_minutes (minutes where avg speed < 50% of free-flow speed for that point) |
| `int_disruption_road_impact` | Disruption × NDW point | Match NS disruption events to nearby NDW points (via seed mapping). For each pair, compute: avg_speed_pre (2h before), avg_speed_during, avg_speed_post (2h after), vehicle_count_pre/during/post, speed_deviation_pct from baseline. Requires 15-min NDW grain. |

### 6.4 Dimension Tables

| Model | Type | Key Attributes |
|-------|------|----------------|
| `dim_stations` | SCD Type 2 (dbt snapshot) | station_code, station_name, station_type, num_platforms, lat, lon, city, corridor. Tracked: station_name, station_type, num_platforms. SCD columns: valid_from, valid_to, is_current |
| `dim_ndw_locations` | Type 1 | location_id, road_name, lat, lon, nearest_station_code, corridor_id |
| `dim_routes` | SCD Type 2 (dbt snapshot) | route_id, route_short_name, route_long_name, route_type, agency_id, num_daily_trips. Tracked: route_short_name, route_long_name, route_type, num_daily_trips. SCD columns: valid_from, valid_to, is_current |
| `dim_date` | Static reference | date, day_of_week, week_number, month, season, is_holiday, is_weekend |

### 6.5 Fact Tables

| Model | Materialization | Grain | Key Metrics |
|-------|----------------|-------|-------------|
| `fct_train_performance` | Incremental (unique_key: `station_code + service_date`) | Station × Day | `avg_delay_min`, `max_delay_min`, `pct_on_time`, `severe_delay_share` (pct of departures with delay > 15 min), `total_departures`, `disruption_count` |
| `fct_road_traffic` | Table | NDW point × Day | `avg_speed_kmh`, `total_vehicle_count`, `congestion_minutes`, `speed_vs_baseline_pct` |
| `fct_transit_service` | Table | Route × Day | `scheduled_trips`, `num_stops_served` |

Note: `delay_category` is a **macro**, not a fact column. It is applied downstream in marts or Streamlit for display purposes: on_time (0-1 min), minor (1-5), moderate (5-15), severe (15+).

### 6.6 Data Marts

| Model | Grain | Purpose | Powers |
|-------|-------|---------|--------|
| `dm_multimodal_daily` | Corridor × Day | Joins train performance + road traffic + transit service by corridor and service_date | Network Overview, Corridor Explorer |
| `dm_disruption_impact` | Disruption event | One row per disruption with: corridor, duration, stations_affected, and pre/during/post road traffic metrics (avg_speed, vehicle_count, speed_deviation_pct) from nearby NDW points | Incidents page |

### 6.7 Seeds

| Seed | Purpose |
|------|---------|
| `station_corridor_mapping.csv` | Assigns each NS station to a corridor (e.g., Amsterdam–Rotterdam) |
| `station_ndw_mapping.csv` | Maps ~20-30 major NS stations to nearest NDW measurement points (manually curated, MVP corridors only) |
| `nl_holidays.csv` | Dutch public holidays for dim_date |

### 6.8 Macros

- `delay_category(delay_minutes)` — classifies delays: on_time (0-1 min), minor (1-5), moderate (5-15), severe (15+). Used in display layers, not stored in facts.
- `is_test_run` variable — limits to 1 month of data in dev, full history in prod

### 6.9 dbt Features Demonstrated

| Feature | Where |
|---------|-------|
| Incremental materialization | `fct_train_performance` with `is_incremental()` + `unique_key` |
| SCD Type 2 snapshots | `dim_routes`, `dim_stations` with `check` strategy |
| Sources + freshness | `loaded_at_field` with warn/error thresholds on raw tables |
| Tests | not_null, unique, accepted_values, custom: pct_on_time between 0-100 |
| Variables | `is_test_run` for dev/prod scope control |
| Documentation | `dbt docs generate` with column descriptions |

---

## 7. Station-to-NDW Mapping

The disruption impact analysis requires connecting train stations to road traffic measurement points.

**Approach**: Manually curated seed CSV (`station_ndw_mapping.csv`).

**Process**:
1. Get station lat/lon from NS API station endpoint
2. Get NDW measurement point lat/lon from their metadata endpoint
3. For ~20-30 major stations on MVP corridors, identify NDW points within ~5km on major connecting roads
4. Curate manually — pick 2-3 most relevant road sensors per station

**Why manual over spatial joins**: With only ~20-30 stations, manual curation is more accurate and avoids PostGIS/BigQuery GIS complexity. Easy to explain in interviews.

**Fallback**: If NDW metadata lacks clean coordinates, match by city/region name instead.

---

## 8. Alerting Layer

### 8.1 Detection

A final task (`check_alerts`) in `dag_dbt_transform`, running after `dbt_test`.

Two SQL checks against the **latest complete service_date** (= execution_date = yesterday):

1. **Reliability alert**: Corridors where the latest complete service_date's `pct_on_time` (from `dm_multimodal_daily`) < threshold (default 80%) AND > 10% worse than 7-day rolling average
2. **Disruption alert**: New entries in `dm_disruption_impact` for the latest complete service_date where `duration_minutes > 60` or `stations_affected > 5`

### 8.2 Notification

POST to Slack webhook. Message format:

```
Alert: NL Transport Pulse
Service date: 2026-03-26
Corridor: Amsterdam – Rotterdam
On-time: 62% (7-day avg: 87%)
Active disruptions: 2
View in dashboard: {app-url}/incidents?date=2026-03-26
```

### 8.3 Alert History

Every alert check is written to `core.alert_history` table in BigQuery:

| Column | Type | Description |
|--------|------|-------------|
| `alert_id` | STRING | Auto-generated UUID |
| `check_ts` | TIMESTAMP | When the check ran |
| `service_date` | DATE | The service_date being evaluated |
| `alert_type` | STRING | `reliability` or `disruption` |
| `corridor` | STRING | Corridor name |
| `metric_value` | FLOAT | The actual value (e.g., on_time_pct) |
| `threshold` | FLOAT | The threshold that was checked |
| `fired` | BOOLEAN | Whether the alert was triggered |

### 8.4 Configuration

Thresholds stored as Airflow Variables:
- `on_time_threshold`: 80 (percent)
- `deviation_threshold`: 10 (percent below rolling average)

---

## 9. Frontend — Streamlit App

### 9.1 Pages

Each page reads exclusively from core/mart tables. No direct raw table access from the frontend.

| Page | Source Table(s) | Required Fields | Key Components |
|------|----------------|-----------------|----------------|
| **Network Overview** | `dm_multimodal_daily`, `dim_stations` | service_date, corridor_id, pct_on_time, avg_delay_min, total_vehicle_count, congestion_minutes, lat, lon | NL map with color-coded corridors, scorecards (overall on-time %, active disruptions, worst corridor), trend sparklines. Date picker. |
| **Corridor Explorer** | `fct_train_performance`, `fct_road_traffic`, `dim_stations`, `dim_ndw_locations` | station_code, service_date, pct_on_time, avg_delay_min, severe_delay_share, avg_speed_kmh, corridor | Corridor/station selector, reliability time series, delay distribution, peak vs off-peak, side-by-side corridor comparison. Date range filter. |
| **Incidents** | `dm_disruption_impact`, `dim_stations` | disruption_id, corridor, service_date, duration_minutes, stations_affected, avg_speed_pre, avg_speed_during, avg_speed_post, speed_deviation_pct | Sortable disruption table. Click into detail: before/during/after road traffic chart, affected stations list. Filter by: date range, corridor, min duration. |
| **Alerts Log** | `core.alert_history` | alert_id, check_ts, service_date, alert_type, corridor, metric_value, threshold, fired | Simple table. Filter by: date range, alert_type, fired status. |

### 9.2 Interactivity

- Global date range picker (persists across pages via `st.session_state`)
- Corridor/station dropdown selectors
- Incidents: filter by date range, corridor, minimum duration
- Alerts Log: filter by alert_type and fired status
- Corridor Explorer: side-by-side corridor comparison

### 9.3 Chart Library

Start with Plotly (default Streamlit). Upgrade to ECharts or Deck.gl maps later if needed. The pipeline is the priority, not chart polish.

### 9.4 BigQuery Connection

`google-cloud-bigquery` Python client. Queries against core/mart tables only. Cache results with `@st.cache_data` where appropriate.

---

## 10. Reproducibility & Deployment

### 10.1 Docker Compose Stack

| Service | Purpose |
|---------|---------|
| `airflow-webserver` | Airflow UI |
| `airflow-scheduler` | DAG scheduling and execution |
| `airflow-postgres` | Airflow metadata database |
| `streamlit` | Dashboard app |

All services share a `.env` file: GCP project ID, credentials path, dataset names, Slack webhook URL.

### 10.2 Terraform

Provisions:
- GCS bucket for raw data lake
- BigQuery datasets: `raw_nl_transport`, `staging_nl_transport`, `core_nl_transport`
- Service account with minimal IAM: BigQuery Data Editor, GCS Object Admin

### 10.3 dbt Core

Runs inside the Airflow container via `BashOperator` or `dbt-airflow` package. Not dbt Cloud — keeps everything self-contained and reproducible.

### 10.4 README

Must cover:
- Prerequisites: Docker, GCP account, Terraform, NS API key
- Quick start: `terraform apply` → `docker compose up` → data flows
- How to trigger backfill: `airflow dags backfill <dag> --start-date ... --end-date ...`
- Dashboard URL (localhost:8501 or Streamlit Cloud)
- Architecture diagram
- Screenshot of dashboard

---

## 11. MVP vs Stretch

### MVP (must ship)

| Component | Scope |
|-----------|-------|
| Data sources | NS API + NDW (15-min grain, MVP corridors only) |
| Ingestion | 2 Airflow DAGs (NS, NDW) with partition-scoped overwrite |
| dbt models | Full staging → intermediate → core pipeline for NS + NDW |
| Fact tables | `fct_train_performance` (incremental), `fct_road_traffic` |
| Data mart | `dm_multimodal_daily` (corridor × day) |
| Dashboard | Network Overview + Corridor Explorer (2 pages) |
| Alerting | 1 alert type: corridor reliability below threshold → Slack |
| Infrastructure | Terraform + Docker Compose + dbt Core |
| SCD | `dim_stations` Type 2 snapshot |
| Backfill | At least 2 weeks of NS + NDW data |

### Stretch (if time permits)

| Component | Scope |
|-----------|-------|
| OVapi GTFS | Ingest, model `fct_transit_service`, add to `dm_multimodal_daily` |
| `dm_disruption_impact` | Event-level impact analysis with pre/during/post road traffic |
| Incidents page | Disruption detail view with road traffic impact chart |
| Alerts Log page | Alert history table with filters |
| Disruption alert | Second alert type: high-severity disruption → Slack |
| `dim_routes` SCD Type 2 | Snapshot with check strategy on GTFS route changes |
| Map enhancement | Deck.gl or ECharts map with corridor overlays |
| Streamlit Cloud deploy | Shareable public URL |

### Ordering Principle

Ship MVP first. Each stretch item is independent and can be added without changing MVP code. OVapi and disruption impact are highest-value stretch items for interviews.

---

## 12. Timeline (4 Weeks)

| Week | Phase | Key Deliverables |
|------|-------|-----------------|
| Week 1 | Infrastructure + Data Discovery | Terraform setup, Docker Compose (Airflow + Streamlit), GCS/BQ provisioned. Confirm NS API endpoints and rate limits. Confirm NDW 15-min data format and download method. Build seed CSVs (station_corridor_mapping, station_ndw_mapping). Start daily collection. |
| Week 2 | Ingestion | NS + NDW ingestion scripts. 2 Airflow DAGs running daily with partition-scoped overwrite. Backfill historical data (target: 2+ weeks). Data flowing into GCS → BigQuery. |
| Week 3 | Transformation | dbt project: staging, intermediate, core models. `fct_train_performance` incremental. `dim_stations` SCD snapshot. Tests. `dm_multimodal_daily`. Alerting task + Slack webhook. |
| Week 4 | Dashboard + Stretch + Polish | Streamlit MVP (2 pages). If time: add stretch items (OVapi, disruption impact, incidents page). README. Architecture diagram. Zoomcamp submission. |

---

## 13. Zoomcamp Evaluation Coverage

| Criteria | How This Project Covers It | Target |
|----------|---------------------------|--------|
| Problem description | README with business questions, data sources, architecture diagram | 4/4 |
| Cloud | Terraform for GCS + BigQuery + IAM + Service Account | 4/4 |
| Data ingestion (batch) | Airflow DAGs for 2-3 APIs → GCS → BQ, with backfill and idempotent reruns | 4/4 |
| Data warehouse | BigQuery with service_date partitioning + corridor/station clustering | 4/4 |
| Transformations (dbt) | Full dbt project: staging/intermediate/core, SCD, incremental, tests, docs | 4/4 |
| Dashboard | Streamlit with 2-4 pages, interactive filters, map visualization | 4/4 |
| Reproducibility | Docker Compose + Terraform + README + seeds + backfill instructions | 4/4 |

---

## 14. Interview Talking Points

1. **"How do you design an end-to-end pipeline?"** → Walk through the architecture diagram, explain each layer's purpose and the service_date convention.
2. **"How do you handle heterogeneous data sources?"** → Two+ sources, different formats (JSON API, CSV dumps), tiered investment strategy, different ingestion cadences.
3. **"How do you ensure data quality?"** → dbt tests, source freshness checks, idempotent partition-scoped overwrite, SCD for dimension history.
4. **"Walk me through your dbt modeling approach"** → Star schema, staging/intermediate/core, incremental facts with unique_key, SCD Type 2 snapshots with check strategy.
5. **"Why batch over streaming?"** → 15-min NDW grain gives sub-daily analysis without streaming complexity. Daily batch sufficient for reliability monitoring. Streaming is a documented stretch goal.
6. **"How would you scale this?"** → Expand NDW to nationwide (more measurement points), partition + cluster BQ tables, move to CeleryExecutor, add GTFS-RT streaming via Pub/Sub, replace manual NDW mapping with BigQuery GIS.
7. **"Tell me about a data challenge you solved"** → Station-to-NDW mapping for disruption impact analysis. Manual curation for accuracy at MVP scale, with clear path to automation via spatial joins.
8. **"How do you monitor your pipelines?"** → Alerting layer: Airflow detects anomalies post-dbt on the latest complete service_date, Slack notification with link to dashboard, alert history table for audit.

---

## 15. Out of Scope

- GTFS-Realtime (streaming vehicle positions / trip updates)
- Real-time dashboard (WebSocket / auto-refresh)
- User authentication or multi-tenancy
- CBS ridership statistics
- RDW vehicle registration data
- Nationwide NDW coverage (MVP is corridor-scoped)
- Custom domain or production hosting (beyond Streamlit Cloud)
- Mobile-responsive design
- Alert acknowledgment, escalation, or PagerDuty integration
