# Interview Prep — Data Engineering Talking Points

## Idempotent Ingestion & Backfill Strategy

### Q: How do you ensure your pipeline doesn't create duplicates on re-runs?

We use **partition-scoped overwrite** — a generic `load_json_to_bq` utility that takes a `partition_field` and `service_date` as parameters. Before loading any data, it deletes existing rows for that partition, then appends fresh data:

```sql
DELETE FROM `{table_id}` WHERE {partition_field} = '{service_date}'
-- then WRITE_APPEND from GCS
```

The same function handles all tables — the caller decides the scope:

| Table | Partition Field | Granularity | Example |
|-------|----------------|-------------|---------|
| `ns_departures` | `_service_date` | Daily | `WHERE _service_date = '2026-04-05'` |
| `rdt_services` | `_year_month` | Monthly | `WHERE _year_month = '2026-01'` |
| `rdt_disruptions` | `_year` | Yearly | `WHERE _year = '2025'` |

This means you can re-run any DAG for the same date/month/year 10 times and always get the same result. No duplicates, no manual cleanup needed.

### Q: How does backfill work?

Two paths — live and historical:

- **Live path**: `dag_ns_ingest` runs daily at 06:00 UTC, fetches yesterday's data from the NS API. `catchup=False` because the NS departures endpoint only returns current data, not historical.
- **Historical path**: `dag_rdt_backfill` is a manual-trigger DAG that downloads archived data from rijdendetreinen.nl. Operators pass months/years as config params via the Airflow UI.

The backfill DAG handles massive datasets (like full-year 5-7GB archives) by processing downloads as a stream. It decompresses the CSV on-the-fly and yields rows to Airflow, which chunks the uploads (200K records per chunk) to GCS. This avoids holding millions of rows in memory and prevents Docker OOM kills (exit code -9). It uses `skip_delete` on subsequent chunks so only the first chunk triggers the BigQuery partition delete.

### Q: Why is the backfill manual instead of automated?

Pragmatic tradeoff. The historical source (rijdendetreinen.nl) is a static archive — it doesn't change once published. There's no value in scheduling recurring pulls. A config-driven manual DAG lets operators load exactly the months they need in controlled batches.

This is common in production too — initial backfills, data source migrations, and one-time historical loads are almost always manual triggers with documented parameters. What matters is that the process is **idempotent and re-runnable**, not that it's scheduled.

### Q: What happens downstream after backfill?

Once raw data lands in BigQuery:

1. `dag_dbt_transform` runs (manually triggered or on schedule)
2. dbt staging models (views) immediately reflect new raw data
3. `int_departures_combined` merges live NS + historical RDT data
4. `fct_train_performance` (incremental, `unique_key = [station_code, service_date]`) upserts — no duplicates at the fact layer either
5. `dm_multimodal_daily` aggregates to corridor-level for the Streamlit dashboard

The idempotency guarantee flows through every layer: raw (partition delete), staging (views), facts (upsert via unique_key), snapshots (no-op if unchanged).

### Q: How would you improve this?

- **Skip-already-loaded optimization**: Query BigQuery for existing partitions before downloading, skip months that are already loaded. Saves bandwidth and compute on repeated runs.
- **Data validation gate**: After loading, run a row-count or checksum comparison between source and BigQuery before marking the task as success.
- **Alerting on backfill**: Notify Slack when a backfill batch completes with row counts, so the team knows new data is available.

## Data Volume & Coverage

| Layer | Rows | Coverage |
|-------|------|----------|
| `rdt_services` (raw) | ~5.6M | Jan-Mar 2026 (3 months) |
| `rdt_disruptions` (raw) | ~17.8K | 2023-2025 (3 years) |
| `ns_departures` (raw, live) | ~600 | Apr 4-5 2026 (daily collection ongoing) |
| `fct_train_performance` (core) | ~47K | Jan 1 - Apr 5, 2026 |
| `dm_multimodal_daily` (mart) | ~460 | Jan 1 - Apr 5, 2026 |

Available for backfill on rijdendetreinen.nl: services from 2019-01 onward, disruptions from 2011 onward.

## Manual vs Automated Boundary

| Step | Manual/Automated | Why |
|------|-----------------|-----|
| API key registration (NS) | Manual | One-time setup, requires human verification |
| Terraform apply | Manual | Infrastructure provisioning is a deliberate action |
| Historical backfill trigger | Manual | One-time load, operators choose scope via params |
| NDW Dexter CSV download | Manual | Captcha-protected, no API available |
| Daily NS ingestion | Automated | `dag_ns_ingest` at 06:00 UTC |
| dbt transforms + tests | Automated | `dag_dbt_transform` at 08:00 UTC |
| Alerting (Slack) | Automated | Post-dbt task checks reliability thresholds |
| Dashboard refresh | Automated | Streamlit reads from BigQuery on each page load |

In production, documenting this boundary clearly is just as important as the automation itself. Every team has manual steps — the professional approach is to make them safe (idempotent), documented, and minimal.

---

## dbt Modeling Decisions

### Q: Walk me through your dbt modeling approach.

Star schema with four layers: staging → intermediate → core (facts + dimensions) → data mart.

```
Raw (BigQuery)
 └─ Staging (views) — 1:1 with source, clean + type-cast
     ├─ stg_ns_departures        (live API)
     ├─ stg_rdt_services          (historical archive)
     ├─ stg_ns_disruptions
     ├─ stg_rdt_disruptions
     └─ stg_ndw_traffic_flow
         │
 └─ Intermediate (tables) — business logic + aggregation
     ├─ int_departures_combined   (merge live + historical)
     ├─ int_train_delays_daily    (station × day aggregates)
     └─ int_ndw_traffic_daily     (road measurement × day)
         │
 └─ Core (tables) — consumption-ready
     ├─ fct_train_performance     (incremental, partitioned)
     ├─ fct_road_traffic
     ├─ dim_stations, dim_date, dim_ndw_locations
     └─ dm_multimodal_daily       (corridor × day data mart)
```

**Materializations are chosen by purpose:**
- **Staging = views**: No data duplication, always reflects current raw. Cheap to maintain.
- **Intermediate = tables**: Expensive joins and aggregations (e.g., merging two sources, daily rollups) — materialize to avoid recomputing on every downstream query.
- **Core facts = tables with partitioning + clustering**: Optimized for dashboard queries that filter by date range and station/corridor.
- **One exception**: `fct_train_performance` is **incremental** because it's the largest fact table and only recent days change.

### Q: How do you handle multiple data sources for the same entity?

The NS live API and rijdendetreinen.nl (RDT) archive both provide train departure data, but with very different characteristics:

| | NS Live API | RDT Archive |
|---|---|---|
| Coverage | ~20-40 upcoming departures per snapshot | Every stop of every train, full day |
| Freshness | Real-time (today's data now) | Monthly (published after the month ends) |
| Completeness | ~5-10% of daily departures | 100% |

`int_departures_combined` merges them with a **source priority** strategy:

```sql
-- RDT has full-day coverage — always prefer it
select * from rdt_departures
union all
select * from api_departures
where service_date not in (select distinct service_date from rdt_departures)
```

RDT takes priority at the **date level**: if RDT has data for a date, all live API rows for that date are excluded. This is intentional — RDT is always more complete for any given day, so mixing partial live data with complete archive data would create inconsistencies.

The model also uses `adapter.get_relation()` to check if the RDT table exists at all. If someone runs the pipeline before loading historical data, it gracefully falls back to API-only mode. No failure, no manual config needed.

### Q: Why is the NS live API incomplete? How did you handle this?

This is a common data engineering problem: **real-time APIs are built for consumer UIs, not analytics**. The NS departures endpoint returns "what's on the departure board right now" (~20-40 upcoming trains), not "every train that ran today." There's no historical endpoint.

This shows up everywhere:

- Booking.com: room availability API shows current state, not historical pricing
- Uber: surge pricing API is real-time, not exportable for analysis
- Flight trackers: show current positions, not historical flight paths

The standard solution is a **dual-source architecture**: use the real-time API as a bridge for freshness, then reconcile with a complete source when it becomes available. Our pipeline does exactly this:

1. `dag_ns_ingest` runs daily, captures whatever's on the board — provides data for the last few days
2. `dag_rdt_backfill` loads the monthly archive once published — provides complete historical record
3. `int_departures_combined` merges with archive priority — fact table is always as complete as possible

**Improvements considered:**
- Poll 4x daily (06:00, 09:00, 13:00, 18:00) to capture ~20-30% instead of ~5%
- Add a reconciliation DAG that compares live vs archive counts after backfill, flagging stations where live coverage was below threshold
- Switch to GTFS-Realtime streaming (Pub/Sub → BigQuery) for near-complete capture without waiting for the archive

### Q: Explain your incremental model strategy.

`fct_train_performance` is the only incremental model, chosen because it's the largest fact table (station × day grain, growing daily).

```sql
config(
    materialized='incremental',
    unique_key=['station_code', 'service_date'],
    partition_by={"field": "service_date", "data_type": "date", "granularity": "day"},
    cluster_by=['station_code']
)
```

**How it works:**
- **Full refresh** (first run or `--full-refresh`): Processes all data, builds the full table with partitioning and clustering.
- **Incremental run** (daily): Only processes the last 3 days (`where service_date >= date_sub(current_date(), interval 3 day)`). The `unique_key` ensures upsert behavior — if a row for the same station + date already exists, it gets replaced.

**Why a 3-day lookback instead of just 1 day:**
- The live API may capture partial data on day 1, then more data on day 2 (if polled again)
- Upstream data corrections can affect recent days
- Low cost: reprocessing 3 days of ~14 stations is trivial

**Why not make everything incremental:**
- `dm_multimodal_daily` is a full table rebuild — it's an aggregation of `fct_train_performance` by corridor. Small enough that incremental adds complexity without meaningful savings.
- Staging models are views — no materialization cost at all.
- Intermediate models are full table rebuilds — they merge two sources, so incremental logic would be complex and error-prone.

**Partitioning + clustering strategy:**

| Model | Partition | Cluster | Why |
|-------|-----------|---------|-----|
| `fct_train_performance` | `service_date` (day) | `station_code` | Dashboard queries filter by date range, then drill into station |
| `fct_road_traffic` | `service_date` (day) | `location_id` | Same pattern for road data |
| `dm_multimodal_daily` | `service_date` (day) | `corridor_id` | Dashboard overview filters by corridor |

This means a query like "show me Amsterdam-Rotterdam corridor for the last 30 days" scans only the relevant partitions and cluster blocks — not the full table.

### Q: How do you handle dimension changes over time?

`snap_dim_stations` is an SCD Type 2 snapshot using dbt's built-in snapshot feature:

```sql
config(
    strategy='check',
    check_cols=['station_name', 'corridor_id', 'corridor_name', 'lat', 'lon'],
    unique_key="station_code || '-' || cast(corridor_id as string)"
)
```

- **Check strategy** (not timestamp): Compares actual column values each run. If any tracked column changes, it closes the old row (`dbt_valid_to` is set) and inserts a new row (`dbt_valid_from` = now).
- **Composite unique key**: `station_code + corridor_id` because a station can belong to multiple corridors (e.g., Utrecht is on both Amsterdam-Utrecht and Amsterdam-Eindhoven).
- **Why SCD Type 2**: Station metadata can change — platforms get added, names change, corridors get reorganized. Keeping history means fact table joins remain accurate for historical analysis.

In practice, with only 14 MVP stations sourced from a seed CSV, changes are rare. But this demonstrates the pattern for interviews and would scale if stations came from a live API.

### Q: What dbt features does the project demonstrate?

| Feature | Where | Why |
|---------|-------|-----|
| Incremental materialization | `fct_train_performance` | Efficient daily processing of growing fact table |
| SCD Type 2 snapshots | `snap_dim_stations` | Track dimension changes over time |
| Dynamic source detection | `int_departures_combined`, `dm_multimodal_daily` | `adapter.get_relation()` for graceful degradation when a source isn't loaded yet |
| Sources + freshness | `_staging__sources.yml` | `loaded_at_field` with warn (36h) / error (48h) thresholds on raw tables |
| Generic tests | `_core__models.yml` | `not_null`, `unique` on keys across all core models |
| Custom tests | `tests/assert_pct_on_time_range.sql` | Validates business logic (on-time % is 0-100) |
| Variables | `is_test_run` | Limits data scope in dev (30 days) vs prod (full history) |
| Macros | `delay_category()` | Reusable delay classification: on_time (≤1 min), minor (≤5), moderate (≤15), severe (>15) |
| Schema override | `generate_schema_name.sql` | Uses custom schema directly instead of dbt's default concatenation (`target.schema + custom_schema`) |
| Seeds | 3 CSV files | `station_corridor_mapping`, `station_ndw_mapping`, `nl_holidays` — reference data maintained in version control |
