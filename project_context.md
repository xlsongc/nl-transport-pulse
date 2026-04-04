# Dutch Multimodal Transport Analytics — DE Project Context

## 1. Background & Goals

### Who
- **Name**: Xiaolong Song
- **Current role**: Data Engineer at Zehnder Group (using Microsoft Fabric)
- **Target companies**: Booking.com (Amsterdam), Uber (Amsterdam)
- **Learning path**: DataTalksClub DE Zoomcamp 2026 (completed Modules 1-4)
- **This project**: Zoomcamp final project, also intended for resume/portfolio

### Why This Topic
- Living/working in the Netherlands — local data adds authenticity
- Transport/mobility domain directly relevant to Uber
- Booking.com HQ is in Amsterdam — Dutch context is a plus in interviews
- Avoids the overused NYC taxi dataset that every Zoomcamp student uses

### Interview Angle
Key talking points this project should enable:
- "How do you design an end-to-end data pipeline?"
- "How do you handle multiple heterogeneous data sources?"
- "How do you ensure data quality in production?"
- "Walk me through your dbt modeling approach"
- "Why did you choose batch over streaming?"
- "How would you scale this?"

---

## 2. Agreed Tech Stack

| Component | Tool | Reason |
|-----------|------|--------|
| Containerization | Docker + Docker Compose | Reproducibility, Zoomcamp requirement |
| Infrastructure as Code | Terraform | Provision GCS, BigQuery, IAM on GCP |
| Workflow Orchestration | **Airflow** (not Kestra/Bruin) | Resume relevance — Booking & Uber both use Airflow heavily |
| Data Lake | Google Cloud Storage (GCS) | Raw data preservation, decoupled from warehouse |
| Data Warehouse | BigQuery | Already set up from Zoomcamp, free tier available |
| Data Transformation | dbt (Cloud or Core) | Layered modeling, tests, docs — already practiced in Module 4 |
| Visualization | Looker Studio | Free, BigQuery-native, shareable |
| Version Control | Git + GitHub | PR workflow, project showcase |

### Key Decision: Airflow over Kestra/Bruin
The Zoomcamp suggests Kestra or Bruin, but we chose Airflow specifically because:
- Booking.com and Uber are heavy Airflow users
- Much higher recognition on resumes
- Larger community and interview question pool
- Docker-based Airflow setup is well-documented

---

## 3. Data Sources

### 3.1 NS API (Nederlandse Spoorwegen — Dutch Railways)
- **What**: Train departures, delays, disruptions, station info, crowdedness
- **API**: https://www.ns.nl/en/travel-information/ns-api
- **Auth**: Free API key (register at NS developer portal)
- **Format**: REST API returning JSON
- **Update frequency**: Real-time, but we'll do daily batch pulls
- **Ingestion strategy**: Daily extract of yesterday's disruptions + departures per station
- **GCS path**: `gs://bucket/raw/ns/disruptions/dt=YYYY-MM-DD/`
- **BQ load**: WRITE_APPEND

### 3.2 OVapi / NDOV (All Dutch Public Transit)
- **What**: GTFS static feeds covering bus, tram, metro across the Netherlands (stops, routes, trips, stop_times); also GTFS-Realtime with vehicle positions and trip updates
- **Source**: NDOV / OVapi — first European agency to publish realtime data as open data
- **Format**: GTFS (CSV files in zip) + GTFS-RT (Protobuf, optional stretch goal)
- **Coverage**: Almost all bus/tram lines, Rotterdam metro, Amsterdam metro
- **Update frequency**: Static GTFS quarterly; RT is continuous
- **Ingestion strategy**: Download full GTFS zip, extract CSVs, load to GCS then BQ
- **GCS path**: `gs://bucket/raw/ovapi/gtfs/version=YYYY-MM-DD/`
- **BQ load**: WRITE_TRUNCATE (full refresh each quarter for static data)
- **Reference**: Crowdedness visualization article on Towards Data Science using OVInfo + NDOV

### 3.3 NDW (Nationaal Dataportaal Wegverkeer — Road Traffic)
- **What**: Road traffic flow measurements — vehicle counts, speeds, congestion per measurement point
- **Source**: NDW Open Data Portaal
- **Format**: REST API / CSV dumps
- **Update frequency**: Measurement every 1-5 minutes; daily aggregates available
- **Ingestion strategy**: Daily download of aggregated traffic counts
- **GCS path**: `gs://bucket/raw/ndw/traffic_flow/dt=YYYY-MM-DD/`
- **BQ load**: WRITE_APPEND

### 3.4 Optional: CBS (Centraal Bureau voor de Statistiek)
- Station-level ridership statistics (monthly/yearly)
- Could enrich station dimension with passenger volume

### 3.5 Optional: RDW (Vehicle Registration)
- Dutch vehicle registration data (fuel types, EV share, emissions)
- Millions of records, Socrata API
- Could add vehicle fleet analysis angle but may dilute transport focus

### Decision: Focus on NS + OVapi + NDW
Three sources is enough to demonstrate multi-source integration without overcomplicating. Each represents a different transport mode (rail, bus/tram/metro, road) enabling multimodal analysis.

---

## 4. Architecture

```
Data Sources (NS API, OVapi GTFS, NDW)
        |  Python ingestion scripts
        v
Google Cloud Storage (Data Lake — raw/)
        |  Airflow orchestration
        v
BigQuery (raw dataset)
        |  dbt transformations
        v
BigQuery (staging → intermediate → core)
        |
        v
Looker Studio (Dashboard)
```

### Airflow DAGs
| DAG | Schedule | Tasks |
|-----|----------|-------|
| dag_ns_ingest | Daily 06:00 UTC | extract_disruptions >> upload_to_gcs >> load_to_bq |
| dag_ovapi_ingest | Quarterly / manual trigger | download_gtfs >> extract_csvs >> upload_to_gcs >> load_to_bq |
| dag_ndw_ingest | Daily 07:00 UTC | extract_traffic >> upload_to_gcs >> load_to_bq |
| dag_dbt_transform | Daily 08:00 UTC | dbt_seed >> dbt_run >> dbt_test >> dbt_source_freshness |

### Key Design Decisions to Discuss in Interviews
- **Batch over streaming**: Daily aggregates sufficient for analytical use case; simpler ops, lower cost. Streaming (GTFS-RT) is a documented stretch goal.
- **GCS before BigQuery**: Raw data preservation, reprocessing capability, decoupling ingestion from transformation.
- **dbt over custom SQL scripts**: Dependency management via ref(), built-in testing, documentation, version control friendly.
- **Idempotent ingestion**: Re-running a DAG for the same date overwrites (not appends) to prevent duplicates.

---

## 5. dbt Model Architecture

### Xiaolong's dbt Experience (from Module 4)
Already completed and passing:
- Sources configuration (schema.yml with database + schema)
- Staging models (stg_green_tripdata, stg_yellow_tripdata)
- Intermediate layer (int_trips_unioned)
- Core models (fact_trips, dim_zones)
- Seeds (taxi_zone_lookup.csv, payment_type_lookup)
- Macros (safe_cast, get_payment_type_description, get_trip_duration_minutes)
- Tests (not_null, accepted_values) — all passing
- dbt build full pipeline: 15/15 PASS

### GCP/dbt Details
- GCP project: `de-zoomcamp-2026-486821`
- BigQuery datasets: `ny_taxi` (source, EU), `dbt_xsong` (dbt dev output, EU)
- dbt Cloud project: `de-zoomcamp-xiaolong`
- Location: **EU** (critical — must match across all datasets)
- GitHub repo connected to dbt Cloud

### Gotchas Encountered
- Dataset location mismatch (EU vs US) causes "not found" errors — all datasets must be in same region
- `dbt source freshness` must run AFTER `dbt build` in jobs, not before (seed dependency)
- `.gitignore` blocking `*.csv` prevents seed files from reaching GitHub/production
- `example/` folder causes test failures — delete it

### Proposed dbt Models for Final Project
```
models/
  staging/
    stg_ns_disruptions.sql        -- clean NS disruption data
    stg_ns_departures.sql         -- clean NS departure/delay data
    stg_gtfs_stops.sql            -- clean transit stop locations
    stg_gtfs_routes.sql           -- clean route definitions
    stg_gtfs_stop_times.sql       -- clean scheduled stop times
    stg_ndw_traffic_flow.sql      -- clean road traffic measurements
  intermediate/
    int_train_delays_daily.sql    -- aggregate train delays per station per day
    int_transit_schedule_joined.sql -- join stops + routes + stop_times
  core/
    dim_stations.sql              -- NS station dimension with geocoordinates
    dim_routes.sql                -- transit route dimension
    dim_ndw_locations.sql         -- NDW measurement point dimension
    dim_date.sql                  -- date dimension (day, week, month, season)
    fct_train_performance.sql     -- train punctuality fact table
    fct_transit_service.sql       -- bus/tram/metro service fact table
    fct_road_traffic.sql          -- road traffic intensity fact table
    dm_multimodal_daily.sql       -- data mart joining all modes by region + date
```

### dbt Features to Demonstrate
- **Materializations**: staging=view, core=table, fct_train_performance=incremental
- **Incremental model**: fct_train_performance only processes new dates (is_incremental() + unique_key)
- **Sources + freshness**: loaded_at_field with freshness thresholds
- **Tests**: not_null, unique, accepted_values + custom delay range checks
- **Macros**: delay_category() for classifying delay severity
- **Variables**: is_test_run for dev (1 month) vs prod (full history)
- **Documentation**: dbt docs generate with column descriptions

---

## 6. Business Questions the Dashboard Should Answer

1. How reliable are Dutch trains compared to buses and trams across different regions?
2. Which stations and routes experience the most frequent and severe delays?
3. How does road traffic congestion correlate with public transit disruptions?
4. What are peak congestion patterns by time of day, day of week, and season?

### Dashboard Tiles (Looker Studio)
| Tile | Source Model | Visualization |
|------|-------------|---------------|
| Overall Punctuality Score | fct_train_performance | Scorecard with trend |
| Delay Heatmap by Station | fct_train_performance + dim_stations | Map |
| Daily Delay Trend | fct_train_performance | Time series |
| Mode Comparison | dm_multimodal_daily | Bar chart |
| Road vs Transit Correlation | dm_multimodal_daily | Dual-axis chart |
| Top 10 Worst Routes | fct_train_performance | Horizontal bar |

---

## 7. Repository Structure

```
dutch-transport-analytics/
├── README.md
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── airflow/
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── dags/
│   │   ├── dag_ns_ingest.py
│   │   ├── dag_ovapi_ingest.py
│   │   ├── dag_ndw_ingest.py
│   │   └── dag_dbt_transform.py
│   └── scripts/
│       ├── ingest_ns.py
│       ├── ingest_ovapi.py
│       └── ingest_ndw.py
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── core/
│   ├── macros/
│   ├── seeds/
│   └── tests/
└── images/
    ├── architecture.png
    └── dashboard.png
```

---

## 8. Timeline (4 Weeks)

| Week | Phase | Key Tasks | Output |
|------|-------|-----------|--------|
| Week 1 | Infrastructure | Terraform setup, Docker Compose for Airflow, GCS/BQ provisioning | Working local Airflow + cloud resources |
| Week 2 | Ingestion | NS API script, OVapi GTFS loader, NDW script, Airflow DAGs | Data flowing into GCS and BigQuery daily |
| Week 3 | Transformation | dbt project: staging + core models, tests, incremental model, docs | Analytical models in BigQuery |
| Week 4 | Dashboard + Polish | Looker Studio dashboard, README, cleanup, project submission | Complete end-to-end pipeline |

---

## 9. Zoomcamp Evaluation Criteria

| Criteria | Coverage | Points |
|----------|----------|--------|
| Problem description | README with business questions, data sources, architecture | 0-4 |
| Cloud | Terraform for GCS + BigQuery + IAM | 0-4 |
| Data ingestion (batch/stream) | Airflow DAGs for 3 APIs → GCS → BQ | 0-4 |
| Data warehouse | BigQuery with partitioning + clustering | 0-4 |
| Transformations (dbt) | Full dbt project with staging/intermediate/core | 0-4 |
| Dashboard | Looker Studio with 3+ tiles and filters | 0-4 |
| Reproducibility | Docker + Terraform + README + seed data | 0-4 |

---

## 10. Open Questions / Decisions Needed

- [ ] NS API: confirm free tier rate limits and data retention
- [ ] OVapi: identify exact GTFS download URL and format
- [ ] NDW: identify best API endpoint for daily traffic aggregates
- [ ] Decide: dbt Cloud (current setup) vs dbt Core (inside Airflow Docker) for final project
- [ ] Decide: scope of GTFS-RT (real-time) — include or leave as stretch goal
- [ ] BigQuery dataset naming: raw_nl_transport / staging_nl_transport / core_nl_transport
- [ ] Airflow executor: LocalExecutor (simpler) vs CeleryExecutor (more production-like)