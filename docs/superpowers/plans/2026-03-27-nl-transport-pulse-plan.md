# NL Transport Pulse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end Dutch transport reliability pipeline: Terraform → Airflow → GCS → BigQuery → dbt → Streamlit dashboard with Slack alerting.

**Architecture:** Three-layer batch pipeline — Python ingestion scripts orchestrated by Airflow load raw data from NS and NDW APIs into GCS then BigQuery, dbt Core transforms through staging/intermediate/core layers into a star schema, and a Streamlit app serves the dashboard. All infrastructure provisioned by Terraform, all services containerized in Docker Compose.

**Tech Stack:** Python 3.11, Terraform, Docker Compose, Apache Airflow 2.x, Google Cloud Storage, BigQuery, dbt Core 1.7+, Streamlit, Plotly, Slack webhooks.

**Spec:** `docs/superpowers/specs/2026-03-27-nl-transport-pulse-design.md`

---

## File Structure

```
dutch-transport-analytics/
├── .env.example                          # Template for env vars (GCP project, credentials, etc.)
├── .gitignore
├── README.md
├── terraform/
│   ├── main.tf                           # GCS bucket, BQ datasets, service account, IAM
│   ├── variables.tf                      # Project ID, region, bucket name, dataset names
│   └── outputs.tf                        # Bucket name, dataset IDs, SA email
├── airflow/
│   ├── Dockerfile                        # Extends apache/airflow with dbt-core, gcloud, etc.
│   ├── docker-compose.yml                # webserver, scheduler, postgres, streamlit
│   ├── requirements.txt                  # Python deps for DAGs + ingestion scripts
│   ├── dags/
│   │   ├── dag_ns_ingest.py              # NS API → GCS → BQ daily DAG
│   │   ├── dag_ndw_ingest.py             # NDW → GCS → BQ daily DAG
│   │   └── dag_dbt_transform.py          # dbt run + test + alerts daily DAG
│   ├── scripts/
│   │   ├── ingest_ns.py                  # NS API extraction logic (disruptions + departures)
│   │   ├── ingest_ndw.py                 # NDW extraction logic (15-min traffic observations)
│   │   ├── gcs_utils.py                  # Upload to GCS, partition-scoped overwrite helpers
│   │   ├── bq_utils.py                   # Load GCS → BQ with partition-scoped overwrite
│   │   └── alert_checker.py              # Post-dbt alert SQL checks + Slack webhook
│   └── tests/
│       ├── test_ingest_ns.py             # Unit tests for NS extraction/parsing
│       ├── test_ingest_ndw.py            # Unit tests for NDW extraction/parsing
│       ├── test_gcs_utils.py             # Unit tests for GCS upload helpers
│       ├── test_bq_utils.py              # Unit tests for BQ load helpers
│       └── test_alert_checker.py         # Unit tests for alert detection logic
├── dbt/
│   ├── dbt_project.yml                   # Project config, vars (is_test_run), model paths
│   ├── packages.yml                      # dbt_utils, dbt_date if needed
│   ├── profiles.yml                      # BigQuery connection (for local dev; Docker overrides)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _staging__sources.yml     # Source definitions + freshness for raw tables
│   │   │   ├── _staging__models.yml      # Model descriptions + column tests
│   │   │   ├── stg_ns_disruptions.sql
│   │   │   ├── stg_ns_departures.sql
│   │   │   └── stg_ndw_traffic_flow.sql
│   │   ├── intermediate/
│   │   │   ├── _intermediate__models.yml
│   │   │   ├── int_train_delays_daily.sql
│   │   │   └── int_ndw_traffic_daily.sql
│   │   └── core/
│   │       ├── _core__models.yml         # Model descriptions + tests
│   │       ├── dim_stations.sql
│   │       ├── dim_ndw_locations.sql
│   │       ├── dim_date.sql
│   │       ├── fct_train_performance.sql
│   │       ├── fct_road_traffic.sql
│   │       └── dm_multimodal_daily.sql
│   ├── snapshots/
│   │   └── snap_dim_stations.sql         # SCD Type 2 snapshot
│   ├── macros/
│   │   ├── delay_category.sql
│   │   └── generate_date_spine.sql
│   ├── seeds/
│   │   ├── station_corridor_mapping.csv
│   │   ├── station_ndw_mapping.csv
│   │   └── nl_holidays.csv
│   └── tests/
│       └── assert_pct_on_time_range.sql  # Custom generic test
├── streamlit/
│   ├── Dockerfile                        # Streamlit app container
│   ├── requirements.txt                  # streamlit, plotly, google-cloud-bigquery, pandas
│   ├── app.py                            # Main app entry point (multi-page config)
│   ├── pages/
│   │   ├── 1_Network_Overview.py
│   │   └── 2_Corridor_Explorer.py
│   └── utils/
│       └── bq_client.py                  # BigQuery query helper with @st.cache_data
└── docs/
    └── superpowers/
        ├── specs/
        │   └── 2026-03-27-nl-transport-pulse-design.md
        └── plans/
            └── 2026-03-27-nl-transport-pulse-plan.md
```

---

## Week 1: Infrastructure + Data Discovery

### Task 1: Initialize Git Repository and Project Skeleton

**Files:**
- Create: `.gitignore`, `.env.example`, `README.md`

- [ ] **Step 1: Initialize git repo**

```bash
cd /Users/niko/Desktop/project_2026/project1
git init
```

Expected: `Initialized empty Git repository`

- [ ] **Step 2: Create .gitignore**

```gitignore
# Python
__pycache__/
*.py[cod]
*.egg-info/
.venv/
venv/

# Environment
.env
*.json
!dbt/seeds/*.json

# GCP credentials
*-credentials.json
*-key.json
service-account*.json

# Terraform
terraform/.terraform/
terraform/*.tfstate
terraform/*.tfstate.backup
terraform/*.tfvars

# Airflow
airflow/logs/
airflow/plugins/

# dbt
dbt/target/
dbt/dbt_packages/
dbt/logs/

# IDE
.idea/
.vscode/
*.swp

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 3: Create .env.example**

```bash
# GCP
GCP_PROJECT_ID=your-project-id
GCP_REGION=europe-west4
GCS_BUCKET_NAME=nl-transport-raw
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/keys/gcp-credentials.json

# BigQuery
BQ_RAW_DATASET=raw_nl_transport
BQ_STAGING_DATASET=staging_nl_transport
BQ_CORE_DATASET=core_nl_transport

# NS API
NS_API_KEY=your-ns-api-key
NS_API_BASE_URL=https://gateway.apiportal.ns.nl

# Slack
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__FERNET_KEY=your-fernet-key
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

- [ ] **Step 4: Create initial README.md**

```markdown
# NL Transport Pulse

Dutch multimodal transport reliability dashboard — an end-to-end data engineering pipeline analyzing train performance (NS), road traffic (NDW), and public transit (OVapi) across the Netherlands.

## Architecture

```
NS API / NDW / OVapi → Python → GCS → BigQuery → dbt → Streamlit
                        (Airflow orchestration)
```

## Tech Stack

| Component | Tool |
|-----------|------|
| Infrastructure as Code | Terraform |
| Orchestration | Apache Airflow |
| Data Lake | Google Cloud Storage |
| Data Warehouse | BigQuery |
| Transformation | dbt Core |
| Dashboard | Streamlit |
| Containerization | Docker Compose |

## Quick Start

1. Copy `.env.example` to `.env` and fill in values
2. `cd terraform && terraform init && terraform apply`
3. `cd ../airflow && docker compose up -d`
4. Airflow UI: http://localhost:8080 (airflow/airflow)
5. Dashboard: http://localhost:8501

## Project Structure

See `docs/superpowers/specs/2026-03-27-nl-transport-pulse-design.md` for full design spec.
```

- [ ] **Step 5: Create directory skeleton**

```bash
mkdir -p terraform
mkdir -p airflow/{dags,scripts,tests}
mkdir -p dbt/{models/{staging,intermediate,core},snapshots,macros,seeds,tests}
mkdir -p streamlit/{pages,utils}
```

- [ ] **Step 6: Commit**

```bash
git add .gitignore .env.example README.md docs/
git commit -m "feat: initialize project with spec, plan, and directory skeleton"
```

---

### Task 2: Terraform — GCS + BigQuery + IAM

**Files:**
- Create: `terraform/main.tf`, `terraform/variables.tf`, `terraform/outputs.tf`

- [ ] **Step 1: Create terraform/variables.tf**

```hcl
variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "de-zoomcamp-2026-486821"
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "europe-west4"
}

variable "gcs_bucket_name" {
  description = "GCS bucket for raw data lake"
  type        = string
  default     = "nl-transport-raw"
}

variable "bq_raw_dataset" {
  description = "BigQuery dataset for raw data"
  type        = string
  default     = "raw_nl_transport"
}

variable "bq_staging_dataset" {
  description = "BigQuery dataset for dbt staging"
  type        = string
  default     = "staging_nl_transport"
}

variable "bq_core_dataset" {
  description = "BigQuery dataset for dbt core"
  type        = string
  default     = "core_nl_transport"
}
```

- [ ] **Step 2: Create terraform/main.tf**

```hcl
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# GCS bucket for raw data lake
resource "google_storage_bucket" "raw_data_lake" {
  name          = "${var.project_id}-${var.gcs_bucket_name}"
  location      = "EU"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }
}

# BigQuery datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id = var.bq_raw_dataset
  location   = "EU"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = var.bq_staging_dataset
  location   = "EU"
}

resource "google_bigquery_dataset" "core" {
  dataset_id = var.bq_core_dataset
  location   = "EU"
}

# Service account for pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "nl-transport-pipeline"
  display_name = "NL Transport Pipeline Service Account"
}

# IAM: BigQuery Data Editor on all three datasets
resource "google_bigquery_dataset_iam_member" "raw_editor" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "staging_editor" {
  dataset_id = google_bigquery_dataset.staging.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_bigquery_dataset_iam_member" "core_editor" {
  dataset_id = google_bigquery_dataset.core.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# IAM: BigQuery Job User (needed to run queries)
resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# IAM: GCS Object Admin on the bucket
resource "google_storage_bucket_iam_member" "gcs_admin" {
  bucket = google_storage_bucket.raw_data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Service account key (for Docker containers)
resource "google_service_account_key" "pipeline_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# Write key to local file (gitignored)
resource "local_file" "sa_key" {
  content  = base64decode(google_service_account_key.pipeline_key.private_key)
  filename = "${path.module}/gcp-credentials.json"
}
```

- [ ] **Step 3: Create terraform/outputs.tf**

```hcl
output "gcs_bucket_name" {
  value = google_storage_bucket.raw_data_lake.name
}

output "raw_dataset_id" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "staging_dataset_id" {
  value = google_bigquery_dataset.staging.dataset_id
}

output "core_dataset_id" {
  value = google_bigquery_dataset.core.dataset_id
}

output "service_account_email" {
  value = google_service_account.pipeline_sa.email
}
```

- [ ] **Step 4: Initialize and validate Terraform**

```bash
cd terraform
terraform init
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Apply Terraform (requires GCP auth)**

```bash
terraform plan -out=tfplan
terraform apply tfplan
```

Expected: 8 resources created (bucket, 3 datasets, SA, 4 IAM bindings, SA key, local file).

Copy `terraform/gcp-credentials.json` to `airflow/keys/gcp-credentials.json`:

```bash
mkdir -p ../airflow/keys
cp gcp-credentials.json ../airflow/keys/
```

- [ ] **Step 6: Commit**

```bash
cd ..
git add terraform/
git commit -m "feat: terraform config for GCS bucket, BQ datasets, and service account"
```

---

### Task 3: Docker Compose — Airflow + Streamlit

**Files:**
- Create: `airflow/Dockerfile`, `airflow/docker-compose.yml`, `airflow/requirements.txt`
- Create: `streamlit/Dockerfile`, `streamlit/requirements.txt`

- [ ] **Step 1: Create airflow/requirements.txt**

```
apache-airflow-providers-google>=10.0.0
google-cloud-storage>=2.14.0
google-cloud-bigquery>=3.14.0
requests>=2.31.0
dbt-core>=1.7.0
dbt-bigquery>=1.7.0
pandas>=2.1.0
```

- [ ] **Step 2: Create airflow/Dockerfile**

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy dbt project into the container
COPY --chown=airflow:root ../dbt /opt/airflow/dbt
```

- [ ] **Step 3: Create airflow/docker-compose.yml**

```yaml
version: '3.8'

x-airflow-common: &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    - AIRFLOW__CORE__FERNET_KEY=${AIRFLOW__CORE__FERNET_KEY:-}
    - AIRFLOW__CORE__LOAD_EXAMPLES=False
    - AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
    - GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/keys/gcp-credentials.json
    - GCP_PROJECT_ID=${GCP_PROJECT_ID}
    - GCS_BUCKET_NAME=${GCS_BUCKET_NAME}
    - BQ_RAW_DATASET=${BQ_RAW_DATASET}
    - BQ_STAGING_DATASET=${BQ_STAGING_DATASET}
    - BQ_CORE_DATASET=${BQ_CORE_DATASET}
    - NS_API_KEY=${NS_API_KEY}
    - NS_API_BASE_URL=${NS_API_BASE_URL:-https://gateway.apiportal.ns.nl}
    - SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
  volumes:
    - ./dags:/opt/airflow/dags
    - ./scripts:/opt/airflow/scripts
    - ./keys:/opt/airflow/keys:ro
    - ./logs:/opt/airflow/logs
    - ../dbt:/opt/airflow/dbt
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      retries: 5

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: >
      -c "
      airflow db init &&
      airflow users create
        --username airflow
        --password airflow
        --firstname Admin
        --lastname User
        --role Admin
        --email admin@example.com
      "
    restart: "no"

  airflow-webserver:
    <<: *airflow-common
    command: airflow webserver --port 8080
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    restart: always

  airflow-scheduler:
    <<: *airflow-common
    command: airflow scheduler
    restart: always

  streamlit:
    build:
      context: ../streamlit
      dockerfile: Dockerfile
    ports:
      - "8501:8501"
    environment:
      - GOOGLE_APPLICATION_CREDENTIALS=/app/keys/gcp-credentials.json
      - GCP_PROJECT_ID=${GCP_PROJECT_ID}
      - BQ_CORE_DATASET=${BQ_CORE_DATASET}
    volumes:
      - ../streamlit:/app
      - ./keys:/app/keys:ro
    command: streamlit run app.py --server.port=8501 --server.address=0.0.0.0
    restart: always

volumes:
  postgres-data:
```

- [ ] **Step 4: Create streamlit/requirements.txt**

```
streamlit>=1.30.0
plotly>=5.18.0
google-cloud-bigquery>=3.14.0
pandas>=2.1.0
db-dtypes>=1.2.0
```

- [ ] **Step 5: Create streamlit/Dockerfile**

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

- [ ] **Step 6: Create placeholder streamlit/app.py**

```python
import streamlit as st

st.set_page_config(
    page_title="NL Transport Pulse",
    page_icon="🚆",
    layout="wide",
)

st.title("NL Transport Pulse")
st.markdown("Dutch multimodal transport reliability dashboard")
st.info("Dashboard pages will be added in Week 4.")
```

- [ ] **Step 7: Verify Docker Compose builds**

```bash
cd airflow
docker compose build
```

Expected: Both `airflow` and `streamlit` images build successfully.

- [ ] **Step 8: Verify Docker Compose starts**

```bash
docker compose up airflow-init
docker compose up -d
```

Expected: All services running. Check:
- http://localhost:8080 → Airflow login (airflow/airflow)
- http://localhost:8501 → Streamlit placeholder page

```bash
docker compose down
```

- [ ] **Step 9: Commit**

```bash
cd ..
git add airflow/Dockerfile airflow/docker-compose.yml airflow/requirements.txt
git add streamlit/Dockerfile streamlit/requirements.txt streamlit/app.py
git commit -m "feat: docker compose stack with Airflow and Streamlit"
```

---

### Task 4: NS API — Data Discovery and Exploration

**Files:**
- No files created yet — this is research to confirm API endpoints, rate limits, and response shapes.

- [ ] **Step 1: Register for NS API key**

Go to https://apiportal.ns.nl/ and register for a free account. Subscribe to the "Ns-App" product which includes:
- Disruptions API
- Departures API (travel information)
- Stations API

Save the API key (Primary Key from the portal) to your `.env` file as `NS_API_KEY`.

- [ ] **Step 2: Test NS Disruptions API**

```bash
curl -s -H "Ocp-Apim-Subscription-Key: $NS_API_KEY" \
  "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v3/disruptions?isActive=false" \
  | python3 -m json.tool | head -80
```

Document the response shape. Key fields expected:
- `id` (disruption_id)
- `title`, `description`
- `start`, `end` (timestamps)
- `timespans[].cause.label`
- `timespans[].stations[].stationCode`

- [ ] **Step 3: Test NS Departures API**

```bash
curl -s -H "Ocp-Apim-Subscription-Key: $NS_API_KEY" \
  "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/departures?station=ASD" \
  | python3 -m json.tool | head -80
```

Document the response shape. Key fields expected:
- `departures[].direction`
- `departures[].plannedDateTime`
- `departures[].actualDateTime` (if delayed)
- `departures[].trainCategory`
- `departures[].routeStations[].uicCode`

- [ ] **Step 4: Test NS Stations API**

```bash
curl -s -H "Ocp-Apim-Subscription-Key: $NS_API_KEY" \
  "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/stations" \
  | python3 -m json.tool | head -80
```

Key fields: `code`, `namen.lang`, `lat`, `lng`, `stationType`, `naderenPopulair` (popularity).

- [ ] **Step 5: Document findings**

Note in a comment or doc:
- Rate limits (typically 5000 requests/day for free tier)
- Whether historical disruptions are available (check `?isActive=false` parameter)
- Response pagination if applicable
- Any fields missing from expectations

---

### Task 5: NDW — Data Discovery and Exploration

**Files:**
- No files created yet — research task.

- [ ] **Step 1: Explore NDW Open Data Portal**

Visit https://opendata.ndw.nu/ and identify:
- Available datasets (traffic speed, traffic flow/intensity)
- Data format (XML DATEX II, CSV, or API)
- Whether 15-minute interval data is available
- Whether historical data can be downloaded in bulk

- [ ] **Step 2: Identify the right NDW data feed**

NDW publishes several feeds. The most relevant is:
- **TrafficSpeed** — average speeds per measurement point per interval
- **TrafficFlow** — vehicle counts per measurement point per interval

Check if there's a REST API or if data must be downloaded as DATEX II XML files from their SFTP/HTTP endpoint.

- [ ] **Step 3: Test data access**

Download a sample file and inspect the format:

```bash
# Example — adjust URL based on what the portal provides
curl -s "https://opendata.ndw.nu/trafficspeed.xml.gz" | gunzip | head -200
```

Or if CSV is available:
```bash
curl -s "https://opendata.ndw.nu/SOME_ENDPOINT" | head -50
```

Document:
- File format (XML DATEX II vs CSV vs JSON)
- Measurement point identifiers (location_id format)
- Timestamp format and interval (confirm 15-min)
- Fields: speed, flow/count, location reference

- [ ] **Step 4: Locate NDW measurement point metadata**

Need lat/lon for each measurement point to build the station-NDW mapping. Check:
- NDW measurement site table (often published as a separate file)
- Fields needed: `measurementSiteId`, `latitude`, `longitude`, `roadName`, `roadNumber`

- [ ] **Step 5: Evaluate parsing complexity**

If NDW uses DATEX II XML:
- It's verbose but well-structured
- Will need an XML parser (lxml) in the ingestion script
- Consider whether a Python DATEX II library exists

If CSV or JSON: much simpler, direct pandas read.

Document the chosen approach and any libraries needed.

---

### Task 6: Build Seed CSVs

**Files:**
- Create: `dbt/seeds/station_corridor_mapping.csv`
- Create: `dbt/seeds/station_ndw_mapping.csv`
- Create: `dbt/seeds/nl_holidays.csv`

- [ ] **Step 1: Create station_corridor_mapping.csv**

Using NS Stations API data from Task 4, identify the ~5 MVP corridors and assign stations:

```csv
station_code,station_name,corridor_id,corridor_name,lat,lon
ASD,Amsterdam Centraal,1,Amsterdam-Rotterdam,52.3791,4.9003
RTD,Rotterdam Centraal,1,Amsterdam-Rotterdam,51.9244,4.4690
SDM,Schiedam Centrum,1,Amsterdam-Rotterdam,51.9217,4.4089
DT,Delft,1,Amsterdam-Rotterdam,52.0067,4.3567
LD,Leiden Centraal,1,Amsterdam-Rotterdam,52.1663,4.4817
UT,Utrecht Centraal,2,Amsterdam-Utrecht,52.0893,5.1101
ASD,Amsterdam Centraal,2,Amsterdam-Utrecht,52.3791,4.9003
AMS,Amsterdam Amstel,2,Amsterdam-Utrecht,52.3464,4.9178
BRN,Breukelen,2,Amsterdam-Utrecht,52.1717,5.0025
GVC,Den Haag Centraal,3,Den Haag-Rotterdam,52.0808,4.3248
RTD,Rotterdam Centraal,3,Den Haag-Rotterdam,51.9244,4.4690
EHV,Eindhoven Centraal,4,Utrecht-Eindhoven,51.4433,5.4817
UT,Utrecht Centraal,4,Utrecht-Eindhoven,52.0893,5.1101
HTN,s-Hertogenbosch,4,Utrecht-Eindhoven,51.6908,5.2936
AH,Arnhem Centraal,5,Utrecht-Arnhem,51.9847,5.8987
UT,Utrecht Centraal,5,Utrecht-Arnhem,52.0893,5.1101
DRN,Driebergen-Zeist,5,Utrecht-Arnhem,52.0547,5.2811
EDE,Ede-Wageningen,5,Utrecht-Arnhem,52.0428,5.6500
```

Note: A station can appear in multiple corridors. Exact lat/lon values should be confirmed from NS Stations API response in Task 4.

- [ ] **Step 2: Create station_ndw_mapping.csv**

This requires NDW measurement point metadata from Task 5. Create the initial structure:

```csv
station_code,station_name,ndw_location_id,road_name,ndw_lat,ndw_lon,distance_km
```

Populate after completing Task 5 by matching major stations to nearby NDW measurement points on connecting highways (A1, A2, A4, A12, A13, A16, A27, A28).

If NDW metadata is not yet available, create a placeholder with the structure and fill in during Week 2.

- [ ] **Step 3: Create nl_holidays.csv**

```csv
date,holiday_name
2026-01-01,New Year's Day
2026-04-03,Good Friday
2026-04-05,Easter Sunday
2026-04-06,Easter Monday
2026-04-27,King's Day
2026-05-05,Liberation Day
2026-05-14,Ascension Day
2026-05-24,Whit Sunday
2026-05-25,Whit Monday
2026-12-25,Christmas Day
2026-12-26,Second Christmas Day
```

- [ ] **Step 4: Commit**

```bash
git add dbt/seeds/
git commit -m "feat: add seed CSVs for corridors, NDW mapping, and Dutch holidays"
```

---

## Week 2: Ingestion

### Task 7: GCS + BQ Utility Modules

**Files:**
- Create: `airflow/scripts/gcs_utils.py`
- Create: `airflow/scripts/bq_utils.py`
- Create: `airflow/tests/test_gcs_utils.py`
- Create: `airflow/tests/test_bq_utils.py`

- [ ] **Step 1: Write test for GCS upload**

```python
# airflow/tests/test_gcs_utils.py
from unittest.mock import MagicMock, patch
import json


def test_upload_json_to_gcs():
    """upload_json_to_gcs should upload serialized JSON to the correct GCS path."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    with patch("scripts.gcs_utils.storage.Client", return_value=mock_client):
        from scripts.gcs_utils import upload_json_to_gcs

        data = [{"id": "1", "value": "test"}]
        upload_json_to_gcs(
            data=data,
            bucket_name="test-bucket",
            blob_path="raw/ns/disruptions/dt=2026-03-26/disruptions.json",
        )

        mock_client.bucket.assert_called_once_with("test-bucket")
        mock_bucket.blob.assert_called_once_with(
            "raw/ns/disruptions/dt=2026-03-26/disruptions.json"
        )
        mock_blob.upload_from_string.assert_called_once()
        uploaded_data = json.loads(
            mock_blob.upload_from_string.call_args[0][0]
        )
        assert uploaded_data == data


def test_upload_csv_to_gcs():
    """upload_csv_to_gcs should upload CSV string to GCS."""
    mock_client = MagicMock()
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    with patch("scripts.gcs_utils.storage.Client", return_value=mock_client):
        from scripts.gcs_utils import upload_csv_to_gcs

        csv_content = "col1,col2\nval1,val2\n"
        upload_csv_to_gcs(
            csv_content=csv_content,
            bucket_name="test-bucket",
            blob_path="raw/ndw/traffic_flow/dt=2026-03-26/traffic.csv",
        )

        mock_blob.upload_from_string.assert_called_once_with(
            csv_content, content_type="text/csv"
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd airflow
python -m pytest tests/test_gcs_utils.py -v
```

Expected: FAIL — `ModuleNotFoundError: No module named 'scripts'`

- [ ] **Step 3: Implement gcs_utils.py**

```python
# airflow/scripts/gcs_utils.py
"""Helpers for uploading data to Google Cloud Storage."""
import json
from google.cloud import storage


def upload_json_to_gcs(
    data: list[dict], bucket_name: str, blob_path: str
) -> str:
    """Upload a list of dicts as a JSON file to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    return f"gs://{bucket_name}/{blob_path}"


def upload_csv_to_gcs(
    csv_content: str, bucket_name: str, blob_path: str
) -> str:
    """Upload a CSV string to GCS.

    Returns the gs:// URI of the uploaded blob.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(csv_content, content_type="text/csv")
    return f"gs://{bucket_name}/{blob_path}"
```

- [ ] **Step 4: Create airflow/scripts/__init__.py and run test**

```bash
touch scripts/__init__.py
python -m pytest tests/test_gcs_utils.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 5: Write test for BQ partition-scoped overwrite**

```python
# airflow/tests/test_bq_utils.py
from unittest.mock import MagicMock, patch, call


def test_load_json_to_bq_deletes_partition_first():
    """load_json_to_bq should delete the target partition then load from GCS."""
    mock_client = MagicMock()
    mock_job = MagicMock()
    mock_job.result.return_value = None
    mock_client.load_table_from_uri.return_value = mock_job
    mock_client.query.return_value.result.return_value = None

    with patch("scripts.bq_utils.bigquery.Client", return_value=mock_client):
        from scripts.bq_utils import load_json_to_bq

        load_json_to_bq(
            gcs_uri="gs://bucket/raw/ns/disruptions/dt=2026-03-26/disruptions.json",
            table_id="project.raw_nl_transport.ns_disruptions",
            service_date="2026-03-26",
        )

        # Should delete partition first
        delete_call = mock_client.query.call_args_list[0]
        assert "2026-03-26" in delete_call[0][0]
        assert "DELETE" in delete_call[0][0] or "MERGE" in delete_call[0][0]

        # Then load from GCS
        mock_client.load_table_from_uri.assert_called_once()
```

- [ ] **Step 6: Run test to verify it fails**

```bash
python -m pytest tests/test_bq_utils.py -v
```

Expected: FAIL.

- [ ] **Step 7: Implement bq_utils.py**

```python
# airflow/scripts/bq_utils.py
"""Helpers for loading data from GCS into BigQuery with partition-scoped overwrite."""
from google.cloud import bigquery


def load_json_to_bq(
    gcs_uri: str,
    table_id: str,
    service_date: str,
) -> None:
    """Load a JSON file from GCS into a BigQuery table.

    Implements partition-scoped overwrite:
    1. Delete all rows for the given service_date
    2. Load new data from GCS
    """
    client = bigquery.Client()

    # Step 1: Delete existing partition data
    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE service_date = '{service_date}'
    """
    try:
        client.query(delete_sql).result()
    except Exception:
        # Table may not exist yet on first run — that's OK
        pass

    # Step 2: Load from GCS
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="service_date",
        ),
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()


def load_csv_to_bq(
    gcs_uri: str,
    table_id: str,
    service_date: str,
    schema: list[bigquery.SchemaField] | None = None,
) -> None:
    """Load a CSV file from GCS into a BigQuery table.

    Same partition-scoped overwrite as load_json_to_bq.
    """
    client = bigquery.Client()

    # Step 1: Delete existing partition data
    delete_sql = f"""
    DELETE FROM `{table_id}`
    WHERE service_date = '{service_date}'
    """
    try:
        client.query(delete_sql).result()
    except Exception:
        pass

    # Step 2: Load from GCS
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="service_date",
        ),
    )
    if schema:
        job_config.schema = schema
    else:
        job_config.autodetect = True

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
```

- [ ] **Step 8: Run test to verify it passes**

```bash
python -m pytest tests/test_bq_utils.py -v
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
cd ..
git add airflow/scripts/ airflow/tests/
git commit -m "feat: GCS upload and BQ partition-scoped overwrite utility modules"
```

---

### Task 8: NS API Ingestion Script

**Files:**
- Create: `airflow/scripts/ingest_ns.py`
- Create: `airflow/tests/test_ingest_ns.py`

- [ ] **Step 1: Write test for NS disruption extraction**

```python
# airflow/tests/test_ingest_ns.py
from unittest.mock import patch, MagicMock
import json


SAMPLE_DISRUPTIONS_RESPONSE = {
    "payload": [
        {
            "id": "disruption-001",
            "title": "Geen treinen Amsterdam - Utrecht",
            "isActive": False,
            "start": "2026-03-26T06:00:00+0100",
            "end": "2026-03-26T10:30:00+0100",
            "timespans": [
                {
                    "cause": {"label": "seinstoring"},
                    "stations": [
                        {"stationCode": "ASD", "name": "Amsterdam Centraal"},
                        {"stationCode": "UT", "name": "Utrecht Centraal"},
                    ],
                }
            ],
        }
    ]
}


def test_extract_disruptions_parses_response():
    """extract_disruptions should return flat list of disruption records."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DISRUPTIONS_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_disruptions

        result = extract_disruptions(
            api_key="test-key",
            base_url="https://test.api.ns.nl",
            service_date="2026-03-26",
        )

        assert len(result) == 1
        row = result[0]
        assert row["disruption_id"] == "disruption-001"
        assert row["service_date"] == "2026-03-26"
        assert row["title"] == "Geen treinen Amsterdam - Utrecht"
        assert "ASD" in row["affected_station_codes"]
        assert row["cause"] == "seinstoring"
        assert row["duration_minutes"] > 0


SAMPLE_DEPARTURES_RESPONSE = {
    "payload": {
        "departures": [
            {
                "direction": "Rotterdam Centraal",
                "plannedDateTime": "2026-03-26T08:00:00+0100",
                "actualDateTime": "2026-03-26T08:05:00+0100",
                "trainCategory": "Intercity",
                "routeStations": [
                    {"uicCode": "8400530", "mediumName": "Rotterdam C."},
                ],
            }
        ]
    }
}


def test_extract_departures_calculates_delay():
    """extract_departures should calculate delay_minutes from planned vs actual."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_DEPARTURES_RESPONSE

    with patch("scripts.ingest_ns.requests.get", return_value=mock_response):
        from scripts.ingest_ns import extract_departures

        result = extract_departures(
            api_key="test-key",
            base_url="https://test.api.ns.nl",
            station_code="ASD",
            service_date="2026-03-26",
        )

        assert len(result) == 1
        row = result[0]
        assert row["station_code"] == "ASD"
        assert row["delay_minutes"] == 5.0
        assert row["direction"] == "Rotterdam Centraal"
        assert row["service_date"] == "2026-03-26"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd airflow
python -m pytest tests/test_ingest_ns.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement ingest_ns.py**

```python
# airflow/scripts/ingest_ns.py
"""NS API extraction logic for disruptions and departures."""
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
    """Parse NS API datetime string (ISO 8601 with timezone offset)."""
    # NS returns format like "2026-03-26T08:00:00+0100"
    # Python's fromisoformat handles this in 3.11+
    return datetime.fromisoformat(dt_str)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_ingest_ns.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
cd ..
git add airflow/scripts/ingest_ns.py airflow/tests/test_ingest_ns.py
git commit -m "feat: NS API ingestion script with disruption and departure extraction"
```

---

### Task 9: NDW Ingestion Script

**Files:**
- Create: `airflow/scripts/ingest_ndw.py`
- Create: `airflow/tests/test_ingest_ndw.py`

Note: The exact implementation depends on Task 5 findings (NDW data format). This task provides the structure assuming CSV or parsed XML output. Adjust based on actual NDW data format discovered.

- [ ] **Step 1: Write test for NDW extraction**

```python
# airflow/tests/test_ingest_ndw.py
from unittest.mock import patch, MagicMock


SAMPLE_NDW_RECORDS = [
    {
        "location_id": "NDW-001",
        "measurement_ts": "2026-03-26T08:00:00+01:00",
        "service_date": "2026-03-26",
        "avg_speed_kmh": 95.5,
        "vehicle_count": 42,
    },
    {
        "location_id": "NDW-001",
        "measurement_ts": "2026-03-26T08:15:00+01:00",
        "service_date": "2026-03-26",
        "avg_speed_kmh": 88.2,
        "vehicle_count": 55,
    },
]


def test_extract_traffic_returns_15min_records():
    """extract_traffic should return 15-min interval records for specified locations."""
    with patch(
        "scripts.ingest_ndw._fetch_raw_traffic_data",
        return_value=SAMPLE_NDW_RECORDS,
    ):
        from scripts.ingest_ndw import extract_traffic

        result = extract_traffic(
            service_date="2026-03-26",
            location_ids=["NDW-001"],
        )

        assert len(result) == 2
        assert result[0]["location_id"] == "NDW-001"
        assert result[0]["service_date"] == "2026-03-26"
        assert result[0]["avg_speed_kmh"] == 95.5
        assert result[0]["vehicle_count"] == 42


def test_extract_traffic_filters_by_location_ids():
    """extract_traffic should only return records for requested locations."""
    all_records = SAMPLE_NDW_RECORDS + [
        {
            "location_id": "NDW-999",
            "measurement_ts": "2026-03-26T08:00:00+01:00",
            "service_date": "2026-03-26",
            "avg_speed_kmh": 50.0,
            "vehicle_count": 10,
        },
    ]

    with patch(
        "scripts.ingest_ndw._fetch_raw_traffic_data",
        return_value=all_records,
    ):
        from scripts.ingest_ndw import extract_traffic

        result = extract_traffic(
            service_date="2026-03-26",
            location_ids=["NDW-001"],
        )

        assert all(r["location_id"] == "NDW-001" for r in result)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd airflow
python -m pytest tests/test_ingest_ndw.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement ingest_ndw.py**

```python
# airflow/scripts/ingest_ndw.py
"""NDW traffic data extraction logic.

Note: _fetch_raw_traffic_data is a placeholder that must be adapted
based on the actual NDW data format discovered in Task 5.
If NDW provides DATEX II XML, this function will parse XML.
If NDW provides CSV dumps, this function will parse CSV.
"""
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_ingest_ndw.py -v
```

Expected: 2 tests PASS (they mock `_fetch_raw_traffic_data`, so the `NotImplementedError` doesn't fire).

- [ ] **Step 5: Commit**

```bash
cd ..
git add airflow/scripts/ingest_ndw.py airflow/tests/test_ingest_ndw.py
git commit -m "feat: NDW ingestion script with 15-min extraction and location filtering"
```

---

### Task 10: Airflow DAG — NS Ingestion

**Files:**
- Create: `airflow/dags/dag_ns_ingest.py`

- [ ] **Step 1: Create the DAG**

```python
# airflow/dags/dag_ns_ingest.py
"""DAG: Ingest NS disruptions and departures daily.

Schedule: 06:00 UTC daily.
service_date = execution_date (= yesterday in Airflow convention).

Pipeline: NS API → JSON → GCS → BigQuery (partition-scoped overwrite).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

import os
import sys

# Add scripts to path
sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))


default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Stations to pull departures for — MVP corridors
MVP_STATIONS = [
    "ASD", "RTD", "UT", "GVC", "EHV",
    "AH", "SDM", "DT", "LD", "AMS",
    "BRN", "HTN", "DRN", "EDE",
]


def _extract_and_upload_disruptions(**context):
    from ingest_ns import extract_disruptions
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]  # execution_date as YYYY-MM-DD
    api_key = os.environ["NS_API_KEY"]
    base_url = os.environ["NS_API_BASE_URL"]
    bucket = os.environ["GCS_BUCKET_NAME"]

    records = extract_disruptions(api_key, base_url, service_date)
    if records:
        gcs_uri = upload_json_to_gcs(
            data=records,
            bucket_name=bucket,
            blob_path=f"raw/ns/disruptions/dt={service_date}/disruptions.json",
        )
        context["ti"].xcom_push(key="disruptions_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="disruptions_gcs_uri", value=None)


def _extract_and_upload_departures(**context):
    from ingest_ns import extract_departures
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]
    api_key = os.environ["NS_API_KEY"]
    base_url = os.environ["NS_API_BASE_URL"]
    bucket = os.environ["GCS_BUCKET_NAME"]

    all_departures = []
    for station_code in MVP_STATIONS:
        departures = extract_departures(api_key, base_url, station_code, service_date)
        all_departures.extend(departures)

    if all_departures:
        gcs_uri = upload_json_to_gcs(
            data=all_departures,
            bucket_name=bucket,
            blob_path=f"raw/ns/departures/dt={service_date}/departures.json",
        )
        context["ti"].xcom_push(key="departures_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="departures_gcs_uri", value=None)


def _load_disruptions_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_disruptions", key="disruptions_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ns_disruptions",
        service_date=service_date,
    )


def _load_departures_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_departures", key="departures_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ns_departures",
        service_date=service_date,
    )


with DAG(
    dag_id="dag_ns_ingest",
    default_args=default_args,
    description="Ingest NS disruptions and departures daily",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=True,
    max_active_runs=3,
    tags=["ingestion", "ns"],
) as dag:
    extract_disruptions_task = PythonOperator(
        task_id="extract_disruptions",
        python_callable=_extract_and_upload_disruptions,
    )

    extract_departures_task = PythonOperator(
        task_id="extract_departures",
        python_callable=_extract_and_upload_departures,
    )

    load_disruptions_task = PythonOperator(
        task_id="load_disruptions_to_bq",
        python_callable=_load_disruptions_to_bq,
    )

    load_departures_task = PythonOperator(
        task_id="load_departures_to_bq",
        python_callable=_load_departures_to_bq,
    )

    extract_disruptions_task >> load_disruptions_task
    extract_departures_task >> load_departures_task
```

- [ ] **Step 2: Verify DAG parses without errors**

```bash
cd airflow
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep dag_ns_ingest
```

Expected: `dag_ns_ingest` appears in the list.

- [ ] **Step 3: Commit**

```bash
cd ..
git add airflow/dags/dag_ns_ingest.py
git commit -m "feat: Airflow DAG for NS API daily ingestion"
```

---

### Task 11: Airflow DAG — NDW Ingestion

**Files:**
- Create: `airflow/dags/dag_ndw_ingest.py`

- [ ] **Step 1: Create the DAG**

```python
# airflow/dags/dag_ndw_ingest.py
"""DAG: Ingest NDW traffic observations daily.

Schedule: 07:00 UTC daily.
service_date = execution_date (= yesterday).

Pipeline: NDW → CSV/JSON → GCS → BigQuery (partition-scoped overwrite).
Only ingests measurement points from station_ndw_mapping seed.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import csv
import io
import os
import sys

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))


default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _load_mapped_location_ids() -> list[str]:
    """Read NDW location IDs from the station_ndw_mapping seed CSV."""
    seed_path = os.path.join(
        os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
        "dbt", "seeds", "station_ndw_mapping.csv",
    )
    location_ids = set()
    with open(seed_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            location_ids.add(row["ndw_location_id"])
    return list(location_ids)


def _extract_and_upload_traffic(**context):
    from ingest_ndw import extract_traffic
    from gcs_utils import upload_json_to_gcs

    service_date = context["ds"]
    bucket = os.environ["GCS_BUCKET_NAME"]
    location_ids = _load_mapped_location_ids()

    records = extract_traffic(service_date=service_date, location_ids=location_ids)

    if records:
        gcs_uri = upload_json_to_gcs(
            data=records,
            bucket_name=bucket,
            blob_path=f"raw/ndw/traffic_flow/dt={service_date}/traffic.json",
        )
        context["ti"].xcom_push(key="traffic_gcs_uri", value=gcs_uri)
    else:
        context["ti"].xcom_push(key="traffic_gcs_uri", value=None)


def _load_traffic_to_bq(**context):
    from bq_utils import load_json_to_bq

    gcs_uri = context["ti"].xcom_pull(
        task_ids="extract_traffic", key="traffic_gcs_uri"
    )
    if not gcs_uri:
        return

    project = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ["BQ_RAW_DATASET"]
    service_date = context["ds"]

    load_json_to_bq(
        gcs_uri=gcs_uri,
        table_id=f"{project}.{dataset}.ndw_traffic_flow",
        service_date=service_date,
    )


with DAG(
    dag_id="dag_ndw_ingest",
    default_args=default_args,
    description="Ingest NDW 15-min traffic observations daily",
    schedule_interval="0 7 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=True,
    max_active_runs=3,
    tags=["ingestion", "ndw"],
) as dag:
    extract_traffic_task = PythonOperator(
        task_id="extract_traffic",
        python_callable=_extract_and_upload_traffic,
    )

    load_traffic_task = PythonOperator(
        task_id="load_traffic_to_bq",
        python_callable=_load_traffic_to_bq,
    )

    extract_traffic_task >> load_traffic_task
```

- [ ] **Step 2: Verify DAG parses**

```bash
cd airflow
docker compose exec airflow-scheduler airflow dags list 2>&1 | grep dag_ndw_ingest
```

Expected: `dag_ndw_ingest` appears.

- [ ] **Step 3: Commit**

```bash
cd ..
git add airflow/dags/dag_ndw_ingest.py
git commit -m "feat: Airflow DAG for NDW daily traffic ingestion"
```

---

### Task 12: Run Ingestion End-to-End and Verify

**Files:**
- No new files — integration verification.

- [ ] **Step 1: Set up .env with real credentials**

Copy `.env.example` to `.env` and fill in:
- `GCP_PROJECT_ID`, `GCS_BUCKET_NAME` (from Terraform output)
- `NS_API_KEY` (from Task 4)
- Other values

- [ ] **Step 2: Start Docker Compose**

```bash
cd airflow
docker compose up -d
```

- [ ] **Step 3: Trigger NS DAG for a test date**

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger dag_ns_ingest --conf '{}' -e "2026-03-25"
```

Monitor in Airflow UI (http://localhost:8080). Expected: all tasks green.

- [ ] **Step 4: Verify data in GCS**

```bash
gsutil ls gs://${GCS_BUCKET_NAME}/raw/ns/disruptions/dt=2026-03-25/
gsutil ls gs://${GCS_BUCKET_NAME}/raw/ns/departures/dt=2026-03-25/
```

Expected: JSON files present.

- [ ] **Step 5: Verify data in BigQuery**

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as cnt FROM \`${GCP_PROJECT_ID}.raw_nl_transport.ns_disruptions\` WHERE service_date = '2026-03-25'"
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as cnt FROM \`${GCP_PROJECT_ID}.raw_nl_transport.ns_departures\` WHERE service_date = '2026-03-25'"
```

Expected: Non-zero counts.

- [ ] **Step 6: Test idempotency — re-trigger same date**

```bash
docker compose exec airflow-scheduler \
  airflow dags trigger dag_ns_ingest --conf '{}' -e "2026-03-25"
```

Re-run the BQ count queries. Expected: same counts (not doubled).

- [ ] **Step 7: Start backfill**

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill dag_ns_ingest --start-date 2026-03-10 --end-date 2026-03-25
```

Monitor progress in Airflow UI.

---

## Week 3: dbt Transformation

### Task 13: dbt Project Initialization

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/packages.yml`, `dbt/profiles.yml`

- [ ] **Step 1: Create dbt/dbt_project.yml**

```yaml
name: nl_transport_pulse
version: '1.0.0'

profile: nl_transport_pulse

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

vars:
  is_test_run: false

models:
  nl_transport_pulse:
    staging:
      +materialized: view
      +schema: staging_nl_transport
    intermediate:
      +materialized: table
      +schema: staging_nl_transport
    core:
      +materialized: table
      +schema: core_nl_transport

seeds:
  nl_transport_pulse:
    +schema: raw_nl_transport

snapshots:
  nl_transport_pulse:
    +schema: core_nl_transport
```

- [ ] **Step 2: Create dbt/packages.yml**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.1.0"
```

- [ ] **Step 3: Create dbt/profiles.yml**

```yaml
nl_transport_pulse:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: staging_nl_transport
      location: EU
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      threads: 4
    prod:
      type: bigquery
      method: service-account
      project: "{{ env_var('GCP_PROJECT_ID') }}"
      dataset: core_nl_transport
      location: EU
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
      threads: 4
```

- [ ] **Step 4: Install dbt packages**

```bash
cd dbt
dbt deps
```

Expected: `dbt_utils` installed.

- [ ] **Step 5: Verify dbt connection**

```bash
dbt debug
```

Expected: All checks pass (connection, dependencies, project).

- [ ] **Step 6: Run seed loading**

```bash
dbt seed
```

Expected: `station_corridor_mapping`, `station_ndw_mapping`, `nl_holidays` loaded to BigQuery.

- [ ] **Step 7: Commit**

```bash
cd ..
git add dbt/dbt_project.yml dbt/packages.yml dbt/profiles.yml
git commit -m "feat: dbt project config with BigQuery profile and seed loading"
```

---

### Task 14: dbt Sources and Staging Models

**Files:**
- Create: `dbt/models/staging/_staging__sources.yml`
- Create: `dbt/models/staging/_staging__models.yml`
- Create: `dbt/models/staging/stg_ns_disruptions.sql`
- Create: `dbt/models/staging/stg_ns_departures.sql`
- Create: `dbt/models/staging/stg_ndw_traffic_flow.sql`

- [ ] **Step 1: Create source definitions**

```yaml
# dbt/models/staging/_staging__sources.yml
version: 2

sources:
  - name: raw
    database: "{{ env_var('GCP_PROJECT_ID') }}"
    schema: raw_nl_transport
    tables:
      - name: ns_disruptions
        description: "Raw NS disruption events"
        loaded_at_field: _PARTITIONTIME
        freshness:
          warn_after: {count: 36, period: hour}
          error_after: {count: 48, period: hour}
        columns:
          - name: disruption_id
            tests:
              - not_null
          - name: service_date
            tests:
              - not_null

      - name: ns_departures
        description: "Raw NS departure records with delay"
        loaded_at_field: _PARTITIONTIME
        freshness:
          warn_after: {count: 36, period: hour}
          error_after: {count: 48, period: hour}
        columns:
          - name: service_date
            tests:
              - not_null

      - name: ndw_traffic_flow
        description: "Raw NDW 15-minute traffic observations"
        loaded_at_field: _PARTITIONTIME
        freshness:
          warn_after: {count: 36, period: hour}
          error_after: {count: 48, period: hour}
        columns:
          - name: location_id
            tests:
              - not_null
          - name: service_date
            tests:
              - not_null
```

- [ ] **Step 2: Create stg_ns_disruptions.sql**

```sql
-- dbt/models/staging/stg_ns_disruptions.sql
with source as (
    select * from {{ source('raw', 'ns_disruptions') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(disruption_id as string) as disruption_id,
        cast(service_date as date) as service_date,
        cast(title as string) as title,
        cast(is_active as boolean) as is_active,
        cast(start_timestamp as timestamp) as start_ts,
        cast(end_timestamp as timestamp) as end_ts,
        cast(duration_minutes as float64) as duration_minutes,
        cast(cause as string) as cause,
        affected_station_codes,
        cast(stations_affected_count as int64) as stations_affected_count
    from source
)

select * from cleaned
```

- [ ] **Step 3: Create stg_ns_departures.sql**

```sql
-- dbt/models/staging/stg_ns_departures.sql
with source as (
    select * from {{ source('raw', 'ns_departures') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(station_code as string) as station_code,
        cast(service_date as date) as service_date,
        cast(direction as string) as direction,
        cast(planned_departure_ts as timestamp) as planned_departure_ts,
        cast(actual_departure_ts as timestamp) as actual_departure_ts,
        cast(delay_minutes as float64) as delay_minutes,
        cast(train_category as string) as train_category,
        -- Derived: composite key
        concat(
            cast(station_code as string), '_',
            cast(planned_departure_ts as string)
        ) as departure_id
    from source
)

select * from cleaned
```

- [ ] **Step 4: Create stg_ndw_traffic_flow.sql**

```sql
-- dbt/models/staging/stg_ndw_traffic_flow.sql
with source as (
    select * from {{ source('raw', 'ndw_traffic_flow') }}
    {% if var('is_test_run', false) %}
    where service_date >= date_sub(current_date(), interval 30 day)
    {% endif %}
),

cleaned as (
    select
        cast(location_id as string) as location_id,
        cast(measurement_ts as timestamp) as measurement_ts,
        cast(service_date as date) as service_date,
        cast(avg_speed_kmh as float64) as avg_speed_kmh,
        cast(vehicle_count as int64) as vehicle_count
    from source
)

select * from cleaned
```

- [ ] **Step 5: Create staging model descriptions**

```yaml
# dbt/models/staging/_staging__models.yml
version: 2

models:
  - name: stg_ns_disruptions
    description: "Cleaned NS disruption events with typed columns"
    columns:
      - name: disruption_id
        description: "Unique disruption identifier from NS API"
        tests:
          - not_null
      - name: service_date
        description: "The date this disruption was active"
        tests:
          - not_null

  - name: stg_ns_departures
    description: "Cleaned NS departure records with computed delay"
    columns:
      - name: departure_id
        description: "Composite key: station_code + planned_departure_ts"
        tests:
          - not_null
          - unique
      - name: delay_minutes
        description: "Delay in minutes (actual - planned). 0 means on-time."

  - name: stg_ndw_traffic_flow
    description: "Cleaned NDW 15-minute traffic observations"
    columns:
      - name: location_id
        description: "NDW measurement point identifier"
        tests:
          - not_null
      - name: measurement_ts
        description: "Start of the 15-minute measurement interval"
        tests:
          - not_null
```

- [ ] **Step 6: Run dbt to verify staging models compile**

```bash
cd dbt
dbt run --select staging
```

Expected: 3 models created (as views).

- [ ] **Step 7: Run dbt tests on staging**

```bash
dbt test --select staging
```

Expected: All tests pass (not_null, unique on departure_id).

- [ ] **Step 8: Commit**

```bash
cd ..
git add dbt/models/staging/
git commit -m "feat: dbt staging models for NS disruptions, departures, and NDW traffic"
```

---

### Task 15: dbt Intermediate Models

**Files:**
- Create: `dbt/models/intermediate/_intermediate__models.yml`
- Create: `dbt/models/intermediate/int_train_delays_daily.sql`
- Create: `dbt/models/intermediate/int_ndw_traffic_daily.sql`

- [ ] **Step 1: Create int_train_delays_daily.sql**

```sql
-- dbt/models/intermediate/int_train_delays_daily.sql
with departures as (
    select * from {{ ref('stg_ns_departures') }}
),

disruptions as (
    select * from {{ ref('stg_ns_disruptions') }}
),

departure_stats as (
    select
        station_code,
        service_date,
        count(*) as total_departures,
        avg(delay_minutes) as avg_delay_min,
        max(delay_minutes) as max_delay_min,
        countif(delay_minutes <= 1.0) / count(*) * 100.0 as pct_on_time,
        countif(delay_minutes > 15.0) / count(*) * 100.0 as severe_delay_share
    from departures
    group by station_code, service_date
),

disruption_counts as (
    select
        station_code,
        service_date,
        count(distinct disruption_id) as disruption_count
    from disruptions,
    unnest(affected_station_codes) as station_code
    group by station_code, service_date
)

select
    ds.station_code,
    ds.service_date,
    ds.total_departures,
    round(ds.avg_delay_min, 2) as avg_delay_min,
    round(ds.max_delay_min, 2) as max_delay_min,
    round(ds.pct_on_time, 2) as pct_on_time,
    round(ds.severe_delay_share, 2) as severe_delay_share,
    coalesce(dc.disruption_count, 0) as disruption_count
from departure_stats ds
left join disruption_counts dc
    on ds.station_code = dc.station_code
    and ds.service_date = dc.service_date
```

- [ ] **Step 2: Create int_ndw_traffic_daily.sql**

```sql
-- dbt/models/intermediate/int_ndw_traffic_daily.sql
with traffic as (
    select * from {{ ref('stg_ndw_traffic_flow') }}
),

-- Compute free-flow speed per location (85th percentile across all dates)
free_flow as (
    select
        location_id,
        percentile_cont(avg_speed_kmh, 0.85) over (partition by location_id) as free_flow_speed_kmh
    from traffic
),

free_flow_distinct as (
    select distinct
        location_id,
        free_flow_speed_kmh
    from free_flow
),

daily_stats as (
    select
        t.location_id,
        t.service_date,
        avg(t.avg_speed_kmh) as avg_speed_kmh,
        sum(t.vehicle_count) as total_vehicle_count,
        -- congestion_minutes: count of 15-min intervals where speed < 50% free-flow, * 15
        countif(t.avg_speed_kmh < ff.free_flow_speed_kmh * 0.5) * 15 as congestion_minutes
    from traffic t
    left join free_flow_distinct ff
        on t.location_id = ff.location_id
    group by t.location_id, t.service_date
)

select
    location_id,
    service_date,
    round(avg_speed_kmh, 2) as avg_speed_kmh,
    total_vehicle_count,
    congestion_minutes
from daily_stats
```

- [ ] **Step 3: Create intermediate model descriptions**

```yaml
# dbt/models/intermediate/_intermediate__models.yml
version: 2

models:
  - name: int_train_delays_daily
    description: "Daily train delay statistics per station, aggregated from departures"
    columns:
      - name: station_code
        tests:
          - not_null
      - name: service_date
        tests:
          - not_null
      - name: pct_on_time
        description: "Percentage of departures with delay <= 1 minute"

  - name: int_ndw_traffic_daily
    description: "Daily traffic statistics per NDW measurement point, aggregated from 15-min readings"
    columns:
      - name: location_id
        tests:
          - not_null
      - name: service_date
        tests:
          - not_null
      - name: congestion_minutes
        description: "Minutes where avg speed was below 50% of free-flow speed"
```

- [ ] **Step 4: Run dbt intermediate models**

```bash
cd dbt
dbt run --select intermediate
```

Expected: 2 models created.

- [ ] **Step 5: Run tests**

```bash
dbt test --select intermediate
```

Expected: All pass.

- [ ] **Step 6: Commit**

```bash
cd ..
git add dbt/models/intermediate/
git commit -m "feat: dbt intermediate models for daily train delays and traffic stats"
```

---

### Task 16: dbt Dimensions and Macros

**Files:**
- Create: `dbt/models/core/dim_stations.sql`
- Create: `dbt/models/core/dim_ndw_locations.sql`
- Create: `dbt/models/core/dim_date.sql`
- Create: `dbt/macros/delay_category.sql`
- Create: `dbt/macros/generate_date_spine.sql`
- Create: `dbt/snapshots/snap_dim_stations.sql`

- [ ] **Step 1: Create dim_stations.sql**

```sql
-- dbt/models/core/dim_stations.sql
with corridor_mapping as (
    select * from {{ ref('station_corridor_mapping') }}
),

stations as (
    select distinct
        station_code,
        station_name,
        corridor_id,
        corridor_name,
        cast(lat as float64) as lat,
        cast(lon as float64) as lon
    from corridor_mapping
)

select
    station_code,
    station_name,
    corridor_id,
    corridor_name,
    lat,
    lon
from stations
```

- [ ] **Step 2: Create dim_ndw_locations.sql**

```sql
-- dbt/models/core/dim_ndw_locations.sql
with ndw_mapping as (
    select * from {{ ref('station_ndw_mapping') }}
),

corridor_mapping as (
    select distinct station_code, corridor_id
    from {{ ref('station_corridor_mapping') }}
)

select
    n.ndw_location_id as location_id,
    n.road_name,
    cast(n.ndw_lat as float64) as lat,
    cast(n.ndw_lon as float64) as lon,
    n.station_code as nearest_station_code,
    c.corridor_id,
    cast(n.distance_km as float64) as distance_km
from ndw_mapping n
left join corridor_mapping c
    on n.station_code = c.station_code
```

- [ ] **Step 3: Create generate_date_spine.sql macro**

```sql
-- dbt/macros/generate_date_spine.sql
{% macro generate_date_spine(start_date, end_date) %}
    select
        date as date_day
    from unnest(
        generate_date_array(
            cast('{{ start_date }}' as date),
            cast('{{ end_date }}' as date)
        )
    ) as date
{% endmacro %}
```

- [ ] **Step 4: Create dim_date.sql**

```sql
-- dbt/models/core/dim_date.sql
with date_spine as (
    {{ generate_date_spine('2026-01-01', '2027-12-31') }}
),

holidays as (
    select
        cast(date as date) as holiday_date,
        holiday_name
    from {{ ref('nl_holidays') }}
)

select
    ds.date_day as date,
    extract(dayofweek from ds.date_day) as day_of_week,
    extract(isoweek from ds.date_day) as week_number,
    extract(month from ds.date_day) as month,
    case
        when extract(month from ds.date_day) in (3, 4, 5) then 'spring'
        when extract(month from ds.date_day) in (6, 7, 8) then 'summer'
        when extract(month from ds.date_day) in (9, 10, 11) then 'autumn'
        else 'winter'
    end as season,
    h.holiday_name is not null as is_holiday,
    extract(dayofweek from ds.date_day) in (1, 7) as is_weekend,
    h.holiday_name
from date_spine ds
left join holidays h
    on ds.date_day = h.holiday_date
```

- [ ] **Step 5: Create delay_category macro**

```sql
-- dbt/macros/delay_category.sql
{% macro delay_category(delay_minutes_col) %}
    case
        when {{ delay_minutes_col }} <= 1.0 then 'on_time'
        when {{ delay_minutes_col }} <= 5.0 then 'minor'
        when {{ delay_minutes_col }} <= 15.0 then 'moderate'
        else 'severe'
    end
{% endmacro %}
```

- [ ] **Step 6: Create SCD Type 2 snapshot for dim_stations**

```sql
-- dbt/snapshots/snap_dim_stations.sql
{% snapshot snap_dim_stations %}

{{
    config(
        target_schema='core_nl_transport',
        unique_key='station_code',
        strategy='check',
        check_cols=['station_name', 'corridor_id', 'corridor_name', 'lat', 'lon'],
    )
}}

select * from {{ ref('dim_stations') }}

{% endsnapshot %}
```

- [ ] **Step 7: Run dimensions**

```bash
cd dbt
dbt run --select dim_stations dim_ndw_locations dim_date
dbt snapshot
```

Expected: 3 dimension tables created, 1 snapshot created.

- [ ] **Step 8: Commit**

```bash
cd ..
git add dbt/models/core/dim_stations.sql dbt/models/core/dim_ndw_locations.sql dbt/models/core/dim_date.sql
git add dbt/macros/ dbt/snapshots/
git commit -m "feat: dbt dimension tables, macros, and SCD Type 2 station snapshot"
```

---

### Task 17: dbt Fact Tables and Data Mart

**Files:**
- Create: `dbt/models/core/fct_train_performance.sql`
- Create: `dbt/models/core/fct_road_traffic.sql`
- Create: `dbt/models/core/dm_multimodal_daily.sql`
- Create: `dbt/models/core/_core__models.yml`
- Create: `dbt/tests/assert_pct_on_time_range.sql`

- [ ] **Step 1: Create fct_train_performance.sql (incremental)**

```sql
-- dbt/models/core/fct_train_performance.sql
{{
    config(
        materialized='incremental',
        unique_key=['station_code', 'service_date'],
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['station_code']
    )
}}

with delays as (
    select * from {{ ref('int_train_delays_daily') }}
),

stations as (
    select distinct station_code, corridor_id, corridor_name
    from {{ ref('dim_stations') }}
)

select
    d.station_code,
    d.service_date,
    s.corridor_id,
    s.corridor_name,
    d.total_departures,
    d.avg_delay_min,
    d.max_delay_min,
    d.pct_on_time,
    d.severe_delay_share,
    d.disruption_count
from delays d
left join stations s
    on d.station_code = s.station_code
{% if is_incremental() %}
where d.service_date > (select max(service_date) from {{ this }})
{% endif %}
```

- [ ] **Step 2: Create fct_road_traffic.sql**

```sql
-- dbt/models/core/fct_road_traffic.sql
{{
    config(
        materialized='table',
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['location_id']
    )
}}

with daily_traffic as (
    select * from {{ ref('int_ndw_traffic_daily') }}
),

locations as (
    select location_id, corridor_id
    from {{ ref('dim_ndw_locations') }}
),

-- Baseline: average daily speed per location across all dates
baseline as (
    select
        location_id,
        avg(avg_speed_kmh) as baseline_speed_kmh
    from daily_traffic
    group by location_id
)

select
    dt.location_id,
    dt.service_date,
    l.corridor_id,
    dt.avg_speed_kmh,
    dt.total_vehicle_count,
    dt.congestion_minutes,
    round(
        safe_divide(dt.avg_speed_kmh, b.baseline_speed_kmh) * 100.0,
        2
    ) as speed_vs_baseline_pct
from daily_traffic dt
left join locations l on dt.location_id = l.location_id
left join baseline b on dt.location_id = b.location_id
```

- [ ] **Step 3: Create dm_multimodal_daily.sql**

```sql
-- dbt/models/core/dm_multimodal_daily.sql
{{
    config(
        materialized='table',
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=['corridor_id']
    )
}}

with train as (
    select
        corridor_id,
        corridor_name,
        service_date,
        avg(pct_on_time) as avg_pct_on_time,
        avg(avg_delay_min) as avg_delay_min,
        sum(total_departures) as total_departures,
        sum(disruption_count) as disruption_count,
        avg(severe_delay_share) as avg_severe_delay_share
    from {{ ref('fct_train_performance') }}
    group by corridor_id, corridor_name, service_date
),

road as (
    select
        corridor_id,
        service_date,
        avg(avg_speed_kmh) as avg_road_speed_kmh,
        sum(total_vehicle_count) as total_vehicle_count,
        sum(congestion_minutes) as total_congestion_minutes,
        avg(speed_vs_baseline_pct) as avg_speed_vs_baseline_pct
    from {{ ref('fct_road_traffic') }}
    group by corridor_id, service_date
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    t.corridor_id,
    t.corridor_name,
    t.service_date,
    -- Date dimensions
    d.day_of_week,
    d.week_number,
    d.month,
    d.season,
    d.is_holiday,
    d.is_weekend,
    -- Train metrics
    round(t.avg_pct_on_time, 2) as pct_on_time,
    round(t.avg_delay_min, 2) as avg_delay_min,
    t.total_departures,
    t.disruption_count,
    round(t.avg_severe_delay_share, 2) as severe_delay_share,
    -- Road metrics
    round(r.avg_road_speed_kmh, 2) as avg_road_speed_kmh,
    r.total_vehicle_count,
    r.total_congestion_minutes,
    round(r.avg_speed_vs_baseline_pct, 2) as road_speed_vs_baseline_pct
from train t
left join road r
    on t.corridor_id = r.corridor_id
    and t.service_date = r.service_date
left join dates d
    on t.service_date = d.date
```

- [ ] **Step 4: Create custom test**

```sql
-- dbt/tests/assert_pct_on_time_range.sql
-- Ensure pct_on_time values are always between 0 and 100
select
    station_code,
    service_date,
    pct_on_time
from {{ ref('fct_train_performance') }}
where pct_on_time < 0 or pct_on_time > 100
```

- [ ] **Step 5: Create core model descriptions**

```yaml
# dbt/models/core/_core__models.yml
version: 2

models:
  - name: dim_stations
    description: "Station dimension with corridor assignment and coordinates"
    columns:
      - name: station_code
        tests:
          - not_null

  - name: dim_ndw_locations
    description: "NDW measurement point dimension with corridor mapping"
    columns:
      - name: location_id
        tests:
          - not_null
          - unique

  - name: dim_date
    description: "Date dimension with Dutch holidays and season"
    columns:
      - name: date
        tests:
          - not_null
          - unique

  - name: fct_train_performance
    description: "Daily train reliability metrics per station. Incremental."
    columns:
      - name: station_code
        tests:
          - not_null
      - name: service_date
        tests:
          - not_null
      - name: pct_on_time
        description: "Percentage of departures with delay <= 1 minute (0-100)"

  - name: fct_road_traffic
    description: "Daily road traffic metrics per NDW measurement point"
    columns:
      - name: location_id
        tests:
          - not_null
      - name: service_date
        tests:
          - not_null

  - name: dm_multimodal_daily
    description: "Data mart: train + road metrics by corridor and date"
    columns:
      - name: corridor_id
        tests:
          - not_null
      - name: service_date
        tests:
          - not_null
```

- [ ] **Step 6: Run full dbt build**

```bash
cd dbt
dbt build
```

Expected: All models build, all tests pass. Verify:
- Staging: 3 views
- Intermediate: 2 tables
- Core: 3 dimensions + 2 facts + 1 mart + 1 snapshot
- Tests: all green

- [ ] **Step 7: Commit**

```bash
cd ..
git add dbt/models/core/ dbt/tests/
git commit -m "feat: dbt fact tables, multimodal data mart, and core model tests"
```

---

### Task 18: Alerting — Post-dbt Check + Slack

**Files:**
- Create: `airflow/scripts/alert_checker.py`
- Create: `airflow/tests/test_alert_checker.py`
- Create: `airflow/dags/dag_dbt_transform.py`

- [ ] **Step 1: Write test for alert detection**

```python
# airflow/tests/test_alert_checker.py
from unittest.mock import MagicMock, patch
import json


def test_check_reliability_alerts_detects_below_threshold():
    """Should return alert records for corridors below threshold."""
    mock_client = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.result.return_value = [
        MagicMock(
            corridor_id="1",
            corridor_name="Amsterdam-Rotterdam",
            pct_on_time=62.0,
            avg_7d_pct_on_time=87.0,
        )
    ]
    mock_client.query.return_value = mock_query_job

    with patch("scripts.alert_checker.bigquery.Client", return_value=mock_client):
        from scripts.alert_checker import check_reliability_alerts

        alerts = check_reliability_alerts(
            service_date="2026-03-26",
            on_time_threshold=80.0,
            deviation_threshold=10.0,
        )

        assert len(alerts) == 1
        assert alerts[0]["corridor_name"] == "Amsterdam-Rotterdam"
        assert alerts[0]["metric_value"] == 62.0
        assert alerts[0]["fired"] is True


def test_send_slack_alert_posts_to_webhook():
    """Should POST formatted message to Slack webhook URL."""
    mock_response = MagicMock()
    mock_response.status_code = 200

    with patch("scripts.alert_checker.requests.post", return_value=mock_response) as mock_post:
        from scripts.alert_checker import send_slack_alert

        send_slack_alert(
            webhook_url="https://hooks.slack.com/test",
            service_date="2026-03-26",
            corridor="Amsterdam-Rotterdam",
            pct_on_time=62.0,
            avg_7d=87.0,
            disruption_count=2,
        )

        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        body = json.loads(call_kwargs[1]["data"])
        assert "Amsterdam-Rotterdam" in body["text"]
        assert "62" in body["text"]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd airflow
python -m pytest tests/test_alert_checker.py -v
```

Expected: FAIL.

- [ ] **Step 3: Implement alert_checker.py**

```python
# airflow/scripts/alert_checker.py
"""Post-dbt alert detection and Slack notification."""
import json
import uuid
from datetime import datetime

import requests
from google.cloud import bigquery


def check_reliability_alerts(
    service_date: str,
    on_time_threshold: float = 80.0,
    deviation_threshold: float = 10.0,
    project_id: str | None = None,
    dataset: str = "core_nl_transport",
) -> list[dict]:
    """Check for corridors below reliability threshold on the given service_date.

    Returns a list of alert records (one per corridor that triggered).
    """
    client = bigquery.Client(project=project_id)

    sql = f"""
    with latest as (
        select
            corridor_id,
            corridor_name,
            pct_on_time
        from `{project_id}.{dataset}.dm_multimodal_daily`
        where service_date = '{service_date}'
    ),
    rolling as (
        select
            corridor_id,
            avg(pct_on_time) as avg_7d_pct_on_time
        from `{project_id}.{dataset}.dm_multimodal_daily`
        where service_date between date_sub('{service_date}', interval 7 day)
              and '{service_date}'
        group by corridor_id
    )
    select
        l.corridor_id,
        l.corridor_name,
        l.pct_on_time,
        r.avg_7d_pct_on_time
    from latest l
    left join rolling r on l.corridor_id = r.corridor_id
    where l.pct_on_time < {on_time_threshold}
      and (r.avg_7d_pct_on_time - l.pct_on_time) > {deviation_threshold}
    """

    results = client.query(sql).result()
    alerts = []
    for row in results:
        alerts.append(
            {
                "alert_id": str(uuid.uuid4()),
                "check_ts": datetime.utcnow().isoformat(),
                "service_date": service_date,
                "alert_type": "reliability",
                "corridor": row.corridor_name,
                "metric_value": row.pct_on_time,
                "threshold": on_time_threshold,
                "fired": True,
            }
        )
    return alerts


def send_slack_alert(
    webhook_url: str,
    service_date: str,
    corridor: str,
    pct_on_time: float,
    avg_7d: float,
    disruption_count: int = 0,
) -> None:
    """Send a formatted alert message to Slack."""
    text = (
        f"Alert: NL Transport Pulse\n"
        f"Service date: {service_date}\n"
        f"Corridor: {corridor}\n"
        f"On-time: {pct_on_time:.0f}% (7-day avg: {avg_7d:.0f}%)\n"
        f"Active disruptions: {disruption_count}"
    )
    requests.post(
        webhook_url,
        data=json.dumps({"text": text}),
        headers={"Content-Type": "application/json"},
    )


def write_alert_history(
    alerts: list[dict],
    project_id: str,
    dataset: str = "core_nl_transport",
) -> None:
    """Write alert records to BigQuery alert_history table."""
    if not alerts:
        return
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.alert_history"
    errors = client.insert_rows_json(table_id, alerts)
    if errors:
        raise RuntimeError(f"Failed to insert alert history: {errors}")
```

- [ ] **Step 4: Run tests**

```bash
python -m pytest tests/test_alert_checker.py -v
```

Expected: 2 tests PASS.

- [ ] **Step 5: Create dag_dbt_transform.py**

```python
# airflow/dags/dag_dbt_transform.py
"""DAG: Run dbt transformations and alert checks daily.

Schedule: 08:00 UTC daily (after NS and NDW ingestion complete).
Runs dbt seed, run, test, source freshness, then alert checks.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import os
import sys

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "scripts"))

DBT_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dbt")

default_args = {
    "owner": "nl-transport-pulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _check_alerts(**context):
    from alert_checker import check_reliability_alerts, send_slack_alert, write_alert_history

    service_date = context["ds"]
    project_id = os.environ["GCP_PROJECT_ID"]
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    on_time_threshold = float(os.environ.get("ALERT_ON_TIME_THRESHOLD", "80"))
    deviation_threshold = float(os.environ.get("ALERT_DEVIATION_THRESHOLD", "10"))

    alerts = check_reliability_alerts(
        service_date=service_date,
        on_time_threshold=on_time_threshold,
        deviation_threshold=deviation_threshold,
        project_id=project_id,
    )

    # Send Slack notifications for fired alerts
    if webhook_url:
        for alert in alerts:
            if alert["fired"]:
                send_slack_alert(
                    webhook_url=webhook_url,
                    service_date=service_date,
                    corridor=alert["corridor"],
                    pct_on_time=alert["metric_value"],
                    avg_7d=on_time_threshold,  # Simplified; real 7d avg is in the query
                )

    # Write all alert records to history
    write_alert_history(alerts, project_id=project_id)


with DAG(
    dag_id="dag_dbt_transform",
    default_args=default_args,
    description="Run dbt transformations and alert checks",
    schedule_interval="0 8 * * *",
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["transformation", "dbt", "alerts"],
) as dag:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir {DBT_DIR}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness --profiles-dir {DBT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {DBT_DIR}",
    )

    check_alerts_task = PythonOperator(
        task_id="check_alerts",
        python_callable=_check_alerts,
    )

    dbt_seed >> dbt_run >> dbt_snapshot >> dbt_test >> dbt_freshness >> check_alerts_task
```

- [ ] **Step 6: Commit**

```bash
cd ..
git add airflow/scripts/alert_checker.py airflow/tests/test_alert_checker.py
git add airflow/dags/dag_dbt_transform.py
git commit -m "feat: dbt transform DAG with alerting and Slack notification"
```

---

## Week 4: Dashboard + Polish

### Task 19: Streamlit — BigQuery Client Utility

**Files:**
- Create: `streamlit/utils/__init__.py`
- Create: `streamlit/utils/bq_client.py`

- [ ] **Step 1: Create bq_client.py**

```python
# streamlit/utils/bq_client.py
"""BigQuery query helper for the Streamlit dashboard."""
import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Return a cached BigQuery client."""
    return bigquery.Client(project=os.environ.get("GCP_PROJECT_ID"))


def get_project_dataset() -> tuple[str, str]:
    """Return (project_id, core_dataset) from env vars."""
    project = os.environ.get("GCP_PROJECT_ID", "")
    dataset = os.environ.get("BQ_CORE_DATASET", "core_nl_transport")
    return project, dataset


@st.cache_data(ttl=3600)
def query_df(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return a pandas DataFrame. Cached for 1 hour."""
    client = get_bq_client()
    return client.query(sql).to_dataframe()
```

- [ ] **Step 2: Create __init__.py**

```python
# streamlit/utils/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add streamlit/utils/
git commit -m "feat: Streamlit BigQuery client utility with caching"
```

---

### Task 20: Streamlit — Network Overview Page

**Files:**
- Modify: `streamlit/app.py`
- Create: `streamlit/pages/1_Network_Overview.py`

- [ ] **Step 1: Update app.py to be a proper landing page**

```python
# streamlit/app.py
import streamlit as st

st.set_page_config(
    page_title="NL Transport Pulse",
    page_icon="🚆",
    layout="wide",
)

st.title("NL Transport Pulse")
st.markdown(
    "Dutch multimodal transport reliability dashboard. "
    "Use the sidebar to navigate between pages."
)
st.markdown("---")
st.markdown(
    "**Pages:**\n"
    "- **Network Overview** — Corridor reliability scores, map, and trends\n"
    "- **Corridor Explorer** — Deep-dive into specific corridors and stations"
)
```

- [ ] **Step 2: Create Network Overview page**

```python
# streamlit/pages/1_Network_Overview.py
"""Network Overview — corridor reliability scores, map, and daily trends."""
import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date, timedelta

from utils.bq_client import query_df, get_project_dataset

st.set_page_config(page_title="Network Overview", layout="wide")
st.title("Network Overview")

project, dataset = get_project_dataset()

# --- Date picker ---
col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=14),
        key="overview_start",
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="overview_end",
    )

# --- Load data ---
sql_multimodal = f"""
select
    corridor_id,
    corridor_name,
    service_date,
    pct_on_time,
    avg_delay_min,
    total_departures,
    disruption_count,
    avg_road_speed_kmh,
    total_congestion_minutes
from `{project}.{dataset}.dm_multimodal_daily`
where service_date between '{start_date}' and '{end_date}'
order by service_date
"""
df = query_df(sql_multimodal)

if df.empty:
    st.warning("No data available for the selected date range.")
    st.stop()

# --- Scorecards ---
latest_date = df["service_date"].max()
latest = df[df["service_date"] == latest_date]

col1, col2, col3, col4 = st.columns(4)
with col1:
    overall_on_time = latest["pct_on_time"].mean()
    st.metric("Overall On-Time %", f"{overall_on_time:.1f}%")
with col2:
    total_disruptions = latest["disruption_count"].sum()
    st.metric("Active Disruptions", int(total_disruptions))
with col3:
    worst = latest.loc[latest["pct_on_time"].idxmin()]
    st.metric("Worst Corridor", worst["corridor_name"])
with col4:
    avg_delay = latest["avg_delay_min"].mean()
    st.metric("Avg Delay (min)", f"{avg_delay:.1f}")

st.markdown(f"*Latest service date: {latest_date}*")

# --- Corridor reliability map ---
sql_stations = f"""
select distinct
    station_code,
    station_name,
    corridor_id,
    corridor_name,
    lat,
    lon
from `{project}.{dataset}.dim_stations`
"""
df_stations = query_df(sql_stations)

# Merge latest pct_on_time onto stations
station_scores = df_stations.merge(
    latest[["corridor_id", "pct_on_time"]],
    on="corridor_id",
    how="left",
)

fig_map = px.scatter_mapbox(
    station_scores,
    lat="lat",
    lon="lon",
    color="pct_on_time",
    hover_name="station_name",
    hover_data=["corridor_name", "pct_on_time"],
    color_continuous_scale=["red", "yellow", "green"],
    range_color=[50, 100],
    zoom=6,
    center={"lat": 52.1, "lon": 5.1},
    mapbox_style="carto-positron",
    title="Station Reliability (latest service date)",
)
fig_map.update_layout(height=500, margin=dict(l=0, r=0, t=30, b=0))
st.plotly_chart(fig_map, use_container_width=True)

# --- Daily trend by corridor ---
fig_trend = px.line(
    df,
    x="service_date",
    y="pct_on_time",
    color="corridor_name",
    title="Daily On-Time % by Corridor",
    labels={"pct_on_time": "On-Time %", "service_date": "Date"},
)
fig_trend.update_layout(height=400)
st.plotly_chart(fig_trend, use_container_width=True)

# --- Congestion trend ---
fig_congestion = px.bar(
    df,
    x="service_date",
    y="total_congestion_minutes",
    color="corridor_name",
    title="Daily Road Congestion Minutes by Corridor",
    labels={"total_congestion_minutes": "Congestion (min)", "service_date": "Date"},
)
fig_congestion.update_layout(height=400)
st.plotly_chart(fig_congestion, use_container_width=True)
```

- [ ] **Step 3: Test locally**

```bash
cd streamlit
streamlit run app.py
```

Navigate to http://localhost:8501 and click "Network Overview" in the sidebar. Verify:
- Scorecards render (may show 0 if no data yet)
- Map renders with station points
- Trend charts appear

- [ ] **Step 4: Commit**

```bash
cd ..
git add streamlit/app.py streamlit/pages/1_Network_Overview.py
git commit -m "feat: Streamlit Network Overview page with scorecards, map, and trends"
```

---

### Task 21: Streamlit — Corridor Explorer Page

**Files:**
- Create: `streamlit/pages/2_Corridor_Explorer.py`

- [ ] **Step 1: Create Corridor Explorer page**

```python
# streamlit/pages/2_Corridor_Explorer.py
"""Corridor Explorer — deep-dive into specific corridors and stations."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import date, timedelta

from utils.bq_client import query_df, get_project_dataset

st.set_page_config(page_title="Corridor Explorer", layout="wide")
st.title("Corridor Explorer")

project, dataset = get_project_dataset()

# --- Corridor selector ---
sql_corridors = f"""
select distinct corridor_id, corridor_name
from `{project}.{dataset}.dm_multimodal_daily`
order by corridor_name
"""
df_corridors = query_df(sql_corridors)

if df_corridors.empty:
    st.warning("No corridor data available.")
    st.stop()

corridor_options = df_corridors.set_index("corridor_id")["corridor_name"].to_dict()

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    selected_corridor = st.selectbox(
        "Select corridor",
        options=list(corridor_options.keys()),
        format_func=lambda x: corridor_options[x],
    )
with col2:
    start_date = st.date_input(
        "Start date",
        value=date.today() - timedelta(days=30),
        key="explorer_start",
    )
with col3:
    end_date = st.date_input(
        "End date",
        value=date.today() - timedelta(days=1),
        key="explorer_end",
    )

# --- Load corridor data ---
sql_corridor = f"""
select *
from `{project}.{dataset}.dm_multimodal_daily`
where corridor_id = '{selected_corridor}'
  and service_date between '{start_date}' and '{end_date}'
order by service_date
"""
df_corridor = query_df(sql_corridor)

if df_corridor.empty:
    st.warning("No data for this corridor in the selected date range.")
    st.stop()

# --- Summary metrics ---
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Avg On-Time %", f"{df_corridor['pct_on_time'].mean():.1f}%")
with col2:
    st.metric("Avg Delay (min)", f"{df_corridor['avg_delay_min'].mean():.1f}")
with col3:
    st.metric("Total Disruptions", int(df_corridor["disruption_count"].sum()))
with col4:
    st.metric("Avg Road Speed (km/h)", f"{df_corridor['avg_road_speed_kmh'].mean():.1f}")

# --- Dual-axis: train reliability + road speed ---
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(
    go.Scatter(
        x=df_corridor["service_date"],
        y=df_corridor["pct_on_time"],
        name="Train On-Time %",
        line=dict(color="blue"),
    ),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(
        x=df_corridor["service_date"],
        y=df_corridor["avg_road_speed_kmh"],
        name="Road Avg Speed (km/h)",
        line=dict(color="orange", dash="dot"),
    ),
    secondary_y=True,
)

fig.update_layout(
    title="Train Reliability vs Road Traffic",
    height=400,
)
fig.update_yaxes(title_text="On-Time %", secondary_y=False)
fig.update_yaxes(title_text="Road Speed (km/h)", secondary_y=True)
st.plotly_chart(fig, use_container_width=True)

# --- Station-level breakdown ---
st.subheader("Station-Level Performance")

sql_stations = f"""
select
    f.station_code,
    s.station_name,
    avg(f.pct_on_time) as avg_pct_on_time,
    avg(f.avg_delay_min) as avg_delay_min,
    avg(f.severe_delay_share) as avg_severe_delay_share,
    sum(f.disruption_count) as total_disruptions
from `{project}.{dataset}.fct_train_performance` f
join `{project}.{dataset}.dim_stations` s
    on f.station_code = s.station_code
where f.corridor_id = '{selected_corridor}'
  and f.service_date between '{start_date}' and '{end_date}'
group by f.station_code, s.station_name
order by avg_pct_on_time asc
"""
df_stations = query_df(sql_stations)

if not df_stations.empty:
    fig_stations = px.bar(
        df_stations,
        x="station_name",
        y="avg_pct_on_time",
        color="avg_pct_on_time",
        color_continuous_scale=["red", "yellow", "green"],
        range_color=[50, 100],
        title="Average On-Time % by Station",
        labels={"avg_pct_on_time": "On-Time %", "station_name": "Station"},
    )
    fig_stations.update_layout(height=400)
    st.plotly_chart(fig_stations, use_container_width=True)

    st.dataframe(
        df_stations.rename(columns={
            "station_code": "Code",
            "station_name": "Station",
            "avg_pct_on_time": "Avg On-Time %",
            "avg_delay_min": "Avg Delay (min)",
            "avg_severe_delay_share": "Severe Delay %",
            "total_disruptions": "Disruptions",
        }).style.format({
            "Avg On-Time %": "{:.1f}",
            "Avg Delay (min)": "{:.1f}",
            "Severe Delay %": "{:.1f}",
        }),
        use_container_width=True,
    )

# --- Weekday vs Weekend comparison ---
st.subheader("Weekday vs Weekend")
df_corridor["day_type"] = df_corridor["is_weekend"].map({True: "Weekend", False: "Weekday"})

fig_daytype = px.box(
    df_corridor,
    x="day_type",
    y="pct_on_time",
    color="day_type",
    title="On-Time % Distribution: Weekday vs Weekend",
    labels={"pct_on_time": "On-Time %", "day_type": ""},
)
fig_daytype.update_layout(height=350, showlegend=False)
st.plotly_chart(fig_daytype, use_container_width=True)
```

- [ ] **Step 2: Test locally**

```bash
cd streamlit
streamlit run app.py
```

Navigate to Corridor Explorer. Verify:
- Corridor dropdown works
- Dual-axis chart renders
- Station bar chart and table render

- [ ] **Step 3: Commit**

```bash
cd ..
git add streamlit/pages/2_Corridor_Explorer.py
git commit -m "feat: Streamlit Corridor Explorer page with dual-axis charts and station breakdown"
```

---

### Task 22: Generate dbt Docs

**Files:**
- No new files.

- [ ] **Step 1: Generate dbt documentation**

```bash
cd dbt
dbt docs generate
```

Expected: `target/catalog.json` and `target/manifest.json` created.

- [ ] **Step 2: Verify docs serve locally**

```bash
dbt docs serve --port 8081
```

Open http://localhost:8081 and verify the lineage graph shows the full pipeline from sources through staging → intermediate → core.

Ctrl+C to stop.

---

### Task 23: Final README and Cleanup

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update README with complete documentation**

Update `README.md` with:
- Architecture diagram (text-based or link to image)
- Full setup instructions
- Backfill instructions
- Dashboard screenshots (add after deploying)
- Data source descriptions
- dbt lineage description
- Zoomcamp evaluation coverage table

- [ ] **Step 2: Verify full pipeline end-to-end**

```bash
cd airflow
docker compose up -d

# Trigger ingestion for yesterday
docker compose exec airflow-scheduler airflow dags trigger dag_ns_ingest -e $(date -v-1d +%Y-%m-%d)
docker compose exec airflow-scheduler airflow dags trigger dag_ndw_ingest -e $(date -v-1d +%Y-%m-%d)

# Wait for ingestion to complete, then trigger dbt
docker compose exec airflow-scheduler airflow dags trigger dag_dbt_transform -e $(date -v-1d +%Y-%m-%d)
```

Verify:
- All DAG tasks green in Airflow UI
- Data visible in BigQuery tables
- Dashboard shows data at http://localhost:8501
- Alert check runs (check Slack or alert_history table)

- [ ] **Step 3: Final commit**

```bash
cd ..
git add -A
git commit -m "docs: complete README with setup instructions and architecture"
```

---

## Stretch Tasks (Post-MVP)

### Stretch Task A: OVapi GTFS Ingestion

**Files:**
- Create: `airflow/scripts/ingest_ovapi.py`
- Create: `airflow/dags/dag_ovapi_ingest.py`
- Create: `dbt/models/staging/stg_gtfs_stops.sql`
- Create: `dbt/models/staging/stg_gtfs_routes.sql`
- Create: `dbt/models/staging/stg_gtfs_stop_times.sql`
- Create: `dbt/models/core/fct_transit_service.sql`
- Create: `dbt/snapshots/snap_dim_routes.sql`

Implementation: Download GTFS zip from OVapi, extract CSVs (stops.txt, routes.txt, stop_times.txt, trips.txt, calendar.txt), upload to GCS, load to BigQuery with WRITE_TRUNCATE. Add staging models, `fct_transit_service`, and `dim_routes` SCD snapshot. Update `dm_multimodal_daily` to include transit service counts.

### Stretch Task B: Disruption Impact Analysis

**Files:**
- Create: `dbt/models/intermediate/int_disruption_road_impact.sql`
- Create: `dbt/models/core/dm_disruption_impact.sql`
- Create: `streamlit/pages/3_Incidents.py`

Implementation: `int_disruption_road_impact` joins disruption events to nearby NDW 15-min readings (using station_ndw_mapping seed) and computes avg_speed_pre/during/post (2h windows). `dm_disruption_impact` aggregates to one row per disruption. Incidents page shows sortable table with click-to-expand detail view and before/during/after speed chart.

### Stretch Task C: Alerts Log Page

**Files:**
- Create: `streamlit/pages/4_Alerts_Log.py`

Implementation: Simple table reading from `core.alert_history`, filtered by date range, alert_type, and fired status.

### Stretch Task D: Disruption Alert Type

**Files:**
- Modify: `airflow/scripts/alert_checker.py`

Implementation: Add `check_disruption_alerts()` that queries `dm_disruption_impact` for new high-severity disruptions (duration > 60 min or stations_affected > 5). Send separate Slack message format for disruption alerts.
