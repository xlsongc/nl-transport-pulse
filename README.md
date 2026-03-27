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
