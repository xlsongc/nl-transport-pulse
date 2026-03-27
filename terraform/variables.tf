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
