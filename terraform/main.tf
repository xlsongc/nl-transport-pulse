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
