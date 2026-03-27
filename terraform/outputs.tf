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
