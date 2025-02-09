terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

resource "google_storage_bucket" "demo-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "external_table" {
  dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
  table_id = "yellow_tripdata_external"
  external_data_configuration {
    autodetect = true
    source_format = "PARQUET"
    source_uris = ["gs://${var.gcs_bucket_name}/*.parquet"]
  }
}

resource "google_bigquery_table" "regular_table" {
  dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
  table_id = "yellow_tripdata_regular"
}

resource "google_bigquery_job" "load_parquet" {
  job_id     = "load_parquet_to_bigquery_job_1"
  location = var.location

  load {
    source_uris = ["gs://${var.gcs_bucket_name}/*.parquet"]
    destination_table {
      project_id = var.project_id
      dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
      table_id   = google_bigquery_table.regular_table.table_id
    }

    source_format    = "PARQUET"
    write_disposition = "WRITE_TRUNCATE"

    autodetect = true
  }
}

