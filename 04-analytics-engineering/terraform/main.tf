terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

resource "google_storage_bucket" "taxi-bucket" {
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

resource "google_bigquery_dataset" "taxi_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "yellow_external_table" {
  dataset_id = google_bigquery_dataset.taxi_dataset.dataset_id
  table_id   = "yellow_tripdata_external"
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.gcs_bucket_name}/yellow/*.parquet"]
  }
  deletion_protection = false
}

resource "google_bigquery_table" "green_external_table" {
  dataset_id = google_bigquery_dataset.taxi_dataset.dataset_id
  table_id   = "green_tripdata_external"
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.gcs_bucket_name}/green/*.parquet"]
  }
  deletion_protection = false
}

resource "google_bigquery_table" "fhv_external_table" {
  dataset_id = google_bigquery_dataset.taxi_dataset.dataset_id
  table_id   = "fhv_tripdata_external"
  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.gcs_bucket_name}/fhv/*.parquet"]
  }
  deletion_protection = false
}
