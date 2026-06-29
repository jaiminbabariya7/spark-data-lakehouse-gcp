# Terraform: GCP infrastructure for Spark Data Lakehouse

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "tfstate-lakehouse"
    prefix = "spark-lakehouse"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" { type = string }
variable "region"     { type = string; default = "us-central1" }
variable "env"        { type = string; default = "prod" }

# GCS Lakehouse bucket (Bronze / Silver / Gold zones)
resource "google_storage_bucket" "lakehouse" {
  name          = "${var.project_id}-lakehouse-${var.env}"
  location      = var.region
  force_destroy = false

  lifecycle_rule {
    condition { age = 90 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }
  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "COLDLINE" }
  }

  versioning { enabled = true }
}

# BigQuery Gold dataset
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "gold"
  location    = var.region
  description = "Gold layer: business-ready aggregates from the Spark Lakehouse"
}

# Dataproc cluster (ephemeral — created per DAG run via Airflow)
resource "google_dataproc_cluster" "lakehouse" {
  name   = "lakehouse-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config   { boot_disk_size_gb = 100 }
    }
    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-4"
      disk_config   { boot_disk_size_gb = 100 }
    }
    software_config {
      image_version = "2.1-debian11"
      override_properties = {
        "spark:spark.sql.adaptive.enabled"              = "true"
        "spark:spark.sql.adaptive.coalescePartitions.enabled" = "true"
      }
    }
    gce_cluster_config {
      service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }
}

# Service account for Dataproc / Spark jobs
resource "google_service_account" "lakehouse_sa" {
  account_id   = "lakehouse-sa"
  display_name = "Spark Lakehouse Service Account"
}

resource "google_project_iam_member" "lakehouse_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

resource "google_project_iam_member" "lakehouse_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

# Outputs
output "lakehouse_bucket" { value = google_storage_bucket.lakehouse.name }
output "bq_gold_dataset"  { value = google_bigquery_dataset.gold.dataset_id }