# Terraform: GCP infrastructure for the Spark Data Lakehouse
#   - GCS bucket for the landing / Bronze / Silver / Gold / dead-letter zones and job code
#   - BigQuery Gold dataset
#   - Service account used by the ephemeral Dataproc clusters
# The Dataproc cluster itself is NOT managed here: Airflow creates it at the
# start of each DAG run and deletes it at the end.

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

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "env" {
  type    = string
  default = "prod"
}

# GCS lakehouse bucket
resource "google_storage_bucket" "lakehouse" {
  name                        = "${var.project_id}-lakehouse-${var.env}"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  versioning {
    enabled = true
  }
}

# BigQuery Gold dataset
resource "google_bigquery_dataset" "gold" {
  dataset_id  = "gold"
  location    = var.region
  description = "Gold layer: business-ready aggregates from the Spark Lakehouse"
}

# Service account for the ephemeral Dataproc clusters / Spark jobs
resource "google_service_account" "lakehouse_sa" {
  account_id   = "lakehouse-sa"
  display_name = "Spark Lakehouse Service Account"
}

resource "google_project_iam_member" "lakehouse_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.lakehouse_sa.email}"
}

output "lakehouse_bucket" {
  value = google_storage_bucket.lakehouse.name
}

output "bq_gold_dataset" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "dataproc_service_account" {
  value = google_service_account.lakehouse_sa.email
}
