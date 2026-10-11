# Spark Data Lakehouse on GCP

![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Dataproc](https://img.shields.io/badge/Dataproc-GCP-blue?logo=google-cloud)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-blue?logo=google-cloud)
![Airflow](https://img.shields.io/badge/Airflow-2.8-red?logo=apache-airflow)
![Terraform](https://img.shields.io/badge/Terraform-1.6-purple?logo=terraform)

Data lakehouse built on Google Cloud using the Medallion Architecture (Bronze → Silver → Gold). Raw e-commerce data lands in GCS, flows through PySpark cleaning and validation jobs running on Dataproc, and surfaces as business-ready aggregates in BigQuery — all orchestrated by Apache Airflow with ephemeral Dataproc clusters to minimise cost.

## Architecture

![Architecture](snapshots/architecture.svg)

## Medallion Architecture

| Layer | Path | Format | Purpose |
|---|---|---|---|
| **Landing** | `gs://bucket/landing/<table>/<YYYY-MM-DD>/` | CSV | One folder per daily drop |
| **Bronze** | `gs://bucket/bronze/` | Parquet (Snappy), partitioned by `_batch_date` | Raw data as-is + audit columns, full history |
| **Dead letter** | `gs://bucket/dead_letter/` | Parquet, partitioned by `_batch_date` | Rows that failed the schema, with the raw line and source file |
| **Silver** | `gs://bucket/silver/` | Parquet (Snappy) | Cleaned, deduped current state (rebuilt each run) |
| **Gold** | `gs://bucket/gold/` + BigQuery | Parquet + BQ tables | Business aggregates, BI-ready |

## Project Structure

```
spark-data-lakehouse-gcp/
├── jobs/
│   ├── bronze/
│   │   └── ingest_raw.py           # Schema-enforced ingestion → Bronze Parquet
│   ├── silver/
│   │   └── clean_and_validate.py   # Dedup + cleaning + net_revenue
│   └── gold/
│       └── build_aggregates.py     # 3 Gold aggregations → GCS + BigQuery
├── airflow/
│   └── dags/lakehouse_pipeline_dag.py
├── terraform/
│   └── main.tf                     # GCS bucket, BQ dataset, Dataproc service account + IAM
├── docker/
│   └── docker-compose.yml          # Local Spark cluster (master + 2 workers + history server)
├── tests/                          # pytest on local Spark, calling the job functions directly
│   ├── conftest.py
│   ├── test_bronze.py
│   ├── test_silver.py
│   └── test_gold.py
├── snapshots/
│   └── architecture.svg
├── .env.example
└── requirements.txt
```

## PySpark Jobs

**Bronze — `ingest_raw.py`**
Reads one day's CSV files from `landing/<table>/<batch_date>/` with a strict schema. Rows whose values don't fit the schema are captured with Spark's corrupt-record column and written to the dead-letter path with the raw line and source file, instead of becoming silent nulls. Valid rows get audit columns (`_ingested_at`, `_source_file`, `_batch_date`) and are written as Parquet partitioned by `_batch_date` with **dynamic partition overwrite**, so a run replaces only its own day: Bronze keeps full history and reruns or backfills are idempotent.

**Silver — `clean_and_validate.py`**
Reads the full Bronze history, applies data quality rules (drops records missing primary keys or with invalid quantities or prices), deduplicates on primary key keeping the record from the latest batch (so a later correction wins), normalises string casing, casts date fields, fills nulls, and computes `net_revenue` and `margin_pct`. Silver is rebuilt as a full snapshot of current state each run.

**Gold — `build_aggregates.py`**
Produces three business aggregations, each writing to GCS Gold Parquet and directly to BigQuery via the Spark–BigQuery connector:
- `daily_revenue_by_channel` — orders, revenue, AOV, unique customers per day/channel
- `customer_metrics` — full RFM profile with LTV tier and churn risk per customer
- `product_performance` — units sold, revenue, gross profit, unique buyers per product

## Airflow DAG

`spark_lakehouse_pipeline` runs daily at 06:00 UTC:
```
create_cluster
    ├── bronze_orders ──► silver_orders ──►
    ├── bronze_customers ► silver_customers ►──► gold_daily_revenue
    └── bronze_products ─► silver_products ──►──► gold_customer_metrics
                                                 └── gold_product_perf
                                                         └── delete_cluster
```
The Dataproc cluster is created at the start of each run and deleted on completion — ephemeral by design.

## Quick Start

```bash
# 1. Clone + install
git clone https://github.com/jaiminbabariya7/spark-data-lakehouse-gcp.git
cd spark-data-lakehouse-gcp
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env  # Fill in GCP_PROJECT_ID, GCS_LAKEHOUSE_BUCKET
set -a; . ./.env; set +a

# 3. Provision infrastructure (bucket, BigQuery dataset, Dataproc service account)
(cd terraform && terraform init && terraform apply -var="project_id=$GCP_PROJECT_ID")

# 4. Upload the job code the DAG runs, and drop a day's files into landing/
make upload-jobs
gsutil cp orders.csv gs://$GCS_LAKEHOUSE_BUCKET/landing/orders/2024-01-15/

# 5. Run tests (local Spark; needs Java 17)
pytest tests/ -v --cov=jobs

# 6. Optional: local Spark cluster (master + 2 workers + history server)
docker compose -f docker/docker-compose.yml up -d

# 7. Submit one job to Dataproc manually
gcloud dataproc jobs submit pyspark jobs/bronze/ingest_raw.py \
    --cluster=lakehouse-cluster --region=us-central1 \
    -- --table=orders --batch_date=2024-01-15 --bucket=$GCS_LAKEHOUSE_BUCKET
```

## Tech Stack

| Component | Technology |
|---|---|
| Compute | Google Cloud Dataproc 2.2 (Spark 3.5) |
| Object Storage | Google Cloud Storage (Bronze / Silver / Gold) |
| Data Warehouse | Google BigQuery (Gold layer) |
| Orchestration | Apache Airflow 2.8 + Dataproc provider |
| Infrastructure | Terraform 1.6 |
| Local Dev | Docker Compose (Bitnami Spark) |
| Language | Python 3.11 |
| Testing | pytest + pyspark local mode |
| Optimisations | Adaptive Query Execution, Parquet Snappy, dynamic partition overwrite |