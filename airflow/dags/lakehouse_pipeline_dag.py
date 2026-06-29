"""
Airflow DAG: Spark Data Lakehouse Pipeline

Orchestrates the full Medallion Architecture pipeline on Google Cloud Dataproc:
  Bronze  -> Silver -> Gold (GCS + BigQuery)

Schedule: Daily at 06:00 UTC.
"""
from __future__ import annotations
import os
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitPySparkJobOperator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
REGION      = os.getenv("DATAPROC_REGION", "us-central1")
CLUSTER     = os.getenv("DATAPROC_CLUSTER", "lakehouse-cluster")
GCS_BUCKET  = os.environ["GCS_LAKEHOUSE_BUCKET"]
JOBS_BASE   = f"gs://{GCS_BUCKET}/code/jobs"

CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-4", "disk_config": {"boot_disk_size_gb": 100}},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-4", "disk_config": {"boot_disk_size_gb": 100}},
    "software_config": {"image_version": "2.1-debian11", "properties": {"spark:spark.sql.adaptive.enabled": "true"}},
}

DEFAULT_ARGS = {
    "owner": "jaimin.babariya",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": True,
}


def pyspark_job(name: str, script: str, args: list[str]) -> DataprocSubmitPySparkJobOperator:
    return DataprocSubmitPySparkJobOperator(
        task_id=name,
        main=f"{JOBS_BASE}/{script}",
        arguments=args,
        cluster_name=CLUSTER,
        region=REGION,
        project_id=PROJECT_ID,
    )


with DAG(
    dag_id="spark_lakehouse_pipeline",
    description="Medallion architecture: Bronze -> Silver -> Gold on Dataproc",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "lakehouse", "dataproc", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER,
        region=REGION,
        trigger_rule="all_done",
    )

    # Bronze jobs (parallel per table)
    bronze_orders    = pyspark_job("bronze_orders",    "bronze/ingest_raw.py",    ["--table", "orders"])
    bronze_customers = pyspark_job("bronze_customers", "bronze/ingest_raw.py",    ["--table", "customers"])
    bronze_products  = pyspark_job("bronze_products",  "bronze/ingest_raw.py",    ["--table", "products"])

    # Silver jobs
    silver_orders    = pyspark_job("silver_orders",    "silver/clean_and_validate.py", ["--table", "orders"])
    silver_customers = pyspark_job("silver_customers", "silver/clean_and_validate.py", ["--table", "customers"])
    silver_products  = pyspark_job("silver_products",  "silver/clean_and_validate.py", ["--table", "products"])

    # Gold jobs
    gold_revenue  = pyspark_job("gold_daily_revenue",     "gold/build_aggregates.py", ["--job", "daily_revenue"])
    gold_customer = pyspark_job("gold_customer_metrics",  "gold/build_aggregates.py", ["--job", "customer_metrics"])
    gold_product  = pyspark_job("gold_product_perf",      "gold/build_aggregates.py", ["--job", "product_performance"])

    # Pipeline topology
    start >> create_cluster
    create_cluster >> [bronze_orders, bronze_customers, bronze_products]
    bronze_orders    >> silver_orders
    bronze_customers >> silver_customers
    bronze_products  >> silver_products
    [silver_orders, silver_customers, silver_products] >> [gold_revenue, gold_customer, gold_product]
    [gold_revenue, gold_customer, gold_product] >> delete_cluster >> end