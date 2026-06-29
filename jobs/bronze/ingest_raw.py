"""
Bronze Layer: Raw Ingestion Job

Reads raw e-commerce event files (CSV/JSON) from the landing zone in GCS
and writes them as partitioned Parquet to the Bronze layer with minimal
transformation — schema enforcement and audit columns only.

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark jobs/bronze/ingest_raw.py \
        --cluster=lakehouse-cluster --region=us-central1 \
        -- --env=prod --table=orders
"""
from __future__ import annotations
import argparse, logging, os
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze.ingest")

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET  = os.environ["GCS_LAKEHOUSE_BUCKET"]

SCHEMAS: dict[str, StructType] = {
    "orders": StructType([
        StructField("order_id",      StringType(),  False),
        StructField("customer_id",   StringType(),  True),
        StructField("product_id",    StringType(),  True),
        StructField("order_date",    StringType(),  True),
        StructField("quantity",      IntegerType(), True),
        StructField("unit_price",    DoubleType(),  True),
        StructField("discount_pct",  DoubleType(),  True),
        StructField("channel",       StringType(),  True),
        StructField("status",        StringType(),  True),
    ]),
    "customers": StructType([
        StructField("customer_id",       StringType(),  False),
        StructField("first_name",        StringType(),  True),
        StructField("last_name",         StringType(),  True),
        StructField("email",             StringType(),  True),
        StructField("country",           StringType(),  True),
        StructField("registration_date", StringType(),  True),
        StructField("is_active",         BooleanType(), True),
    ]),
    "products": StructType([
        StructField("product_id",   StringType(),  False),
        StructField("product_name", StringType(),  True),
        StructField("category",     StringType(),  True),
        StructField("unit_price",   DoubleType(),  True),
        StructField("unit_cost",    DoubleType(),  True),
    ]),
}


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def add_audit_columns(df: DataFrame, source_path: str) -> DataFrame:
    """Append Bronze audit metadata to every record."""
    return (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(source_path))
        .withColumn("_batch_date",   F.to_date(F.current_timestamp()))
    )


def ingest_table(spark: SparkSession, table: str, env: str) -> int:
    landing_path = f"gs://{GCS_BUCKET}/landing/{table}/"
    bronze_path  = f"gs://{GCS_BUCKET}/bronze/{table}/"
    schema       = SCHEMAS[table]

    logger.info("Reading %s from %s", table, landing_path)
    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(landing_path)
    )

    df = add_audit_columns(df, landing_path)

    record_count = df.count()
    logger.info("Writing %d records to Bronze layer: %s", record_count, bronze_path)

    (
        df.write
        .mode("overwrite")
        .partitionBy("_batch_date")
        .parquet(bronze_path)
    )
    return record_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(SCHEMAS))
    parser.add_argument("--env",   default="prod")
    args = parser.parse_args()

    spark = get_spark(f"bronze_ingest_{args.table}")
    count = ingest_table(spark, args.table, args.env)
    logger.info("Bronze ingestion complete: %d records for %s", count, args.table)
    spark.stop()