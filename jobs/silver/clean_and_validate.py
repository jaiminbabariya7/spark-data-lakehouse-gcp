"""
Silver Layer: Clean and Validate Job

Reads raw Parquet from the Bronze layer, applies data quality checks,
cleanses and normalises fields, deduplicates records, and writes
clean Parquet to the Silver layer.

Data quality rules enforced:
  - Drop records missing primary key
  - Drop records with quantity <= 0 or unit_price <= 0
  - Deduplicate on primary key (keep latest _ingested_at)
  - Normalise string casing and trim whitespace
  - Cast date strings to proper date types
  - Compute net_revenue = quantity * unit_price * (1 - discount_pct)
"""
from __future__ import annotations
import argparse, logging, os
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver.clean")

GCS_BUCKET = os.environ["GCS_LAKEHOUSE_BUCKET"]


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def deduplicate(df: DataFrame, pk: str) -> DataFrame:
    """Keep the most recently ingested record per primary key."""
    w = Window.partitionBy(pk).orderBy(F.desc("_ingested_at"))
    return (
        df
        .withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df
        # Drop invalid records
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        # Normalise
        .withColumn("channel",      F.upper(F.trim(F.col("channel"))))
        .withColumn("status",       F.lower(F.trim(F.col("status"))))
        .withColumn("order_date",   F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("discount_pct", F.coalesce(F.col("discount_pct"), F.lit(0.0)))
        # Derived field
        .withColumn(
            "net_revenue",
            F.round(
                F.col("unit_price") * F.col("quantity") * (1 - F.col("discount_pct")),
                2,
            ),
        )
    )


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("email").isNotNull())
        .withColumn("first_name",        F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",         F.initcap(F.trim(F.col("last_name"))))
        .withColumn("email",             F.lower(F.trim(F.col("email"))))
        .withColumn("country",           F.initcap(F.trim(F.col("country"))))
        .withColumn("registration_date", F.to_date(F.col("registration_date"), "yyyy-MM-dd"))
        .withColumn("is_active",         F.coalesce(F.col("is_active"), F.lit(True)))
    )


def clean_products(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("product_id").isNotNull())
        .filter(F.col("unit_price") > 0)
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category",     F.initcap(F.trim(F.col("category"))))
        .withColumn(
            "margin_pct",
            F.round(
                (F.col("unit_price") - F.col("unit_cost")) / F.col("unit_price") * 100,
                2,
            ),
        )
    )


CLEANERS = {
    "orders":    (clean_orders,    "order_id"),
    "customers": (clean_customers, "customer_id"),
    "products":  (clean_products,  "product_id"),
}


def process(spark: SparkSession, table: str) -> int:
    bronze_path = f"gs://{GCS_BUCKET}/bronze/{table}/"
    silver_path = f"gs://{GCS_BUCKET}/silver/{table}/"
    cleaner_fn, pk = CLEANERS[table]

    logger.info("Reading Bronze: %s", bronze_path)
    raw = spark.read.parquet(bronze_path)

    clean  = cleaner_fn(raw)
    deduped = deduplicate(clean, pk)

    count = deduped.count()
    logger.info("Writing %d clean records to Silver: %s", count, silver_path)

    (
        deduped.write
        .mode("overwrite")
        .partitionBy("_batch_date")
        .parquet(silver_path)
    )
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(CLEANERS))
    args = parser.parse_args()

    spark = get_spark(f"silver_clean_{args.table}")
    count = process(spark, args.table)
    logger.info("Silver complete: %d records for %s", count, args.table)
    spark.stop()