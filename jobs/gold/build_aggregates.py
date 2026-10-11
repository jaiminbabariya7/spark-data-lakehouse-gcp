"""
Gold Layer: Build Aggregates Job

Reads clean Silver Parquet, computes business-level aggregates,
and writes them to both GCS (Gold Parquet) and BigQuery for BI consumption.

Aggregations produced:
  - daily_revenue_by_channel: revenue, orders, avg order value per day/channel
  - customer_metrics: lifetime value, order frequency, recency, LTV tier
  - product_performance: units sold, revenue, gross profit, margin per product

The aggregation logic is in pure DataFrame functions (daily_revenue,
customer_metrics, product_performance) that the unit tests call directly.
"""
from __future__ import annotations
import argparse, logging, os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold.aggregates")


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ── Aggregations (pure) ───────────────────────────────────────────────────────

def daily_revenue(orders: DataFrame) -> DataFrame:
    return (
        orders
        .filter(F.col("status").isin(["completed", "shipped"]))
        .groupBy("order_date", "channel")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.round(F.avg("net_revenue"), 2).alias("avg_order_value"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )


def customer_metrics(orders: DataFrame, customers: DataFrame,
                     as_of: str | None = None) -> DataFrame:
    """RFM profile per customer; recency is measured from `as_of` (default today)."""
    ref_date = F.to_date(F.lit(as_of)) if as_of else F.current_date()
    rfm = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.datediff(ref_date, F.max("order_date")).alias("recency_days"),
            F.countDistinct("order_date").alias("purchase_frequency"),
            F.round(F.sum("net_revenue"), 2).alias("lifetime_value_usd"),
            F.round(F.avg("net_revenue"), 2).alias("avg_order_value_usd"),
            F.count("order_id").alias("total_orders"),
            F.min("order_date").alias("first_purchase_date"),
            F.max("order_date").alias("last_purchase_date"),
        )
    )
    return (
        rfm
        .withColumn(
            "ltv_tier",
            F.when(F.col("lifetime_value_usd") >= 5000, "platinum")
             .when(F.col("lifetime_value_usd") >= 2000, "gold")
             .when(F.col("lifetime_value_usd") >= 500,  "silver")
             .otherwise("bronze"),
        )
        .withColumn(
            "churn_risk",
            F.when((F.col("recency_days") > 90) & (F.col("purchase_frequency") <= 2), "high")
             .when((F.col("recency_days") > 60) & (F.col("purchase_frequency") <= 4), "medium")
             .otherwise("low"),
        )
        .join(customers.select("customer_id", "first_name", "last_name", "email", "country"),
              "customer_id", "left")
    )


def product_performance(orders: DataFrame, products: DataFrame) -> DataFrame:
    return (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
        .join(products.select("product_id", "product_name", "category",
                              "unit_price", "unit_cost", "margin_pct"),
              "product_id", "left")
        .withColumn(
            "gross_profit",
            F.round(F.col("total_revenue") - F.col("unit_cost") * F.col("units_sold"), 2),
        )
    )


# ── I/O ───────────────────────────────────────────────────────────────────────

class GoldWriter:
    def __init__(self, spark: SparkSession, bucket: str, project: str, bq_dataset: str):
        self.spark, self.bucket, self.project, self.bq_dataset = spark, bucket, project, bq_dataset

    def silver(self, table: str) -> DataFrame:
        return self.spark.read.parquet(f"gs://{self.bucket}/silver/{table}/")

    def write(self, df: DataFrame, name: str) -> None:
        gcs_path = f"gs://{self.bucket}/gold/{name}/"
        bq_table = f"{self.project}.{self.bq_dataset}.{name}"
        logger.info("Writing Gold to GCS: %s", gcs_path)
        df.write.mode("overwrite").parquet(gcs_path)
        logger.info("Writing Gold to BigQuery: %s", bq_table)
        (
            df.write
            .format("bigquery")             # Spark-BigQuery connector (Dataproc built-in)
            .option("table", bq_table)
            .option("writeMethod", "direct")
            .mode("overwrite")
            .save()
        )


def build_daily_revenue(w: GoldWriter) -> None:
    w.write(daily_revenue(w.silver("orders")), "daily_revenue_by_channel")


def build_customer_metrics(w: GoldWriter) -> None:
    w.write(customer_metrics(w.silver("orders"), w.silver("customers")), "customer_metrics")


def build_product_performance(w: GoldWriter) -> None:
    w.write(product_performance(w.silver("orders"), w.silver("products")), "product_performance")


JOBS = {
    "daily_revenue":       build_daily_revenue,
    "customer_metrics":    build_customer_metrics,
    "product_performance": build_product_performance,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=["all"] + list(JOBS))
    parser.add_argument("--bucket",     default=os.getenv("GCS_LAKEHOUSE_BUCKET"))
    parser.add_argument("--project",    default=os.getenv("GCP_PROJECT_ID"))
    parser.add_argument("--bq_dataset", default=os.getenv("BQ_GOLD_DATASET", "gold"))
    args = parser.parse_args()
    if not (args.bucket and args.project):
        parser.error("--bucket/--project (or GCS_LAKEHOUSE_BUCKET/GCP_PROJECT_ID) are required")

    spark = get_spark(f"gold_{args.job}")
    writer = GoldWriter(spark, args.bucket, args.project, args.bq_dataset)
    targets = list(JOBS.items()) if args.job == "all" else [(args.job, JOBS[args.job])]
    for name, fn in targets:
        logger.info("Running Gold job: %s", name)
        fn(writer)
    spark.stop()
