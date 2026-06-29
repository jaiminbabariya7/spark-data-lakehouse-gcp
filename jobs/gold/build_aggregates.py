"""
Gold Layer: Build Aggregates Job

Reads clean Silver Parquet, computes business-level aggregates,
and writes them to both GCS (Gold Parquet) and BigQuery for BI consumption.

Aggregations produced:
  - daily_revenue_by_channel: revenue, orders, avg order value per day/channel
  - customer_metrics: lifetime value, order frequency, recency, LTV tier
  - product_performance: units sold, revenue, gross profit, margin per product
"""
from __future__ import annotations
import argparse, logging, os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold.aggregates")

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET  = os.environ["GCS_LAKEHOUSE_BUCKET"]
BQ_DATASET  = os.getenv("BQ_GOLD_DATASET", "gold")


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("temporaryGcsBucket", GCS_BUCKET)
        .getOrCreate()
    )


def write_gold(df: DataFrame, name: str) -> None:
    gcs_path = f"gs://{GCS_BUCKET}/gold/{name}/"
    bq_table = f"{PROJECT_ID}.{BQ_DATASET}.{name}"

    logger.info("Writing Gold to GCS: %s", gcs_path)
    df.write.mode("overwrite").parquet(gcs_path)

    logger.info("Writing Gold to BigQuery: %s", bq_table)
    (
        df.write
        .format("bigquery")
        .option("table", bq_table)
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save()
    )


def build_daily_revenue(spark: SparkSession) -> None:
    orders = spark.read.parquet(f"gs://{GCS_BUCKET}/silver/orders/")
    agg = (
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
        .orderBy("order_date", "channel")
    )
    write_gold(agg, "daily_revenue_by_channel")


def build_customer_metrics(spark: SparkSession) -> None:
    orders    = spark.read.parquet(f"gs://{GCS_BUCKET}/silver/orders/")
    customers = spark.read.parquet(f"gs://{GCS_BUCKET}/silver/customers/")

    rfm = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("customer_id")
        .agg(
            F.datediff(F.current_date(), F.max("order_date")).alias("recency_days"),
            F.countDistinct("order_date").alias("purchase_frequency"),
            F.round(F.sum("net_revenue"), 2).alias("lifetime_value_usd"),
            F.round(F.avg("net_revenue"), 2).alias("avg_order_value_usd"),
            F.count("order_id").alias("total_orders"),
            F.min("order_date").alias("first_purchase_date"),
            F.max("order_date").alias("last_purchase_date"),
        )
    )

    enriched = (
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
        .join(customers.select("customer_id","first_name","last_name","email","country"), "customer_id", "left")
    )
    write_gold(enriched, "customer_metrics")


def build_product_performance(spark: SparkSession) -> None:
    orders   = spark.read.parquet(f"gs://{GCS_BUCKET}/silver/orders/")
    products = spark.read.parquet(f"gs://{GCS_BUCKET}/silver/products/")

    perf = (
        orders
        .filter(F.col("status") != "cancelled")
        .groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("net_revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_buyers"),
        )
        .join(products, "product_id", "left")
        .withColumn(
            "gross_profit",
            F.round(F.col("total_revenue") - F.col("unit_cost") * F.col("units_sold"), 2),
        )
    )
    write_gold(perf, "product_performance")


JOBS = {
    "daily_revenue":     build_daily_revenue,
    "customer_metrics":  build_customer_metrics,
    "product_performance": build_product_performance,
}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job", required=True, choices=["all"] + list(JOBS))
    args = parser.parse_args()

    spark = get_spark(f"gold_{args.job}")
    targets = list(JOBS.items()) if args.job == "all" else [(args.job, JOBS[args.job])]
    for name, fn in targets:
        logger.info("Running Gold job: %s", name)
        fn(spark)
    spark.stop()