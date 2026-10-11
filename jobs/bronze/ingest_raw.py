"""
Bronze Layer: Raw Ingestion Job

Reads one day's raw CSV files from the GCS landing zone
(gs://<bucket>/landing/<table>/<batch_date>/), enforces the table schema,
and writes partitioned Parquet to the Bronze layer with minimal
transformation: schema enforcement and audit columns only.

  * Malformed rows (values that don't fit the schema) are written to
    gs://<bucket>/dead_letter/<table>/_batch_date=<date>/ with the raw line.
  * Only the run's own _batch_date partition is replaced (dynamic partition
    overwrite), so Bronze keeps full history and reruns are idempotent.

Usage (Dataproc):
    gcloud dataproc jobs submit pyspark jobs/bronze/ingest_raw.py \
        --cluster=lakehouse-cluster --region=us-central1 \
        -- --table=orders --batch_date=2024-01-15 --bucket=my-lakehouse
"""
from __future__ import annotations
import argparse, logging, os
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bronze.ingest")

CORRUPT_COL = "_corrupt_record"

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
        # overwrite only the partitions present in the written data
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def read_landing(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """Read CSV with the table schema; rows that don't fit keep their raw line
    in _corrupt_record instead of failing the job."""
    read_schema = StructType(schema.fields + [StructField(CORRUPT_COL, StringType(), True)])
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(read_schema)
        .csv(path)
        .withColumn("_source_file", F.input_file_name())
        .cache()        # required before filtering on the corrupt-record column
    )


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return (valid rows, malformed rows with the raw line and source file)."""
    valid = df.filter(F.col(CORRUPT_COL).isNull()).drop(CORRUPT_COL)
    bad = (
        df.filter(F.col(CORRUPT_COL).isNotNull())
        .select(F.col(CORRUPT_COL).alias("raw_line"), "_source_file")
    )
    return valid, bad


def add_audit_columns(df: DataFrame, batch_date: str) -> DataFrame:
    """Append Bronze audit metadata (_source_file is set at read time)."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_date",  F.to_date(F.lit(batch_date)))
    )


def write_partitioned(df: DataFrame, path: str) -> None:
    """Write Parquet partitioned by _batch_date, replacing only those partitions."""
    df.write.mode("overwrite").partitionBy("_batch_date").parquet(path)


def ingest_table(spark: SparkSession, table: str, batch_date: str, bucket: str) -> int:
    landing_path = f"gs://{bucket}/landing/{table}/{batch_date}/"
    bronze_path  = f"gs://{bucket}/bronze/{table}/"
    dlq_path     = f"gs://{bucket}/dead_letter/{table}/"

    logger.info("Reading %s from %s", table, landing_path)
    raw = read_landing(spark, landing_path, SCHEMAS[table])
    valid, bad = split_valid(raw)

    bad_count = bad.count()
    if bad_count:
        logger.warning("%d malformed %s rows -> %s", bad_count, table, dlq_path)
        write_partitioned(add_audit_columns(bad, batch_date), dlq_path)

    valid = add_audit_columns(valid, batch_date)
    record_count = valid.count()
    logger.info("Writing %d records to Bronze layer: %s", record_count, bronze_path)
    write_partitioned(valid, bronze_path)
    raw.unpersist()
    return record_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, choices=list(SCHEMAS))
    parser.add_argument("--batch_date", default=date.today().isoformat(),
                        help="Landing folder / partition to load (YYYY-MM-DD)")
    parser.add_argument("--bucket", default=os.getenv("GCS_LAKEHOUSE_BUCKET"))
    args = parser.parse_args()
    if not args.bucket:
        parser.error("--bucket or GCS_LAKEHOUSE_BUCKET is required")

    spark = get_spark(f"bronze_ingest_{args.table}")
    count = ingest_table(spark, args.table, args.batch_date, args.bucket)
    logger.info("Bronze ingestion complete: %d records for %s", count, args.table)
    spark.stop()
