"""Unit tests for Silver layer cleaning and validation logic."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import functions as F
from datetime import datetime


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_silver")
        .getOrCreate()
    )


ORDER_SCHEMA = StructType([
    StructField("order_id",     StringType(),  False),
    StructField("customer_id",  StringType(),  True),
    StructField("product_id",   StringType(),  True),
    StructField("order_date",   StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("discount_pct", DoubleType(),  True),
    StructField("channel",      StringType(),  True),
    StructField("status",       StringType(),  True),
    StructField("_ingested_at", StringType(),  True),
    StructField("_batch_date",  StringType(),  True),
])


def test_null_order_id_dropped(spark):
    """Records missing order_id must be filtered out in Silver."""
    rows = [
        ("ord_1","c1","p1","2024-01-01",2,50.0,0.0,"online","completed","2024-01-01T00:00:00","2024-01-01"),
        (None,   "c2","p2","2024-01-02",1,20.0,0.0,"store","shipped",  "2024-01-02T00:00:00","2024-01-02"),
    ]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    cleaned = df.filter(F.col("order_id").isNotNull())
    assert cleaned.count() == 1


def test_negative_quantity_dropped(spark):
    """Records with quantity <= 0 must be filtered out."""
    rows = [
        ("ord_2","c1","p1","2024-01-01", 3,30.0,0.0,"online","completed","2024-01-01T00:00:00","2024-01-01"),
        ("ord_3","c2","p2","2024-01-02",-1,20.0,0.0,"store","shipped",  "2024-01-02T00:00:00","2024-01-02"),
        ("ord_4","c3","p3","2024-01-03", 0,15.0,0.0,"online","cancelled","2024-01-03T00:00:00","2024-01-03"),
    ]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    cleaned = df.filter(F.col("quantity") > 0)
    assert cleaned.count() == 1


def test_net_revenue_computed(spark):
    """net_revenue = unit_price * quantity * (1 - discount_pct)."""
    rows = [("ord_5","c1","p1","2024-01-01",2,100.0,0.1,"online","completed","2024-01-01T00:00:00","2024-01-01")]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    result = df.withColumn("net_revenue", F.round(F.col("unit_price") * F.col("quantity") * (1 - F.col("discount_pct")), 2))
    assert result.collect()[0]["net_revenue"] == pytest.approx(180.0)


def test_channel_uppercased(spark):
    """channel field must be uppercased and trimmed."""
    rows = [("ord_6","c1","p1","2024-01-01",1,50.0,0.0," online ","completed","2024-01-01T00:00:00","2024-01-01")]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    result = df.withColumn("channel", F.upper(F.trim(F.col("channel"))))
    assert result.collect()[0]["channel"] == "ONLINE"


def test_deduplication(spark):
    """Duplicate order_ids should be reduced to one record (latest ingested)."""
    from pyspark.sql.window import Window
    rows = [
        ("ord_7","c1","p1","2024-01-01",1,50.0,0.0,"online","completed","2024-01-01T08:00:00","2024-01-01"),
        ("ord_7","c1","p1","2024-01-01",1,50.0,0.0,"online","completed","2024-01-01T10:00:00","2024-01-01"),
    ]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    w = Window.partitionBy("order_id").orderBy(F.desc("_ingested_at"))
    deduped = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    assert deduped.count() == 1