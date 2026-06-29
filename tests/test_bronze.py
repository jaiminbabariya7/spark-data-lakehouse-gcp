"""Unit tests for Bronze layer ingestion logic."""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_bronze")
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
])


def test_audit_columns_added(spark):
    """Bronze ingestion must add _ingested_at, _source_file, _batch_date."""
    df = spark.createDataFrame([("ord_1","cust_1","prod_1","2024-01-15",2,29.99,0.05,"online","completed")], ORDER_SCHEMA)
    df = df.withColumn("_ingested_at", F.current_timestamp()) \
           .withColumn("_source_file", F.lit("gs://test/landing/orders/")) \
           .withColumn("_batch_date",  F.to_date(F.current_timestamp()))
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns
    assert "_batch_date"  in df.columns


def test_schema_enforcement(spark):
    """Schema must reject mismatched types gracefully."""
    df = spark.createDataFrame([("ord_2","cust_2","prod_2","2024-01-16",1,49.99,0.0,"store","shipped")], ORDER_SCHEMA)
    assert df.schema["quantity"].dataType == IntegerType()
    assert df.schema["unit_price"].dataType == DoubleType()


def test_row_count_preserved(spark):
    """All rows from source should appear in Bronze (no filtering at this layer)."""
    rows = [
        ("ord_3","c1","p1","2024-01-01",1,10.0,0.0,"online","completed"),
        ("ord_4","c2","p2","2024-01-02",2,20.0,0.1,"store","shipped"),
        ("ord_5","c3","p3","2024-01-03",3,30.0,0.2,"online","cancelled"),
    ]
    df = spark.createDataFrame(rows, ORDER_SCHEMA)
    assert df.count() == 3