"""Shared fixtures: a local SparkSession and import paths for the job modules."""
import os
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
for layer in ("bronze", "silver", "gold"):
    sys.path.insert(0, os.path.join(ROOT, "jobs", layer))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
