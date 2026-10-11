"""Unit tests for the Bronze ingestion job (jobs/bronze/ingest_raw.py)."""
import os

from ingest_raw import SCHEMAS, add_audit_columns, read_landing, split_valid, write_partitioned

HEADER = "order_id,customer_id,product_id,order_date,quantity,unit_price,discount_pct,channel,status\n"


def _write_csv(tmp_path, name, body):
    folder = tmp_path / name
    folder.mkdir()
    (folder / "orders.csv").write_text(HEADER + body)
    return str(folder)


def test_malformed_rows_split_to_dead_letter(spark, tmp_path):
    path = _write_csv(tmp_path, "landing",
                      "o1,c1,p1,2024-01-15,2,29.99,0.05,online,completed\n"
                      "o2,c2,p2,2024-01-15,not_a_number,10.0,0,store,shipped\n")
    valid, bad = split_valid(read_landing(spark, path, SCHEMAS["orders"]))
    assert [r.order_id for r in valid.collect()] == ["o1"]
    bad_rows = bad.collect()
    assert len(bad_rows) == 1
    assert "not_a_number" in bad_rows[0].raw_line
    assert bad_rows[0]._source_file.endswith("orders.csv")
    assert "_corrupt_record" not in valid.columns


def test_audit_columns(spark, tmp_path):
    path = _write_csv(tmp_path, "landing", "o1,c1,p1,2024-01-15,2,29.99,0.05,online,completed\n")
    valid, _ = split_valid(read_landing(spark, path, SCHEMAS["orders"]))
    row = add_audit_columns(valid, "2024-01-15").collect()[0]
    assert str(row._batch_date) == "2024-01-15"
    assert row._ingested_at is not None
    assert row._source_file.endswith("orders.csv")


def test_rerun_replaces_only_its_own_partition(spark, tmp_path):
    out = str(tmp_path / "bronze")
    day = lambda d, ids: add_audit_columns(
        spark.createDataFrame([(i,) for i in ids], "order_id string"), d)

    write_partitioned(day("2024-01-01", ["a", "b"]), out)
    write_partitioned(day("2024-01-02", ["c"]), out)
    write_partitioned(day("2024-01-02", ["c", "d"]), out)        # rerun of day 2

    partitions = sorted(p for p in os.listdir(out) if p.startswith("_batch_date="))
    assert partitions == ["_batch_date=2024-01-01", "_batch_date=2024-01-02"]
    ids = sorted(r.order_id for r in spark.read.parquet(out).collect())
    assert ids == ["a", "b", "c", "d"]                             # day 1 kept, day 2 not duplicated
