"""Unit tests for the Silver cleaning job (jobs/silver/clean_and_validate.py)."""
from datetime import date, datetime

import pytest

from clean_and_validate import clean_orders, clean_products, deduplicate

COLS = ("order_id string, customer_id string, product_id string, order_date string, "
        "quantity int, unit_price double, discount_pct double, channel string, status string, "
        "_ingested_at timestamp, _batch_date date")


def _orders(spark, rows):
    return spark.createDataFrame(rows, COLS)


def _row(oid, qty=1, price=50.0, disc=0.0, channel="online", status="Completed",
         ingested="2024-01-01 08:00:00", batch="2024-01-01"):
    return (oid, "c1", "p1", "2024-01-01", qty, price, disc, channel, status,
            datetime.fromisoformat(ingested), date.fromisoformat(batch))


def test_invalid_orders_dropped(spark):
    df = _orders(spark, [_row("o1"), _row(None), _row("o2", qty=0), _row("o3", qty=-1),
                         _row("o4", price=0.0)])
    assert [r.order_id for r in clean_orders(df).collect()] == ["o1"]


def test_net_revenue_and_normalisation(spark):
    row = clean_orders(_orders(spark, [_row("o1", qty=2, price=100.0, disc=0.1,
                                            channel=" online ", status=" Shipped ")])).collect()[0]
    assert row.net_revenue == pytest.approx(180.0)
    assert row.channel == "ONLINE"
    assert row.status == "shipped"
    assert row.order_date == date(2024, 1, 1)


def test_missing_discount_treated_as_zero(spark):
    row = clean_orders(_orders(spark, [_row("o1", qty=1, price=40.0, disc=None)])).collect()[0]
    assert row.net_revenue == pytest.approx(40.0)


def test_dedup_keeps_latest_batch(spark):
    df = clean_orders(_orders(spark, [
        _row("o1", qty=1, ingested="2024-01-01 08:00:00", batch="2024-01-01"),
        _row("o1", qty=5, ingested="2024-01-02 08:00:00", batch="2024-01-02"),   # correction
        _row("o2", qty=1),
    ]))
    out = {r.order_id: r.quantity for r in deduplicate(df, "order_id").collect()}
    assert out == {"o1": 5, "o2": 1}


def test_product_margin(spark):
    df = spark.createDataFrame([("p1", " Lamp ", "home decor", 50.0, 30.0),
                                ("p2", "Bad", "x", 0.0, 1.0)],
                               "product_id string, product_name string, category string, "
                               "unit_price double, unit_cost double")
    rows = clean_products(df).collect()
    assert len(rows) == 1
    assert rows[0].margin_pct == pytest.approx(40.0)
    assert rows[0].category == "Home Decor"
