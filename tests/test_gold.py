"""Unit tests for the Gold aggregations (jobs/gold/build_aggregates.py)."""
from datetime import date

import pytest

from build_aggregates import customer_metrics, daily_revenue, product_performance

ORDER_COLS = ("order_id string, customer_id string, product_id string, order_date date, "
              "quantity int, net_revenue double, channel string, status string")


def _orders(spark):
    d = date(2024, 1, 1)
    return spark.createDataFrame([
        ("o1", "c1", "p1", d, 2, 100.0, "ONLINE", "completed"),
        ("o2", "c2", "p1", d, 1, 50.0,  "ONLINE", "shipped"),
        ("o3", "c1", "p2", d, 1, 30.0,  "STORE",  "pending"),
        ("o4", "c2", "p2", d, 4, 999.0, "STORE",  "cancelled"),
    ], ORDER_COLS)


def test_daily_revenue_counts_completed_and_shipped(spark):
    rows = {r.channel: r for r in daily_revenue(_orders(spark)).collect()}
    assert set(rows) == {"ONLINE"}
    assert rows["ONLINE"].total_orders == 2
    assert rows["ONLINE"].total_revenue == pytest.approx(150.0)
    assert rows["ONLINE"].unique_customers == 2


def test_customer_metrics_rfm_and_tiers(spark):
    customers = spark.createDataFrame([("c1", "Ann", "Lee", "a@x.com", "Canada")],
                                      "customer_id string, first_name string, last_name string, "
                                      "email string, country string")
    rows = {r.customer_id: r for r in customer_metrics(_orders(spark), customers,
                                                         as_of="2024-04-30").collect()}
    c1 = rows["c1"]
    assert c1.total_orders == 2
    assert c1.lifetime_value_usd == pytest.approx(130.0)
    assert c1.recency_days == 120
    assert c1.churn_risk == "high"
    assert c1.ltv_tier == "bronze"
    assert c1.first_name == "Ann"
    assert rows["c2"].total_orders == 1             # cancelled order excluded


def test_product_performance_gross_profit(spark):
    products = spark.createDataFrame([("p1", "Lamp", "Home", 50.0, 20.0, 60.0)],
                                     "product_id string, product_name string, category string, "
                                     "unit_price double, unit_cost double, margin_pct double")
    rows = {r.product_id: r for r in product_performance(_orders(spark), products).collect()}
    assert rows["p1"].units_sold == 3
    assert rows["p1"].total_revenue == pytest.approx(150.0)
    assert rows["p1"].gross_profit == pytest.approx(150.0 - 20.0 * 3)
    assert rows["p2"].units_sold == 1               # cancelled line excluded
