"""Unit tests for scripts/transformations.py (run with: pytest tests -v).

All tests use small in-memory DataFrames, so they are fast and independent of the source files.
"""
import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from scripts import transformations as T


# --------------------------------------------------------------------------- builders
def customers_df(spark, rows):
    return spark.createDataFrame(rows, "`Customer ID` STRING, `Customer Name` STRING, Country STRING")


def products_df(spark, rows):
    return spark.createDataFrame(rows, T.PRODUCTS_SCHEMA)


def orders_df(spark, rows):
    schema = StructType([f for f in T.ORDERS_SCHEMA.fields if f.name != T.CORRUPT_COL])
    return spark.createDataFrame(rows, schema)


def order(row_id, oid, odate, cid, pid, profit, qty=1):
    return (row_id, oid, odate, odate, "Standard Class", cid, pid, qty, 10.0, 0.0, profit)


@pytest.fixture
def dims(spark):
    cust = T.enrich_customers(customers_df(spark, [("C1", "Ann Lee", "United States")]))
    prod = T.enrich_products(products_df(spark, [
        ("P1", "Furniture", "Chairs", "A", "NY", "1"),
        ("P1", "Furniture", "Chairs", "B", "TX", "2"),  # duplicate id must not fan out
        ("P2", "Technology", "Phones", "C", "CA", "3"),
    ]))
    return cust, prod


# --------------------------------------------------------------------------- helpers
@pytest.mark.parametrize("raw, expected", [
    ("Sub-Category", "sub_category"),
    ("Customer ID", "customer_id"),
    (" Price per product ", "price_per_product"),
    ("_corrupt_record", "_corrupt_record"),  # metadata prefix kept
])
def test_to_snake_case(raw, expected):
    assert T.to_snake_case(raw) == expected


def test_require_columns_raises(spark):
    df = spark.createDataFrame([(1,)], "a INT")
    with pytest.raises(ValueError, match="missing required columns"):
        T.require_columns(df, ["a", "b"], "t")


def test_trim_strings_blank_to_null(spark):
    df = spark.createDataFrame([("  x ", 1), ("   ", 2)], "s STRING, n INT")
    assert [r.s for r in T.trim_strings(df).orderBy("n").collect()] == ["x", None]


# --------------------------------------------------------------------------- customers
@pytest.mark.parametrize("raw, expected", [
    ("Gary567 Hansen", "Gary Hansen"),
    ("Tam&^*ara Willing___)ham", "Tamara Willingham"),
    ("Ad.       ..am Hart", "Adam Hart"),
    ("  O'Neil Smith-Jones ", "O'Neil Smith-Jones"),
    ("Bi1l Eplett", "Bill Eplett"),                 # look-alike digit
    ("Helen Wa55erman", "Helen Wasserman"),         # doubled look-alike
    ("Rick Wi 4567@#$lson", "Rick Wilson"),         # junk + space split a word
    ("Paul  4567Van Hugh", "Paul Van Hugh"),        # junk between words -> space
    ("Anna H\u00e4berlin", "Anna H\u00e4berlin"),   # unicode letters kept
    ("Maribeth 5chnelling", "Maribeth Schnelling"), # look-alike at word start
    ("Ji11 Stevenson", "Jill Stevenson"),           # doubled look-alike at word end
    ("Jason Fortune-", "Jason Fortune"),            # trailing hyphen
    ("@#$123", T.UNKNOWN),                          # all noise
    (None, T.UNKNOWN),                              # missing
])
def test_customer_name_cleaning(spark, raw, expected):
    df = T.enrich_customers(customers_df(spark, [("C1", raw, "US")]))
    assert df.first().customer_name == expected


def test_customer_missing_country(spark):
    df = T.enrich_customers(customers_df(spark, [("C1", "Ann Lee", None)]))
    assert df.first().country == T.UNKNOWN


def test_customer_dedup_and_null_id(spark):
    df = T.enrich_customers(customers_df(spark, [
        ("c1", None, "US"), ("C1", "Ann Lee", "US"), (None, "No Id", "US"), ("  ", "Blank", "US")]))
    rows = df.collect()
    assert len(rows) == 1                       # null/blank ids dropped, case-insensitive dup collapsed
    assert rows[0].customer_name == "Ann Lee"   # known name preferred


def test_enriched_customers_drop_metadata(spark):
    raw = T.add_ingestion_metadata(T.standardize_columns(
        customers_df(spark, [("C1", "Ann Lee", "US")])), "src")
    assert not [c for c in T.enrich_customers(raw).columns if c.startswith("_")]


# --------------------------------------------------------------------------- products
def test_product_dedup_and_price_cast(spark):
    df = T.enrich_products(products_df(spark, [
        ("P1", "Furniture", "Chairs", "B chair", "NY", "10.5"),
        ("P1", "Furniture", "Chairs", "A chair", "TX", "12"),
        ("P2", None, None, "X", "CA", "abc"),
    ]))
    got = {r.product_id: r for r in df.collect()}
    assert len(got) == 2
    assert got["P1"].product_name == "A chair"          # deterministic pick
    assert got["P1"].price_per_product == 12.0
    assert got["P2"].price_per_product is None          # invalid price -> NULL, not a failure
    assert (got["P2"].category, got["P2"].sub_category) == (T.UNKNOWN, T.UNKNOWN)


# --------------------------------------------------------------------------- orders
def test_order_date_parsing_and_quarantine(spark):
    valid, invalid = T.clean_orders(orders_df(spark, [
        order(1, "O1", "21/8/2016", "C1", "P1", 1.0),
        order(2, "O2", "1/1/2017", "C1", "P1", 1.0),
        order(3, "O3", "2017-13-45", "C1", "P1", 1.0),  # bad date
        order(4, None, "1/1/2017", "C1", "P1", 1.0),     # null order id
        order(5, "O5", "1/1/2017", "C1", "P1", None),    # null profit
        order(6, "O6", "1/1/2017", None, None, 1.0),     # multiple errors
        order(1, "O1", "21/8/2016", "C1", "P1", 1.0),    # duplicate row
    ]))
    dates = {r.row_id: r.order_date for r in valid.collect()}
    assert dates == {1: dt.date(2016, 8, 21), 2: dt.date(2017, 1, 1)}
    errs = {r.row_id: r._dq_errors for r in invalid.collect()}
    assert errs == {3: "invalid_order_date", 4: "null_order_id", 5: "null_profit",
                    6: "null_customer_id,null_product_id"}


def test_corrupt_record_quarantined(spark):
    schema = T.ORDERS_SCHEMA
    df = spark.createDataFrame(
        [(None, None, None, None, None, None, None, None, None, None, None, "{bad json")], schema)
    valid, invalid = T.clean_orders(df)
    assert valid.count() == 0
    assert "corrupt_record" in invalid.first()._dq_errors


# --------------------------------------------------------------------------- enriched orders
def test_enriched_orders(spark, dims):
    cust, prod = dims
    orders, _ = T.clean_orders(orders_df(spark, [
        order(1, "O1", "21/8/2016", "C1", "P1", 10.456),
        order(2, "O2", "1/1/2017", "C1", "P2", -14.924),
        order(3, "O3", "1/1/2017", "C9", "P9", 5.0),   # unknown customer and product
    ]))
    e = T.build_enriched_orders(orders, cust, prod)
    assert e.count() == 3                               # no fan-out, no dropped orders
    got = {r.row_id: r for r in e.collect()}
    assert got[1].profit == Decimal("10.46")
    assert got[2].profit == Decimal("-14.92")
    assert (got[1].customer_name, got[1].country) == ("Ann Lee", "United States")
    assert (got[1].category, got[1].sub_category) == ("Furniture", "Chairs")
    assert got[1].order_year == 2016
    assert (got[3].customer_name, got[3].category, got[3].sub_category) == (T.UNKNOWN,) * 3
    assert dict(e.dtypes)["profit"] == "decimal(18,2)"


# --------------------------------------------------------------------------- aggregate
def test_profit_aggregate(spark, dims):
    cust, prod = dims
    orders, _ = T.clean_orders(orders_df(spark, [
        order(1, "O1", "1/1/2016", "C1", "P1", 1.111),
        order(2, "O1", "1/1/2016", "C1", "P1", 2.222),
        order(3, "O2", "1/1/2017", "C1", "P1", 3.0),
        order(4, "O3", "1/1/2017", "C1", "P2", -1.0),
    ]))
    agg = T.build_profit_aggregate(T.build_enriched_orders(orders, cust, prod))
    got = {(r.order_year, r.sub_category): (r.total_profit, r.order_count) for r in agg.collect()}
    assert got[(2016, "Chairs")] == (Decimal("3.33"), 1)   # 1.11 + 2.22, one distinct order
    assert got[(2017, "Chairs")] == (Decimal("3.00"), 1)
    assert got[(2017, "Phones")] == (Decimal("-1.00"), 1)
    assert agg.agg(F.sum("total_profit")).first()[0] == Decimal("5.33")  # totals preserved


def test_empty_input(spark, dims):
    cust, prod = dims
    orders, invalid = T.clean_orders(orders_df(spark, []))
    agg = T.build_profit_aggregate(T.build_enriched_orders(orders, cust, prod))
    assert agg.count() == 0
    assert invalid.count() == 0
