"""
Reusable PySpark transformations for the e-commerce sales pipeline.

Design rules
------------
* Every transformation takes DataFrames and returns DataFrames (no side effects),
  so it can be unit tested with small in-memory data.
* Readers receive the SparkSession explicitly (no reliance on a global ``spark``).
* Only native Spark expressions are used (no Python UDFs) so logic runs in the engine.
"""
import re
from typing import Iterable, Tuple

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField,
                               StructType)

UNKNOWN = "Unknown"
DATE_FORMAT = "d/M/yyyy"
CORRUPT_COL = "_corrupt_record"

# Explicit source schemas: avoid a costly inference pass and protect against type drift
ORDERS_SCHEMA = StructType([
    StructField("Row ID", LongType()),
    StructField("Order ID", StringType()),
    StructField("Order Date", StringType()),
    StructField("Ship Date", StringType()),
    StructField("Ship Mode", StringType()),
    StructField("Customer ID", StringType()),
    StructField("Product ID", StringType()),
    StructField("Quantity", LongType()),
    StructField("Price", DoubleType()),
    StructField("Discount", DoubleType()),
    StructField("Profit", DoubleType()),
    StructField(CORRUPT_COL, StringType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField(c, StringType()) for c in
    ["Product ID", "Category", "Sub-Category", "Product Name", "State", "Price per product"]
])


# --------------------------------------------------------------------------- helpers
def to_snake_case(name: str) -> str:
    """'Sub-Category' -> 'sub_category'; a leading underscore (metadata column) is kept."""
    stripped = name.strip()
    prefix = "_" if stripped.startswith("_") else ""
    return prefix + re.sub(r"[^0-9a-zA-Z]+", "_", stripped).strip("_").lower()


def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename all columns to snake_case (Delta does not allow spaces in names)."""
    return df.toDF(*[to_snake_case(c) for c in df.columns])


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim every string column; empty strings become NULL."""
    exprs = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            t = F.trim(F.col(field.name))
            exprs.append(F.when(t == "", None).otherwise(t).alias(field.name))
        else:
            exprs.append(F.col(field.name))
    return df.select(*exprs)


def add_ingestion_metadata(df: DataFrame, source: str) -> DataFrame:
    """Add lineage columns to raw data."""
    return (df.withColumn("_source_file", F.lit(source))
              .withColumn("_ingested_at", F.current_timestamp()))


def require_columns(df: DataFrame, cols: Iterable[str], name: str) -> None:
    """Fail fast with a clear message if an expected column is missing."""
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name}: missing required columns {missing}; found {df.columns}")


def _business_columns(df: DataFrame) -> DataFrame:
    """Drop technical columns (prefixed with '_')."""
    return df.select(*[c for c in df.columns if not c.startswith("_")])


# --------------------------------------------------------------------------- readers
def read_orders(spark: SparkSession, path: str) -> DataFrame:
    """Orders.json is a multi-line JSON array. Malformed records land in _corrupt_record."""
    return (spark.read.schema(ORDERS_SCHEMA)
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .json(path))


def read_products(spark: SparkSession, path: str) -> DataFrame:
    """Products.csv: header row, quoted fields may contain commas."""
    return (spark.read.schema(PRODUCTS_SCHEMA)
            .option("header", "true")
            .option("quote", '"').option("escape", '"')
            .option("multiLine", "true")
            .csv(path))


def read_customers(spark: SparkSession, path: str, sheet: str = "Worksheet") -> DataFrame:
    """Customer.xlsx (Databricks native Excel reader). Cells are read as strings,
    which preserves values such as postal codes with leading zeros."""
    return (spark.read.format("excel")
            .option("headerRows", 1)
            .option("dataAddress", sheet)
            .load(path))


# --------------------------------------------------------------------------- customers
# Look-alike characters used inside words ('Bi1l', 'N0ra', 'Wa55erman', 'C@thy', 'Dan!els')
LEET = {"0": "o", "1": "l", "5": "s", "@": "a", "!": "i"}


def clean_name(col: Column) -> Column:
    """Clean noisy customer names.

    1. Restore look-alike characters embedded in a word (between letters / at word start).
    2. A 'junk run' = non-letters containing at least one digit or symbol (inner spaces
       included). It is removed when followed by a lowercase letter (it split a word,
       e.g. 'Wi 4567@#$lson' -> 'Wilson'), otherwise replaced by a space
       (it separated words, e.g. 'Gary567 Hansen' -> 'Gary Hansen').
    3. Strip leading/trailing apostrophes/hyphens and collapse whitespace.
    Unicode letters (e.g. 'Haberlin' with umlaut) are preserved. Empty result -> NULL.
    """
    c = col
    for ch, rep in LEET.items():
        e = re.escape(ch)
        c = F.regexp_replace(c, rf"(?<=\p{{L}}){e}{e}(?=\p{{L}}|\s|$)", rep * 2)
        c = F.regexp_replace(c, rf"(?<=\p{{L}}){e}(?=\p{{L}})", rep)
        c = F.regexp_replace(c, rf"(?<![\p{{L}}\d]){e}(?=\p{{Ll}})", rep.upper())
    junk = r"[^\p{L}]*[^\p{L}\s'\-][^\p{L}]*"
    c = F.regexp_replace(c, junk + r"(?=\p{Ll})", "")
    c = F.regexp_replace(c, junk, " ")
    c = F.regexp_replace(c, r"^['\-\s]+|['\-\s]+$", "")
    c = F.trim(F.regexp_replace(c, r"\s+", " "))
    return F.when(c == "", None).otherwise(c)


def enrich_customers(raw: DataFrame) -> DataFrame:
    """Clean, type and de-duplicate customers (one row per customer_id)."""
    df = trim_strings(standardize_columns(raw))
    require_columns(df, ["customer_id", "customer_name", "country"], "customers")
    df = (df.withColumn("customer_id", F.upper("customer_id"))
            .filter(F.col("customer_id").isNotNull())
            .withColumn("customer_name", F.coalesce(clean_name(F.col("customer_name")), F.lit(UNKNOWN)))
            .withColumn("country", F.coalesce(F.col("country"), F.lit(UNKNOWN))))
    # Deterministic de-duplication: prefer a row with a known name
    w = Window.partitionBy("customer_id").orderBy(
        (F.col("customer_name") == UNKNOWN).asc(), F.col("customer_name"))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    return _business_columns(df)


# --------------------------------------------------------------------------- products
def enrich_products(raw: DataFrame) -> DataFrame:
    """Clean, type and de-duplicate products (one row per product_id).

    The source has duplicate product IDs with different names/states but the same
    category; keeping one row per ID prevents join fan-out (double-counted profit).
    """
    df = trim_strings(standardize_columns(raw))
    require_columns(df, ["product_id", "category", "sub_category"], "products")
    df = (df.withColumn("product_id", F.upper("product_id"))
            .filter(F.col("product_id").isNotNull())
            .withColumn("category", F.coalesce("category", F.lit(UNKNOWN)))
            .withColumn("sub_category", F.coalesce("sub_category", F.lit(UNKNOWN))))
    if "price_per_product" in df.columns:
        df = df.withColumn("price_per_product", F.expr("try_cast(price_per_product AS DOUBLE)"))
    order_cols = [F.col(c).asc_nulls_last() for c in ("product_name", "state") if c in df.columns]
    w = Window.partitionBy("product_id").orderBy(*(order_cols or [F.lit(1)]))
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
    return _business_columns(df)


# --------------------------------------------------------------------------- orders
def clean_orders(raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Type and validate orders.

    Returns (valid_orders, quarantined_orders). Quarantined rows keep all source
    columns plus ``_dq_errors`` (comma-separated reasons) instead of failing the run.
    """
    df = trim_strings(standardize_columns(raw))
    require_columns(df, ["order_id", "order_date", "customer_id", "product_id", "profit"], "orders")
    df = (df.withColumn("customer_id", F.upper("customer_id"))
            .withColumn("product_id", F.upper("product_id"))
            .withColumn("order_date", F.to_date(F.try_to_timestamp("order_date", F.lit(DATE_FORMAT))))
            .withColumn("ship_date", F.to_date(F.try_to_timestamp("ship_date", F.lit(DATE_FORMAT))))
            .withColumn("quantity", F.col("quantity").cast("int"))
            .withColumn("profit", F.col("profit").cast("double")))
    checks = [
        F.when(F.col("order_id").isNull(), F.lit("null_order_id")),
        F.when(F.col("customer_id").isNull(), F.lit("null_customer_id")),
        F.when(F.col("product_id").isNull(), F.lit("null_product_id")),
        F.when(F.col("order_date").isNull(), F.lit("invalid_order_date")),
        F.when(F.col("profit").isNull(), F.lit("null_profit")),
    ]
    if CORRUPT_COL in df.columns:
        checks.append(F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("corrupt_record")))
    reasons = F.concat_ws(",", *checks)
    df = df.withColumn("_dq_errors", F.when(reasons == "", None).otherwise(reasons))

    valid = df.filter(F.col("_dq_errors").isNull()).drop("_dq_errors")
    valid = _business_columns(valid)
    if "row_id" in valid.columns:  # exact duplicate line items
        valid = valid.dropDuplicates(["row_id"])
    invalid = df.filter(F.col("_dq_errors").isNotNull())
    return valid, invalid


def build_enriched_orders(orders: DataFrame, customers: DataFrame, products: DataFrame) -> DataFrame:
    """Master table: order info + profit (2 dp) + customer name/country + product category/sub-category.

    Customer and product tables are small, so they are broadcast (no shuffle of orders).
    LEFT joins keep every order; missing attributes become 'Unknown'.
    """
    cust = customers.select("customer_id", "customer_name", "country")
    prod = products.select("product_id", "category", "sub_category")
    return (orders
            .join(F.broadcast(cust), "customer_id", "left")
            .join(F.broadcast(prod), "product_id", "left")
            .withColumn("profit", F.round(F.col("profit"), 2).cast("decimal(18,2)"))
            .withColumn("order_year", F.year("order_date"))
            .fillna(UNKNOWN, subset=["customer_name", "country", "category", "sub_category"])
            .select("row_id", "order_id", "order_date", "order_year", "ship_date", "ship_mode",
                    "customer_id", "customer_name", "country",
                    "product_id", "category", "sub_category",
                    "quantity", "price", "discount", "profit"))


# --------------------------------------------------------------------------- aggregate
def build_profit_aggregate(enriched: DataFrame) -> DataFrame:
    """Profit by year, product category, product sub-category and customer."""
    return (enriched
            .groupBy("order_year", "category", "sub_category", "customer_id", "customer_name")
            .agg(F.sum("profit").cast("decimal(18,2)").alias("total_profit"),
                 F.countDistinct("order_id").alias("order_count"),
                 F.sum("quantity").alias("total_quantity")))
