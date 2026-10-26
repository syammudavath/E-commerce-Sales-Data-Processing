"""
Orchestration for the e-commerce sales pipeline:
    raw tables -> enriched customers/products -> enriched orders (+ quarantine)
    -> profit aggregate -> reconciliation checks.

Usage (Databricks notebook or job):
    from scripts.pipeline import PipelineConfig, run_pipeline
    cfg = PipelineConfig(catalog="dev_test", schema="ecommerce_sales", data_path="/Workspace/.../data")
    summary = run_pipeline(spark, cfg)
"""
import logging
from dataclasses import dataclass
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from scripts import transformations as T

logger = logging.getLogger("ecommerce_pipeline")


class DataQualityError(Exception):
    """Raised when a post-load reconciliation check fails."""


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str
    schema: str
    data_path: str  # folder with Orders.json, Customer.xlsx, Products.csv
    orders_file: str = "Orders.json"
    customers_file: str = "Customer.xlsx"
    products_file: str = "Products.csv"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{name}"

    def path(self, file_name: str) -> str:
        """Workspace files must be read with the file: scheme; Volumes / cloud paths as-is."""
        full = f"{self.data_path.rstrip('/')}/{file_name}"
        return f"file:{full}" if full.startswith("/Workspace/") else full


def write_table(df: DataFrame, full_name: str) -> None:
    """Idempotent overwrite of a Delta table; failures are logged and re-raised."""
    try:
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true").saveAsTable(full_name))
        logger.info("Wrote %s", full_name)
    except Exception:
        logger.exception("Failed writing %s", full_name)
        raise


def load_raw(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Task 1: one raw table per source file (snake_case columns + lineage metadata)."""
    sources = {
        "raw_orders": (T.read_orders, cfg.path(cfg.orders_file)),
        "raw_customers": (T.read_customers, cfg.path(cfg.customers_file)),
        "raw_products": (T.read_products, cfg.path(cfg.products_file)),
    }
    for table, (reader, path) in sources.items():
        try:
            df = reader(spark, path)
        except Exception:
            logger.exception("Failed reading %s", path)
            raise
        write_table(T.add_ingestion_metadata(T.standardize_columns(df), path), cfg.table(table))


def build_enriched(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Tasks 2 and 3: enriched customers, products and orders (+ quarantine)."""
    write_table(T.enrich_customers(spark.table(cfg.table("raw_customers"))), cfg.table("enriched_customers"))
    write_table(T.enrich_products(spark.table(cfg.table("raw_products"))), cfg.table("enriched_products"))

    valid, invalid = T.clean_orders(spark.table(cfg.table("raw_orders")))
    enriched = T.build_enriched_orders(
        valid, spark.table(cfg.table("enriched_customers")), spark.table(cfg.table("enriched_products")))
    write_table(enriched, cfg.table("enriched_orders"))
    write_table(invalid, cfg.table("quarantine_orders"))


def build_aggregate(spark: SparkSession, cfg: PipelineConfig) -> None:
    """Task 4: profit by year, category, sub-category and customer."""
    write_table(T.build_profit_aggregate(spark.table(cfg.table("enriched_orders"))), cfg.table("agg_profit"))


def reconcile(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, object]:
    """Integration checks on the loaded tables. Raises DataQualityError on failure."""
    raw_cnt = spark.table(cfg.table("raw_orders")).count()
    enr = spark.table(cfg.table("enriched_orders"))
    enr_cnt = enr.count()
    q_cnt = spark.table(cfg.table("quarantine_orders")).count()
    raw_unique = spark.table(cfg.table("raw_orders")).select("row_id").distinct().count()
    enr_total = enr.agg(F.sum("profit")).first()[0]
    agg_total = spark.table(cfg.table("agg_profit")).agg(F.sum("total_profit")).first()[0]

    def dup_keys(table: str, key: str) -> int:
        return spark.table(cfg.table(table)).groupBy(key).count().filter("count > 1").count()

    checks = {
        # Only exact duplicate row_ids may be dropped; everything else is enriched or quarantined
        "no rows lost (distinct raw <= enriched + quarantined <= raw)":
            raw_unique <= enr_cnt + q_cnt <= raw_cnt,
        "no join fan-out (row_id unique)": enr.select("row_id").distinct().count() == enr_cnt,
        "profit preserved (enriched = aggregate)": enr_total == agg_total,
        "unique product_id": dup_keys("enriched_products", "product_id") == 0,
        "unique customer_id": dup_keys("enriched_customers", "customer_id") == 0,
    }
    failed = [name for name, ok in checks.items() if not ok]
    if failed:
        raise DataQualityError(f"Reconciliation failed: {failed}")

    return {
        "raw_orders": raw_cnt,
        "enriched_orders": enr_cnt,
        "quarantined_orders": q_cnt,
        "orders_unknown_product": enr.filter(F.col("category") == T.UNKNOWN).count(),
        "orders_unknown_customer_name": enr.filter(F.col("customer_name") == T.UNKNOWN).count(),
        "total_profit": enr_total,
    }


def run_pipeline(spark: SparkSession, cfg: PipelineConfig) -> Dict[str, object]:
    """Run all stages end to end and return a reconciliation summary."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")
    load_raw(spark, cfg)
    build_enriched(spark, cfg)
    build_aggregate(spark, cfg)
    summary = reconcile(spark, cfg)
    logger.info("Pipeline finished: %s", summary)
    return summary
