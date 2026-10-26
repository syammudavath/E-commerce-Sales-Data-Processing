"""Shared pytest fixtures.

* On Databricks the notebook's existing SparkSession is reused.
* Locally a small local[2] session is created (requires pyspark>=3.5 and Java).
"""
import os
import sys

import pytest

# Make the repo root importable (`from scripts import transformations`)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # Databricks notebook / job (classic or serverless Spark Connect): reuse the session
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        yield SparkSession.builder.getOrCreate()
        return

    # Local development: never stop a shared Databricks session, only our own local one

    session = (SparkSession.builder
               .master("local[2]")
               .appName("ecommerce-sales-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "UTC")
               .getOrCreate())
    yield session
    session.stop()
