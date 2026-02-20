"""
test_transforms.py — Unit tests for transformation functions.

Uses pytest with a shared SparkSession fixture. Each test creates small
DataFrames in-memory to verify transform logic without needing real data
files or Delta tables.

Run with: pytest tests/ -v
"""

import pytest
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType,
)

from utils.transforms import (
    standardize_columns,
    add_ingestion_metadata,
    deduplicate_events,
    enrich_customers,
    compute_customer_segments,
    build_daily_summary,
)


@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-transforms")
        .config("spark.sql.shuffle.partitions", "2")  # Speed up tests
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestStandardizeColumns:
    def test_lowercase_columns(self, spark):
        df = spark.createDataFrame(
            [("alice", 1)], ["Customer Name", "Age"]
        )
        result = standardize_columns(df)
        assert result.columns == ["customer_name", "age"]

    def test_replace_hyphens(self, spark):
        df = spark.createDataFrame(
            [("x",)], ["first-name"]
        )
        result = standardize_columns(df)
        assert "first_name" in result.columns

    def test_no_change_needed(self, spark):
        df = spark.createDataFrame(
            [("x",)], ["already_clean"]
        )
        result = standardize_columns(df)
        assert result.columns == ["already_clean"]


class TestDeduplicateEvents:
    def test_keeps_latest_event(self, spark):
        data = [
            ("evt-1", "cust-1", "click", datetime(2024, 1, 1, 10, 0)),
            ("evt-1", "cust-1", "click", datetime(2024, 1, 1, 12, 0)),  # later
        ]
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_timestamp", TimestampType()),
        ])
        df = spark.createDataFrame(data, schema)
        result = deduplicate_events(df)

        assert result.count() == 1
        row = result.collect()[0]
        assert row.event_timestamp == datetime(2024, 1, 1, 12, 0)

    def test_no_duplicates_unchanged(self, spark):
        data = [
            ("evt-1", "cust-1", "click", datetime(2024, 1, 1)),
            ("evt-2", "cust-2", "view", datetime(2024, 1, 2)),
        ]
        schema = StructType([
            StructField("event_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("event_type", StringType()),
            StructField("event_timestamp", TimestampType()),
        ])
        df = spark.createDataFrame(data, schema)
        result = deduplicate_events(df)
        assert result.count() == 2


class TestEnrichCustomers:
    def test_enrichment_adds_metrics(self, spark):
        customers = spark.createDataFrame(
            [("cust-1", "Alice", "active")],
            ["customer_id", "first_name", "status"]
        )
        events = spark.createDataFrame(
            [
                ("cust-1", "click", datetime(2024, 1, 1)),
                ("cust-1", "view", datetime(2024, 1, 2)),
                ("cust-1", "click", datetime(2024, 1, 3)),
            ],
            ["customer_id", "event_type", "event_timestamp"]
        )

        result = enrich_customers(customers, events)
        row = result.collect()[0]

        assert row.total_events == 3
        assert row.distinct_event_types == 2

    def test_inner_join_drops_customers_without_events(self, spark):
        """This test documents the BUG in enrich_customers.

        Customers with zero events are dropped because it uses
        an inner join instead of a left join.
        """
        customers = spark.createDataFrame(
            [("cust-1", "Alice"), ("cust-2", "Bob")],
            ["customer_id", "first_name"]
        )
        events = spark.createDataFrame(
            [("cust-1", "click", datetime(2024, 1, 1))],
            ["customer_id", "event_type", "event_timestamp"]
        )

        result = enrich_customers(customers, events)
        # Bob is lost! This is the documented bug.
        assert result.count() == 1
        assert result.collect()[0].customer_id == "cust-1"


class TestCustomerSegments:
    def test_power_user_segment(self, spark):
        df = spark.createDataFrame(
            [("cust-1", 150, 7)],
            ["customer_id", "total_events", "distinct_event_types"]
        )
        result = compute_customer_segments(df)
        assert result.collect()[0].segment == "power_user"

    def test_active_segment(self, spark):
        df = spark.createDataFrame(
            [("cust-1", 25, 2)],
            ["customer_id", "total_events", "distinct_event_types"]
        )
        result = compute_customer_segments(df)
        assert result.collect()[0].segment == "active"

    def test_dormant_segment(self, spark):
        df = spark.createDataFrame(
            [("cust-1", 3, 1)],
            ["customer_id", "total_events", "distinct_event_types"]
        )
        result = compute_customer_segments(df)
        assert result.collect()[0].segment == "dormant"
