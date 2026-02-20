"""
transforms.py — Business transformation functions.

Each function takes a DataFrame and returns a transformed DataFrame,
following the principle of "DataFrames in, DataFrames out." This makes
every transform composable, testable, and stateless — like pure functions
in functional programming.
"""

from typing import List, Optional

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType,
)


# ---- Schema Definitions ----

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("region", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("status", StringType(), True),
])

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("event_type", StringType(), True),
    StructField("event_date", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("properties", StringType(), True),  # JSON string
    StructField("_corrupt_record", StringType(), True),
])


# ---- Bronze Layer Transforms (raw → cleaned) ----

def standardize_columns(df: DataFrame) -> DataFrame:
    """Normalize column names: lowercase, replace spaces/hyphens with underscores.

    This is the first step for any raw data — source systems use wildly
    inconsistent naming conventions. Standardizing early prevents a
    cascade of mismatched column references downstream.
    """
    for col_name in df.columns:
        clean_name = col_name.strip().lower().replace(" ", "_").replace("-", "_")
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    return df


def add_ingestion_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Add audit columns for data lineage tracking.

    Every row gets stamped with when it was ingested and where it came
    from. This is essential for debugging data issues — "when did this
    bad row arrive?" becomes a simple filter instead of a forensic hunt.
    """
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
        .withColumn("_pipeline_version", F.lit("1.2.0"))
    )


def quarantine_corrupt_records(df: DataFrame) -> tuple:
    """Separate corrupt JSON records from valid ones.

    Spark's PERMISSIVE mode puts unparseable rows into the
    _corrupt_record column. This function splits the DataFrame
    into two: clean records and quarantined records.

    Returns:
        Tuple of (clean_df, corrupt_df)
    """
    if "_corrupt_record" not in df.columns:
        return df, None

    corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
    clean_df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    return clean_df, corrupt_df


# ---- Silver Layer Transforms (cleaned → enriched) ----

def deduplicate_events(df: DataFrame, key_column: str = "event_id",
                       order_column: str = "event_timestamp") -> DataFrame:
    """Remove duplicate events, keeping the latest version.

    Uses a window function to rank rows within each key group,
    ordered by timestamp descending. Only the first (latest) row
    per key survives. This is a standard SCD Type 1 dedup pattern.

    NOTE: This approach collects all duplicates into memory on a
    single partition per key. For keys with extreme cardinality
    (billions of unique values), this can cause data skew.
    """
    window = Window.partitionBy(key_column).orderBy(F.col(order_column).desc())

    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def enrich_customers(customers_df: DataFrame, events_df: DataFrame) -> DataFrame:
    """Join customer profiles with aggregated event metrics.

    Creates a "customer 360" view by joining the customer master
    with event aggregations. This is the classic star schema pattern —
    a fact table (events) aggregated and joined to a dimension (customers).

    BUG: This join uses an inner join. Customers with zero events
    are silently dropped from the output. For a "customer 360" view,
    this should be a LEFT join to retain all customers.
    """
    event_metrics = (
        events_df
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("event_type").alias("distinct_event_types"),
            F.max("event_timestamp").alias("last_event_at"),
            F.min("event_timestamp").alias("first_event_at"),
        )
    )

    return customers_df.join(event_metrics, on="customer_id", how="inner")


def compute_customer_segments(df: DataFrame) -> DataFrame:
    """Assign customer segments based on activity level.

    Segmentation logic:
        - "power_user": 100+ events AND 5+ distinct event types
        - "active": 10+ events
        - "dormant": < 10 events

    WARNING: These thresholds are hardcoded. In production, they should
    come from config or a feature store so business stakeholders can
    tune them without code changes.
    """
    return df.withColumn(
        "segment",
        F.when(
            (F.col("total_events") >= 100) & (F.col("distinct_event_types") >= 5),
            F.lit("power_user")
        ).when(
            F.col("total_events") >= 10,
            F.lit("active")
        ).otherwise(
            F.lit("dormant")
        )
    )


def parse_event_properties(df: DataFrame) -> DataFrame:
    """Extract structured fields from the JSON properties column.

    The raw events store a free-form JSON string in `properties`.
    This function extracts known fields into proper typed columns
    for easier downstream querying.
    """
    return (
        df
        .withColumn("prop_parsed", F.from_json(F.col("properties"), "map<string,string>"))
        .withColumn("page_url", F.col("prop_parsed").getItem("page_url"))
        .withColumn("device_type", F.col("prop_parsed").getItem("device_type"))
        .withColumn("session_id", F.col("prop_parsed").getItem("session_id"))
        .drop("prop_parsed", "properties")
    )


# ---- Gold Layer Transforms (enriched → aggregated) ----

def build_daily_summary(events_df: DataFrame) -> DataFrame:
    """Build a daily aggregated summary of events by type and region.

    This is the "gold" layer — pre-aggregated tables designed for
    BI dashboards and analyst SQL queries. Instead of analysts scanning
    billions of raw events, they query a small summary table.
    """
    return (
        events_df
        .withColumn("event_date", F.to_date("event_timestamp"))
        .groupBy("event_date", "event_type", "region")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("session_id").alias("unique_sessions"),
        )
        .orderBy("event_date", "event_type")  # BUG: orderBy on full dataset
        # triggers a global sort — expensive and unnecessary for a table that
        # will be queried with its own ORDER BY clauses. Should be removed.
    )


def build_customer_lifetime_value(
    customers_df: DataFrame,
    transactions_df: DataFrame,
) -> DataFrame:
    """Calculate customer lifetime value (CLV) and spending patterns.

    Joins customer data with transaction history to compute:
    - Total lifetime spend
    - Average transaction value
    - Transaction frequency (days between purchases)
    - Months since first purchase

    This powers the revenue analytics dashboard.
    """
    txn_metrics = (
        transactions_df
        .groupBy("customer_id")
        .agg(
            F.sum("amount").alias("lifetime_spend"),
            F.avg("amount").alias("avg_transaction_value"),
            F.count("*").alias("transaction_count"),
            F.min("txn_date").alias("first_purchase"),
            F.max("txn_date").alias("last_purchase"),
        )
    )

    clv_df = (
        customers_df
        .join(txn_metrics, on="customer_id", how="left")
        .withColumn(
            "months_since_first_purchase",
            F.months_between(F.current_date(), F.col("first_purchase"))
        )
        .withColumn(
            "purchase_frequency_days",
            F.when(
                F.col("transaction_count") > 1,
                F.datediff(F.col("last_purchase"), F.col("first_purchase"))
                / (F.col("transaction_count") - 1)
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "clv_score",
            # Simple CLV = avg_value * frequency * tenure
            # BUG: division by zero if months_since_first_purchase is 0
            # for same-day signups. Should use greatest(1, months).
            F.col("avg_transaction_value")
            * (F.col("transaction_count") / F.col("months_since_first_purchase"))
            * F.lit(12)  # annualize
        )
    )

    return clv_df
