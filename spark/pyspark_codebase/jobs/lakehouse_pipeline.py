"""
lakehouse_pipeline.py — Main pipeline orchestrator.

This is the entry point. It runs the full medallion architecture pipeline:
  Bronze (raw ingest) → Silver (cleaned + enriched) → Gold (aggregated)

Each layer is a logical step, not a separate job. This keeps the pipeline
simple but means a failure in the gold layer still blocks the silver output
from being committed — a tradeoff between simplicity and fault isolation.

Usage:
    spark-submit --packages io.delta:delta-spark_2.12:3.0.0 \\
        jobs/lakehouse_pipeline.py --config config/pipeline_config.yaml
"""

import argparse
import logging
import sys
import time
from datetime import datetime

from pyspark.sql import functions as F

from utils.session import load_config, create_spark_session
from utils.quality import QualityEngine, DataQualityError
from utils.delta_helpers import write_delta, compact_table
from utils.transforms import (
    standardize_columns,
    add_ingestion_metadata,
    quarantine_corrupt_records,
    deduplicate_events,
    enrich_customers,
    compute_customer_segments,
    parse_event_properties,
    build_daily_summary,
    build_customer_lifetime_value,
    CUSTOMER_SCHEMA,
    EVENT_SCHEMA,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("lakehouse_pipeline")


def main():
    parser = argparse.ArgumentParser(description="ACME Lakehouse Pipeline")
    parser.add_argument("--config", required=True, help="Path to pipeline config YAML")
    parser.add_argument("--env", default="production", help="Environment (production/staging/dev)")
    parser.add_argument("--layers", nargs="+", default=["bronze", "silver", "gold"],
                        help="Which layers to run (default: all)")
    parser.add_argument("--date", default=None,
                        help="Processing date override (YYYY-MM-DD)")
    args = parser.parse_args()

    # Load config and create Spark session
    config = load_config(args.config, args.env)
    spark = create_spark_session(config)

    processing_date = args.date or datetime.utcnow().strftime("%Y-%m-%d")
    logger.info(f"Starting pipeline for date={processing_date}, layers={args.layers}")

    pipeline_metrics = {
        "start_time": time.time(),
        "processing_date": processing_date,
        "layers": {},
    }

    try:
        # ── BRONZE LAYER ──────────────────────────────────────────────
        if "bronze" in args.layers:
            logger.info("=" * 60)
            logger.info("BRONZE LAYER — Raw Ingestion")
            logger.info("=" * 60)

            bronze_metrics = run_bronze_layer(spark, config, processing_date)
            pipeline_metrics["layers"]["bronze"] = bronze_metrics

        # ── SILVER LAYER ──────────────────────────────────────────────
        if "silver" in args.layers:
            logger.info("=" * 60)
            logger.info("SILVER LAYER — Clean & Enrich")
            logger.info("=" * 60)

            silver_metrics = run_silver_layer(spark, config, processing_date)
            pipeline_metrics["layers"]["silver"] = silver_metrics

        # ── GOLD LAYER ────────────────────────────────────────────────
        if "gold" in args.layers:
            logger.info("=" * 60)
            logger.info("GOLD LAYER — Aggregate")
            logger.info("=" * 60)

            gold_metrics = run_gold_layer(spark, config, processing_date)
            pipeline_metrics["layers"]["gold"] = gold_metrics

        pipeline_metrics["status"] = "success"

    except DataQualityError as e:
        logger.error(f"Pipeline failed on quality checks: {e}")
        logger.error(f"Quality report: {e.report}")
        pipeline_metrics["status"] = "failed_quality"
        sys.exit(1)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        pipeline_metrics["status"] = "failed"
        sys.exit(1)

    finally:
        elapsed = time.time() - pipeline_metrics["start_time"]
        pipeline_metrics["elapsed_seconds"] = round(elapsed, 2)
        logger.info(f"Pipeline completed in {elapsed:.1f}s — metrics: {pipeline_metrics}")
        spark.stop()


# ── BRONZE ────────────────────────────────────────────────────────────

def run_bronze_layer(spark, config, processing_date) -> dict:
    """Bronze layer: ingest raw data from sources into Delta tables.

    This layer's job is simple: read raw files, add metadata, write to Delta.
    No business logic here — just reliable, auditable ingestion.
    """
    metrics = {}

    for source_name, source_config in config["sources"].items():
        logger.info(f"Ingesting source: {source_name}")

        # Read raw data
        reader = spark.read.format(source_config["format"])

        for opt_key, opt_val in source_config.get("read_options", {}).items():
            reader = reader.option(opt_key, opt_val)

        # Apply schema if available (prevents inferSchema cost)
        if source_name == "raw_events":
            reader = reader.schema(EVENT_SCHEMA)
        elif source_name == "raw_customers":
            reader = reader.schema(CUSTOMER_SCHEMA)

        raw_df = reader.load(source_config["path"])

        # Standardize and tag
        bronze_df = standardize_columns(raw_df)
        bronze_df = add_ingestion_metadata(bronze_df, source_name)

        # Handle corrupt records for JSON sources
        if source_config["format"] == "json":
            bronze_df, corrupt_df = quarantine_corrupt_records(bronze_df)
            if corrupt_df is not None:
                corrupt_count = corrupt_df.count()
                if corrupt_count > 0:
                    logger.warning(f"Quarantined {corrupt_count} corrupt records from {source_name}")
                    corrupt_df.write.mode("append").parquet(
                        f"{config['quality']['quarantine_path']}/{source_name}/corrupt/"
                    )

        # Write to bronze Delta table
        bronze_path = f"{config['layers']['bronze']['path']}/{source_name}"
        write_result = write_delta(
            bronze_df, bronze_path,
            mode=config["layers"]["bronze"]["write_mode"],
            partition_by=source_config.get("partition_columns"),
        )

        metrics[source_name] = write_result
        logger.info(f"Bronze write complete for {source_name}: {write_result}")

    return metrics


# ── SILVER ────────────────────────────────────────────────────────────

def run_silver_layer(spark, config, processing_date) -> dict:
    """Silver layer: clean, deduplicate, validate, and enrich data.

    Reads from bronze, applies business rules, runs quality checks,
    and writes enriched data to silver Delta tables.
    """
    metrics = {}
    quality_engine = QualityEngine(spark, config["quality"])

    # Read bronze tables
    events_df = spark.read.format("delta").load(
        f"{config['layers']['bronze']['path']}/raw_events"
    )
    customers_df = spark.read.format("delta").load(
        f"{config['layers']['bronze']['path']}/raw_customers"
    )

    # ── Process Events ──
    logger.info("Processing events...")
    events_clean = parse_event_properties(events_df)
    events_deduped = deduplicate_events(events_clean)

    # Run quality checks
    events_validated, events_report = quality_engine.validate(events_deduped, "events")
    logger.info(f"Events quality report: {events_report}")

    # Write silver events
    events_path = f"{config['layers']['silver']['path']}/events"
    events_result = write_delta(
        events_validated, events_path,
        mode="append",  # BUG: config says merge, but this hardcodes append.
        # Over time, the silver events table accumulates duplicates
        # from repeated pipeline runs on the same date.
    )
    metrics["events"] = {**events_result, "quality": events_report}

    # ── Process Customers ──
    logger.info("Processing customers...")
    customers_clean = standardize_columns(customers_df)

    # Validate customers
    customers_validated, customers_report = quality_engine.validate(
        customers_clean, "customers"
    )
    logger.info(f"Customers quality report: {customers_report}")

    # Enrich with event data
    enriched = enrich_customers(customers_validated, events_validated)
    segmented = compute_customer_segments(enriched)

    # Write silver customers with merge (upsert)
    customers_path = f"{config['layers']['silver']['path']}/customers"
    customers_result = write_delta(
        segmented, customers_path,
        mode=config["layers"]["silver"]["write_mode"],
        merge_key=config["layers"]["silver"]["merge_key"],
        partition_by=config["layers"]["silver"].get("partition_by"),
    )
    metrics["customers"] = {**customers_result, "quality": customers_report}

    # Run compaction on silver tables
    for table_name in ["events", "customers"]:
        table_path = f"{config['layers']['silver']['path']}/{table_name}"
        try:
            compact_table(spark, table_path)
            logger.info(f"Compacted {table_name}")
        except Exception as e:
            logger.warning(f"Compaction failed for {table_name}: {e}")

    return metrics


# ── GOLD ──────────────────────────────────────────────────────────────

def run_gold_layer(spark, config, processing_date) -> dict:
    """Gold layer: build pre-aggregated tables for analytics.

    Reads from silver, computes aggregations, and writes to gold.
    These tables are directly consumed by BI dashboards (Looker,
    Tableau) and analyst SQL queries.
    """
    metrics = {}

    # Read silver tables
    events_df = spark.read.format("delta").load(
        f"{config['layers']['silver']['path']}/events"
    )
    customers_df = spark.read.format("delta").load(
        f"{config['layers']['silver']['path']}/customers"
    )

    # Read transactions from bronze (no silver processing yet)
    # BUG: Transactions skip the silver layer entirely — no dedup,
    # no quality checks. This inconsistency means gold tables
    # are built from mixed-quality data.
    transactions_df = spark.read.format("delta").load(
        f"{config['layers']['bronze']['path']}/raw_transactions"
    )

    # ── Daily Event Summary ──
    logger.info("Building daily event summary...")
    # Need to re-join with customers for region data
    events_with_region = events_df.join(
        customers_df.select("customer_id", "region"),
        on="customer_id",
        how="left"
    )
    daily_summary = build_daily_summary(events_with_region)

    summary_path = f"{config['layers']['gold']['path']}/daily_summary"
    summary_result = write_delta(
        daily_summary, summary_path,
        mode=config["layers"]["gold"]["write_mode"],
        partition_by=config["layers"]["gold"].get("partition_by"),
    )
    metrics["daily_summary"] = summary_result

    # ── Customer Lifetime Value ──
    logger.info("Building customer lifetime value...")
    clv_df = build_customer_lifetime_value(customers_df, transactions_df)

    clv_path = f"{config['layers']['gold']['path']}/customer_clv"
    clv_result = write_delta(
        clv_df, clv_path,
        mode="overwrite",
    )
    metrics["customer_clv"] = clv_result

    return metrics


if __name__ == "__main__":
    main()
