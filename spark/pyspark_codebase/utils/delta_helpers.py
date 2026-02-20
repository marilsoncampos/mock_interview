"""
delta_helpers.py — Delta Lake read/write utilities.

Wraps Delta Lake operations (merge, overwrite, time travel) into
reusable functions. Think of Delta as a "database on top of a data lake" —
it gives you ACID transactions, schema enforcement, and versioning on
plain Parquet files.
"""

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable


def read_delta(spark: SparkSession, path: str, version: Optional[int] = None) -> DataFrame:
    """Read a Delta table, optionally at a specific version (time travel).

    Args:
        spark: Active SparkSession.
        path: S3/HDFS path to the Delta table.
        version: Optional version number for time travel.

    Returns:
        DataFrame with the table contents.
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[List[str]] = None,
    merge_key: Optional[str] = None,
    merge_columns: Optional[List[str]] = None,
) -> dict:
    """Write a DataFrame to a Delta table with configurable strategy.

    Supports three modes:
      - "append": Add rows to the table. Fast, no dedup.
      - "overwrite": Replace the table or partition. Clean but destructive.
      - "merge": Upsert — update existing rows, insert new ones.

    Args:
        df: DataFrame to write.
        path: Target Delta table path.
        mode: Write strategy ("append", "overwrite", "merge").
        partition_by: Columns to partition by.
        merge_key: Column for merge matching (required if mode="merge").
        merge_columns: Columns to update on match. If None, updates all.

    Returns:
        Dict with write metrics (rows_written, mode, etc.)
    """
    row_count = df.count()

    if mode == "merge":
        if not merge_key:
            raise ValueError("merge_key is required for merge mode")
        return _delta_merge(df, path, merge_key, merge_columns)

    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)

    return {"rows_written": row_count, "mode": mode, "path": path}


def _delta_merge(
    source_df: DataFrame,
    target_path: str,
    merge_key: str,
    merge_columns: Optional[List[str]] = None,
) -> dict:
    """Perform a Delta MERGE (upsert) operation.

    Equivalent to SQL:
        MERGE INTO target USING source
        ON target.key = source.key
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *

    Args:
        source_df: New/updated rows.
        target_path: Path to existing Delta table.
        merge_key: Column to match rows on.
        merge_columns: Specific columns to update. None = update all.

    Returns:
        Dict with merge metrics.
    """
    spark = source_df.sparkSession

    # Check if table exists; if not, just write it
    try:
        target_table = DeltaTable.forPath(spark, target_path)
    except Exception:
        # Table doesn't exist yet — create it
        source_df.write.format("delta").save(target_path)
        return {
            "rows_inserted": source_df.count(),
            "rows_updated": 0,
            "mode": "merge (initial write)",
            "path": target_path,
        }

    merge_condition = f"target.{merge_key} = source.{merge_key}"

    merge_builder = (
        target_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
    )

    if merge_columns:
        update_set = {col: f"source.{col}" for col in merge_columns}
        merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
    else:
        merge_builder = merge_builder.whenMatchedUpdateAll()

    merge_builder = merge_builder.whenNotMatchedInsertAll()

    # BUG: No whenNotMatchedBySourceDelete() — if a customer is deleted
    # from the source system, they persist forever in the target.
    # This is a "soft delete" gap that will cause count mismatches
    # between source and target over time.

    merge_builder.execute()

    return {
        "mode": "merge",
        "merge_key": merge_key,
        "path": target_path,
    }


def compact_table(spark: SparkSession, path: str, target_file_size: str = "128mb") -> None:
    """Run OPTIMIZE (compaction) on a Delta table to reduce small files.

    Small files are the enemy of query performance on data lakes.
    This is like defragmenting a hard drive — it combines many tiny
    files into fewer, optimally-sized ones.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        target_file_size: Target output file size.
    """
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()


def vacuum_table(spark: SparkSession, path: str, retention_hours: int = 168) -> None:
    """Remove old versions of a Delta table to reclaim storage.

    WARNING: After vacuum, time travel to versions older than the
    retention period will no longer work.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        retention_hours: Hours of history to retain (default: 7 days).
    """
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)
