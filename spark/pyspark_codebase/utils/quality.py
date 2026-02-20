"""
quality.py — Data quality validation framework.

Runs configurable checks against DataFrames and routes failing records
to a quarantine zone. Think of this as the "quality inspector" on the
assembly line — bad parts get pulled off the belt before they reach
the customer.
"""

from datetime import datetime
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


class QualityCheck:
    """A single data quality rule."""

    def __init__(self, name: str, column: str, rule: str, severity: str = "warning", **kwargs):
        self.name = name
        self.column = column
        self.rule = rule
        self.severity = severity
        self.params = kwargs

    def build_filter(self) -> str:
        """Convert this check into a SQL WHERE clause that matches FAILING rows."""
        if self.rule == "not_null":
            return f"`{self.column}` IS NULL"

        elif self.rule == "regex":
            pattern = self.params.get("pattern", ".*")
            return f"`{self.column}` NOT RLIKE '{pattern}'"

        elif self.rule == "greater_than":
            value = self.params.get("value", 0)
            return f"`{self.column}` <= {value} OR `{self.column}` IS NULL"

        elif self.rule == "less_than_or_equal":
            value = self.params.get("value", "0")
            return f"`{self.column}` > {value}"

        elif self.rule == "unique":
            # NOTE: uniqueness can't be expressed as a row-level filter.
            # This is handled separately in validate().
            return None

        else:
            raise ValueError(f"Unknown quality rule: {self.rule}")


class QualityEngine:
    """Orchestrates quality checks on DataFrames.

    Usage:
        engine = QualityEngine(spark, config["quality"])
        clean_df, report = engine.validate(df)
    """

    def __init__(self, spark: SparkSession, quality_config: dict):
        self.spark = spark
        self.quarantine_path = quality_config.get("quarantine_path")
        self.fail_on_error = quality_config.get("fail_on_error", False)

        self.checks = []
        for check_def in quality_config.get("checks", []):
            self.checks.append(QualityCheck(
                name=check_def["name"],
                column=check_def["column"],
                rule=check_def["rule"],
                severity=check_def.get("severity", "warning"),
                **{k: v for k, v in check_def.items()
                   if k not in ("name", "column", "rule", "severity")}
            ))

    def validate(self, df: DataFrame, source_name: str = "unknown") -> Tuple[DataFrame, List[dict]]:
        """Run all quality checks against a DataFrame.

        Returns:
            Tuple of (clean_df, report) where:
            - clean_df has failing rows removed for critical checks
            - report is a list of check results with pass/fail counts

        Strategy:
            1. For each check, find failing rows
            2. Critical failures → remove from DataFrame, send to quarantine
            3. Warnings → tag but keep in DataFrame
            4. Generate a summary report
        """
        report = []
        quarantine_frames = []
        total_rows = df.count()  # BUG: expensive action called upfront; triggers full scan

        for check in self.checks:
            filter_expr = check.build_filter()

            if filter_expr is None:
                # Handle uniqueness check separately
                if check.rule == "unique":
                    dupes = df.groupBy(check.column).count().filter("count > 1")
                    dupe_count = dupes.count()
                    report.append({
                        "check": check.name,
                        "severity": check.severity,
                        "total_rows": total_rows,
                        "failing_rows": dupe_count,
                        "pass_rate": round((1 - dupe_count / max(total_rows, 1)) * 100, 2),
                    })
                continue

            # Find rows that FAIL this check
            failing_df = df.filter(filter_expr)
            failing_count = failing_df.count()  # BUG: another action per check — N scans for N checks

            pass_rate = round((1 - failing_count / max(total_rows, 1)) * 100, 2)

            report.append({
                "check": check.name,
                "severity": check.severity,
                "total_rows": total_rows,
                "failing_rows": failing_count,
                "pass_rate": pass_rate,
            })

            if failing_count > 0 and check.severity == "critical":
                # Tag and quarantine failing rows
                tagged = failing_df.withColumn(
                    "_quality_check", F.lit(check.name)
                ).withColumn(
                    "_quarantine_ts", F.lit(datetime.utcnow().isoformat())
                )
                quarantine_frames.append(tagged)

                # Remove failing rows from main DataFrame
                df = df.filter(f"NOT ({filter_expr})")

        # Write quarantined records
        if quarantine_frames and self.quarantine_path:
            quarantined = quarantine_frames[0]
            for frame in quarantine_frames[1:]:
                # BUG: union requires matching schemas. If different checks
                # fail on different columns, the schemas won't match and
                # this will throw an error at runtime.
                quarantined = quarantined.union(frame)

            quarantined.write.mode("append").partitionBy("_quality_check").parquet(
                f"{self.quarantine_path}/{source_name}/"
            )

        # Raise if critical checks failed and fail_on_error is set
        critical_failures = [r for r in report if r["severity"] == "critical" and r["failing_rows"] > 0]
        if critical_failures and self.fail_on_error:
            raise DataQualityError(
                f"{len(critical_failures)} critical quality check(s) failed",
                report=report,
            )

        return df, report


class DataQualityError(Exception):
    """Raised when critical quality checks fail and fail_on_error is True."""

    def __init__(self, message: str, report: List[dict] = None):
        super().__init__(message)
        self.report = report or []
