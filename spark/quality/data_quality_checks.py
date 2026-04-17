"""
data_quality_checks.py
-----------------------
Production-grade data quality checks using PySpark.
Inspired by Great Expectations — returns structured results for logging.

Usage:
    from spark.quality.data_quality_checks import DataQualityChecker
    checker = DataQualityChecker(spark, df)
    report = checker.run_all()
"""

import logging
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("data_quality")

# ─── CHECK RESULT ─────────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    check_name:     str
    table_name:     str
    column_name:    Optional[str]
    check_type:     str
    total_records:  int
    passed_records: int
    failed_records: int
    pass_rate:      float
    threshold:      float               # minimum acceptable pass_rate
    status:         str                 # PASS | WARN | FAIL
    details:        str = ""
    run_timestamp:  str = field(default_factory=lambda: datetime.utcnow().isoformat())

    @property
    def is_ok(self) -> bool:
        return self.status in ("PASS", "WARN")


@dataclass
class QualityReport:
    table_name:     str
    run_timestamp:  str
    checks:         List[CheckResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.status != "FAIL" for c in self.checks)

    @property
    def summary(self) -> str:
        total = len(self.checks)
        n_pass = sum(1 for c in self.checks if c.status == "PASS")
        n_warn = sum(1 for c in self.checks if c.status == "WARN")
        n_fail = sum(1 for c in self.checks if c.status == "FAIL")
        return (f"Quality Report [{self.table_name}] | "
                f"PASS={n_pass}/{total} WARN={n_warn} FAIL={n_fail}")


# ─── CHECKER ──────────────────────────────────────────────────────────────────

class DataQualityChecker:
    """
    Runs a configurable suite of data quality checks on a PySpark DataFrame.
    All checks return CheckResult objects — no side effects.
    """

    def __init__(self, spark: SparkSession, df: DataFrame, table_name: str = "unknown"):
        self.spark = spark
        self.df = df
        self.table_name = table_name
        self.total = df.count()
        log.info(f"DQ Checker initialized | table={table_name} | rows={self.total:,}")

    def _make_result(self, check_name: str, col: Optional[str], check_type: str,
                     failed: int, threshold: float, details: str = "") -> CheckResult:
        passed = self.total - failed
        pass_rate = passed / self.total if self.total > 0 else 0.0

        if pass_rate >= threshold:
            status = "PASS"
        elif pass_rate >= threshold * 0.95:
            status = "WARN"   # within 5% of threshold → warning
        else:
            status = "FAIL"

        result = CheckResult(
            check_name=check_name,
            table_name=self.table_name,
            column_name=col,
            check_type=check_type,
            total_records=self.total,
            passed_records=passed,
            failed_records=failed,
            pass_rate=round(pass_rate, 6),
            threshold=threshold,
            status=status,
            details=details,
        )
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[status]
        log.info(f"{icon} [{status}] {check_name}: {passed:,}/{self.total:,} "
                 f"({pass_rate:.2%}) threshold={threshold:.0%}")
        return result

    # ── Individual Checks ─────────────────────────────────────────────────────

    def check_null_rate(self, column: str, threshold: float = 0.99) -> CheckResult:
        """Check that at least `threshold` fraction of values are non-null."""
        failed = self.df.filter(F.col(column).isNull()).count()
        return self._make_result(
            f"null_check_{column}", column, "null_check",
            failed, threshold,
            details=f"{failed:,} null values in column '{column}'"
        )

    def check_uniqueness(self, column: str, threshold: float = 1.0) -> CheckResult:
        """Check that values in `column` are unique (no duplicates)."""
        total_distinct = self.df.select(column).distinct().count()
        duplicates = self.total - total_distinct
        return self._make_result(
            f"uniqueness_{column}", column, "uniqueness_check",
            duplicates, threshold,
            details=f"{duplicates:,} duplicate values in '{column}'"
        )

    def check_value_in_set(self, column: str, valid_set: set,
                           threshold: float = 0.99) -> CheckResult:
        """Check that column values belong to an allowed set."""
        failed = self.df.filter(~F.col(column).isin(list(valid_set))).count()
        return self._make_result(
            f"value_set_{column}", column, "value_set_check",
            failed, threshold,
            details=f"Valid values: {valid_set}"
        )

    def check_range(self, column: str, min_val=None, max_val=None,
                    threshold: float = 0.99) -> CheckResult:
        """Check that numeric column values are within [min_val, max_val]."""
        cond = F.lit(False)
        if min_val is not None:
            cond = cond | (F.col(column) < min_val)
        if max_val is not None:
            cond = cond | (F.col(column) > max_val)
        failed = self.df.filter(cond).count()
        return self._make_result(
            f"range_{column}", column, "range_check",
            failed, threshold,
            details=f"Expected [{min_val}, {max_val}]"
        )

    def check_referential_integrity(self, column: str, ref_df: DataFrame,
                                    ref_col: str, threshold: float = 0.99) -> CheckResult:
        """Check that values in `column` exist in `ref_df[ref_col]`."""
        ref_keys = ref_df.select(F.col(ref_col).alias("_ref")).distinct()
        failed = (
            self.df.join(ref_keys, self.df[column] == ref_keys["_ref"], how="left")
            .filter(F.col("_ref").isNull())
            .count()
        )
        return self._make_result(
            f"ref_integrity_{column}_{ref_col}", column, "referential_integrity",
            failed, threshold,
            details=f"Column '{column}' → ref '{ref_col}'"
        )

    def check_date_format(self, column: str, fmt: str = "yyyy-MM-dd",
                          threshold: float = 0.99) -> CheckResult:
        """Check that date strings parse correctly."""
        failed = (
            self.df
            .filter(F.col(column).isNotNull())
            .filter(F.to_date(F.col(column), fmt).isNull())
            .count()
        )
        return self._make_result(
            f"date_format_{column}", column, "date_format_check",
            failed, threshold,
            details=f"Expected format: {fmt}"
        )

    def check_completeness(self, columns: list, threshold: float = 0.95) -> CheckResult:
        """
        Check that a row has no nulls across ALL specified columns.
        Useful for critical composite fields.
        """
        cond = F.lit(False)
        for col in columns:
            cond = cond | F.col(col).isNull()
        failed = self.df.filter(cond).count()
        return self._make_result(
            f"completeness_{'_'.join(columns[:3])}", None, "completeness_check",
            failed, threshold,
            details=f"All of {columns} must be non-null"
        )

    # ── Run All (Preset for POS Data) ─────────────────────────────────────────

    def run_all_pos(self) -> QualityReport:
        """Run the full DQ suite for POS sales data."""
        report = QualityReport(
            table_name=self.table_name,
            run_timestamp=datetime.utcnow().isoformat()
        )
        report.checks.extend([
            # Critical fields — zero tolerance for nulls
            self.check_null_rate("transaction_id",   threshold=1.00),
            self.check_null_rate("product_id",       threshold=1.00),
            self.check_null_rate("store_id",         threshold=1.00),
            self.check_null_rate("transaction_date", threshold=1.00),

            # Soft nulls — allow small % missing
            self.check_null_rate("product_name",     threshold=0.95),
            self.check_null_rate("customer_id",      threshold=0.70),  # anonymous ok

            # Uniqueness
            self.check_uniqueness("transaction_id",  threshold=0.99),

            # Ranges
            self.check_range("quantity",   min_val=1, max_val=10_000, threshold=0.99),
            self.check_range("unit_price", min_val=0.01, max_val=100_000, threshold=0.99),
            self.check_range("discount",   min_val=0.0,  max_val=1.0,   threshold=0.99),

            # Value sets
            self.check_value_in_set(
                "payment_method",
                {"Cash", "Credit", "Debit", "UPI", "Gift Card"},
                threshold=0.98
            ),

            # Date format
            self.check_date_format("transaction_date", threshold=1.00),

            # Completeness
            self.check_completeness(
                ["transaction_id", "product_id", "store_id", "quantity", "unit_price"],
                threshold=0.99
            ),
        ])

        log.info(report.summary)
        return report


# ─── STANDALONE USAGE ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("DQ_Checks").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Example: run checks on sample data
    sample_path = "data/sample/pos_sales.csv"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(sample_path)

    checker = DataQualityChecker(spark, df, table_name="pos_sales_raw")
    report = checker.run_all_pos()

    print(f"\n{'='*60}")
    print(report.summary)
    print(f"Overall Status: {'✅ PASS' if report.passed else '❌ FAIL'}")
    print('='*60)

    for check in report.checks:
        print(f"  [{check.status:4s}] {check.check_name}: "
              f"{check.pass_rate:.2%} (threshold: {check.threshold:.0%}) "
              f"— {check.details}")

    spark.stop()
