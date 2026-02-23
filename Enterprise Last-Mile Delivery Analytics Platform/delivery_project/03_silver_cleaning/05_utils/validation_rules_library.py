# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql import functions as F

# COMMAND ----------

# =====================================================
# GLOBAL CONFIG (make your platform configurable)
# =====================================================
RESULT_TABLE = "delivery_analytics.silver.bronze_validation_results"

# Thresholds (tunable without touching validators)
FRESHNESS_THRESHOLD_DAYS = 2
MIN_VOLUME_THRESHOLD = 100

print("✅ Validation rules library loaded")

# COMMAND ----------

# =====================================================
# INTERNAL HELPERS
# =====================================================
def _normalize_table_name(name: str) -> str:
    """
    Prevent whitespace / casing issues.
    Fixes bugs like 'orders_ra w'
    """
    if name is None:
        return "unknown_table"
    return name.strip().lower()

def _safe_int(val):
    """Ensure numeric stability."""
    try:
        return int(val)
    except Exception:
        return 0


# COMMAND ----------


# =====================================================
# CORE LOGGER (single source of truth)
# =====================================================
def log_validation_result(
    table_name: str,
    check_name: str,
    check_type: str,
    failed_rows: int,
    total_rows: int,
    severity: str = "ERROR"
):
    """
    Centralized validation logger.
    One row per check.
    """

    table_name = _normalize_table_name(table_name)
    status = "PASS" if _safe_int(failed_rows) == 0 else "FAIL"

    result_df = spark.createDataFrame(
        [(
            table_name,
            check_name,
            check_type,
            _safe_int(failed_rows),
            _safe_int(total_rows),
            status,
            severity.upper()
        )],
        [
            "table_name",
            "check_name",
            "check_type",
            "failed_rows",
            "total_rows",
            "status",
            "severity"
        ]
    ).withColumn("run_ts", current_timestamp())

    result_df.write.mode("append").saveAsTable(RESULT_TABLE)

    print(
        f"📋 {table_name} | {check_name} | {status} "
        f"(failed={failed_rows}, total={total_rows})"
    )

# COMMAND ----------

# =====================================================
# REUSABLE CHECK HELPERS (used by elite validators)
# =====================================================
def compute_freshness_failure(max_ingest_ts):
    """
    Returns 1 if freshness breached else 0.
    """
    from datetime import datetime

    if max_ingest_ts is None:
        return 1

    freshness_days = (datetime.utcnow() - max_ingest_ts).days
    return 1 if freshness_days > FRESHNESS_THRESHOLD_DAYS else 0

def compute_volume_failure(total_rows: int):
    """
    Volume guardrail.
    """
    return 1 if total_rows < MIN_VOLUME_THRESHOLD else 0

print("🏁 Validation utilities ready")