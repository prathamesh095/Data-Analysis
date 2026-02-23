# Databricks notebook source
# =====================================================
# BATCH CONTEXT (Incremental Validation)
# =====================================================

dbutils.widgets.text("batch_start_ts", "")
dbutils.widgets.text("batch_end_ts", "")

BATCH_START_TS = dbutils.widgets.get("batch_start_ts")
BATCH_END_TS = dbutils.widgets.get("batch_end_ts")

print(f"🧩 Batch window: {BATCH_START_TS} → {BATCH_END_TS}")

# COMMAND ----------

# MAGIC %run ../05_utils/validation_rules_library

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, count

# COMMAND ----------



# COMMAND ----------

TABLE_NAME = "shifts_raw"
BRONZE_TABLE = "delivery_analytics.bronze.shifts_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.shifts_quarantine"

print(f"🔍 Starting ELITE validation for {TABLE_NAME}")

# COMMAND ----------

full_df = spark.table(BRONZE_TABLE)

if BATCH_START_TS and BATCH_END_TS:
    print("⚡ Incremental validation mode")

    working_df = (
        full_df.filter(
            (F.col("bronze_ingestion_ts") >= F.to_timestamp(F.lit(BATCH_START_TS), "yyyyMMddHHmmss")) &
            (F.col("bronze_ingestion_ts") <  F.to_timestamp(F.lit(BATCH_END_TS), "yyyyMMddHHmmss"))
        )
    )
else:
    print("📦 Full validation mode")
    working_df = full_df

total_rows_full = full_df.count()
total_rows = working_df.count()

print(f"📊 Full table rows: {total_rows_full}")
print(f"📊 Batch rows: {total_rows}")

# COMMAND ----------

# =====================================================
# STEP 0 — SCHEMA DRIFT CHECK
# =====================================================
expected_cols = {
    "shift_id","rider_id","city",
    "shift_start_ts","shift_end_ts","scheduled_hours",
    "actual_login_ts","actual_logout_ts",
    "shift_status","late_start_flag","early_logout_flag",
    "overtime_flag","shift_type","manager_id",
    "ingestion_ts","source_file","bronze_ingestion_ts"
}

actual_cols = set(full_df.columns)
schema_drift = actual_cols != expected_cols
failed_schema = 1 if schema_drift else 0

log_validation_result(
    TABLE_NAME,
    "schema_exact_match",
    "schema_check",
    failed_schema,
    total_rows,
    "CRITICAL"
)

if schema_drift:
    print("🚨 SCHEMA DRIFT DETECTED — investigate immediately")

# COMMAND ----------

# =====================================================
# CHECK — table not empty
# =====================================================
failed_empty = 1 if total_rows_full == 0 else 0

log_validation_result(
    TABLE_NAME,
    "table_not_empty",
    "table_check",
    failed_empty,
    total_rows,
    "CRITICAL"
)

# COMMAND ----------

# =====================================================
# SINGLE PASS METRICS
# =====================================================
validation_stats = (
    working_df.select(
        # null shift id
        F.sum(F.when(F.col("shift_id").isNull(), 1).otherwise(0))
            .alias("failed_null_shift"),

        # null rider id
        F.sum(F.when(F.col("rider_id").isNull(), 1).otherwise(0))
            .alias("failed_null_rider"),

        # end before start
        F.sum(
            F.when(F.col("shift_end_ts") < F.col("shift_start_ts"), 1).otherwise(0)
        ).alias("failed_time_order"),

        # scheduled hours invalid
        F.sum(
            F.when(F.col("scheduled_hours") <= 0, 1).otherwise(0)
        ).alias("failed_scheduled_hours"),

        # invalid shift status
        F.sum(
            F.when(
                ~F.col("shift_status").isin(["Completed","Cancelled"]),
                1
            ).otherwise(0)
        ).alias("failed_status_domain"),

        # very long shifts (>14h)
        F.sum(
            F.when(F.col("scheduled_hours") > 14, 1).otherwise(0)
        ).alias("failed_long_shift"),

        # logout before login
        F.sum(
            F.when(
                F.col("actual_logout_ts") < F.col("actual_login_ts"),
                1
            ).otherwise(0)
        ).alias("failed_login_logout_order"),

        # freshness
        F.max("bronze_ingestion_ts").alias("max_ingest")
    )
    .collect()[0]
)

# COMMAND ----------

# =====================================================
# FRESHNESS + VOLUME
# =====================================================
failed_freshness = compute_freshness_failure(validation_stats["max_ingest"])
failed_volume = compute_volume_failure(total_rows)

# COMMAND ----------

# =====================================================
# DUPLICATE CHECK
# =====================================================
dup_df = (
    working_df.groupBy("shift_id")
      .agg(count("*").alias("cnt"))
      .filter(col("cnt") > 1)
)

failed_duplicates = dup_df.count()

# COMMAND ----------

# =====================================================
# ELITE — QUALITY FLAG
# =====================================================
working_df_with_flags = working_df.withColumn(
    "is_valid",
    (
        col("shift_id").isNotNull() &
        col("rider_id").isNotNull() &
        (col("scheduled_hours") > 0) &
        (col("shift_end_ts") >= col("shift_start_ts"))
    )
)

# COMMAND ----------

# =====================================================
# ELITE — QUARANTINE
# =====================================================
bad_df = working_df_with_flags.filter(col("is_valid") == False)

if bad_df.limit(1).count() > 0:
    print(f"🚑 Quarantining bad records to {QUARANTINE_TABLE}")

    if not spark.catalog.tableExists(QUARANTINE_TABLE):
        bad_df.limit(0).write.format("delta").saveAsTable(QUARANTINE_TABLE)

    bad_df.write.mode("append").saveAsTable(QUARANTINE_TABLE)

# COMMAND ----------

# =====================================================
# LOGGING SECTION
# =====================================================
log_validation_result(
    TABLE_NAME,"shift_id_not_null","null_check",
    int(validation_stats["failed_null_shift"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"shift_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"rider_id_not_null","null_check",
    int(validation_stats["failed_null_rider"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"shift_time_order_valid","timestamp_check",
    int(validation_stats["failed_time_order"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"scheduled_hours_positive","range_check",
    int(validation_stats["failed_scheduled_hours"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"shift_status_domain_valid","domain_check",
    int(validation_stats["failed_status_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"shift_length_reasonable","range_check",
    int(validation_stats["failed_long_shift"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"login_logout_order_valid","timestamp_check",
    int(validation_stats["failed_login_logout_order"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Shifts validation completed")