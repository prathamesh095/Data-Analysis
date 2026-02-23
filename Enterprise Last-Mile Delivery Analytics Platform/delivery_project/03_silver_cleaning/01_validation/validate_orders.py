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
from pyspark.sql.functions import col, count, current_timestamp
from datetime import datetime

# COMMAND ----------



# COMMAND ----------

TABLE_NAME = "orders_raw"
BRONZE_TABLE = "delivery_analytics.bronze.orders_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.orders_quarantine"

print(f"🔍 Starting validation for {TABLE_NAME}")

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
# STEP 0 — SCHEMA DRIFT CHECK (CRITICAL GATE)
# =====================================================
expected_cols = {
    "order_id","customer_id","city","restaurant_id","delivery_zone",
    "created_ts","accepted_ts","promised_ts",
    "estimated_pickup_ts","estimated_drop_ts",
    "estimated_distance_km","estimated_duration_min",
    "order_value","tax_amount","tip_amount",
    "payment_method","payment_status","order_status",
    "priority_flag","device_type","app_version",
    "customer_rating_expected","ingestion_ts","source_file",
    "bronze_ingestion_ts"
}

actual_cols = set(full_df.columns)

schema_drift = actual_cols != expected_cols
failed_schema = 1 if schema_drift else 0

log_validation_result(
    table_name=TABLE_NAME,
    check_name="schema_exact_match",
    check_type="schema_check",
    failed_rows=failed_schema,
    total_rows=total_rows,
    severity="CRITICAL"
)

if schema_drift:
    print("🚨 SCHEMA DRIFT DETECTED — investigate immediately")

# COMMAND ----------

# =====================================================
# CHECK 0 — table not empty
# =====================================================
failed_empty = 1 if total_rows_full == 0 else 0

log_validation_result(
    table_name=TABLE_NAME,
    check_name="table_not_empty",
    check_type="table_check",
    failed_rows=failed_empty,
    total_rows=total_rows,
    severity="CRITICAL"
)

# COMMAND ----------

# =====================================================
# SINGLE PASS METRICS
# =====================================================
validation_stats = (
    working_df.select(
        F.sum(F.when(F.col("order_id").isNull(), 1).otherwise(0))
            .alias("failed_null_id"),

        F.sum(F.when(F.col("order_value") < 0, 1).otherwise(0))
            .alias("failed_negative_value"),

        F.sum(
            F.when(F.col("promised_ts") < F.col("created_ts"), 1).otherwise(0)
        ).alias("failed_bad_promise"),

        F.sum(
            F.when(F.col("accepted_ts") < F.col("created_ts"), 1).otherwise(0)
        ).alias("failed_bad_accept"),

        F.sum(
            F.when(
                ~F.col("payment_status").isin(["Paid","Pending","Failed"]),
                1
            ).otherwise(0)
        ).alias("failed_payment_domain"),

        F.max("bronze_ingestion_ts").alias("max_ingest")
    )
    .collect()[0]
)


# COMMAND ----------

# =====================================================
# FRESHNESS CHECK
# =====================================================
max_ingest = validation_stats["max_ingest"]

if max_ingest is None:
    freshness_days = 999
else:
    freshness_days = (datetime.utcnow() - max_ingest).days

failed_freshness = 1 if freshness_days > 2 else 0


# COMMAND ----------

# =====================================================
# DUPLICATE CHECK (separate heavy check)
# =====================================================
dup_df = (
    working_df.groupBy("order_id")
      .agg(count("*").alias("cnt"))
      .filter(col("cnt") > 1)
)

failed_duplicates = dup_df.count()

# COMMAND ----------

# =====================================================
# 🧪 ELITE FEATURE — QUALITY FLAG
# =====================================================
working_df_with_flags = working_df.withColumn(
    "is_valid",
    (
        col("order_id").isNotNull() &
        (col("order_value") >= 0) &
        (col("promised_ts") >= col("created_ts"))
    )
)

# COMMAND ----------

# =====================================================
# 🧪 ELITE FEATURE — QUARANTINE BAD RECORDS
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
    TABLE_NAME,"order_id_not_null","null_check",
    int(validation_stats["failed_null_id"]), total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"order_id_unique","duplicate_check",
    int(failed_duplicates), total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"order_value_positive","range_check",
    int(validation_stats["failed_negative_value"]), total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"promised_after_created","timestamp_check",
    int(validation_stats["failed_bad_promise"]), total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"accepted_after_created","timestamp_check",
    int(validation_stats["failed_bad_accept"]), total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"payment_status_domain_valid","domain_check",
    int(validation_stats["failed_payment_domain"]), total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_2_days","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

# COMMAND ----------

# volume guardrail
failed_volume = 1 if total_rows_full < 100 else 0

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Orders validation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delivery_analytics.silver.bronze_validation_results
# MAGIC WHERE table_name = 'orders_raw'
# MAGIC ORDER BY run_ts DESC;