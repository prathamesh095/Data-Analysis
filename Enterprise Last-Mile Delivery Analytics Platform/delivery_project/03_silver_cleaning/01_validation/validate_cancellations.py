# Databricks notebook source
dbutils.widgets.text("batch_start_ts", "")
dbutils.widgets.text("batch_end_ts", "")

BATCH_START_TS = dbutils.widgets.get("batch_start_ts")
BATCH_END_TS = dbutils.widgets.get("batch_end_ts")

# COMMAND ----------

# MAGIC %run ../05_utils/validation_rules_library

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, count

# COMMAND ----------

TABLE_NAME = "cancellations_raw"
BRONZE_TABLE = "delivery_analytics.bronze.cancellations_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.cancellations_quarantine"

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
    "cancel_id","order_id","cancel_ts","cancel_stage",
    "cancel_actor","cancel_reason_code","cancel_reason_text",
    "refund_amount","compensation_paid","auto_cancel_flag",
    "linked_support_ticket","ingestion_ts","source_file",
    "bronze_ingestion_ts"
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
        # null cancel id
        F.sum(F.when(F.col("cancel_id").isNull(), 1).otherwise(0))
            .alias("failed_null_cancel"),

        # missing cancel_ts
        F.sum(F.when(F.col("cancel_ts").isNull(), 1).otherwise(0))
            .alias("failed_null_cancel_ts"),

        # negative refund
        F.sum(
            F.when(F.col("refund_amount") < 0, 1).otherwise(0)
        ).alias("failed_negative_refund"),

        # negative compensation
        F.sum(
            F.when(F.col("compensation_paid") < 0, 1).otherwise(0)
        ).alias("failed_negative_comp"),

        # invalid cancel actor
        F.sum(
            F.when(
                ~F.col("cancel_actor").isin(["Customer","Rider","System"]),
                1
            ).otherwise(0)
        ).alias("failed_actor_domain"),

        # suspicious refund (>200)
        F.sum(
            F.when(F.col("refund_amount") > 200, 1).otherwise(0)
        ).alias("failed_high_refund"),

        # missing support link (warning)
        F.sum(
            F.when(F.col("linked_support_ticket").isNull(), 1).otherwise(0)
        ).alias("failed_missing_support_link"),

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
    working_df.groupBy("cancel_id")
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
        col("cancel_id").isNotNull() &
        col("cancel_ts").isNotNull() &
        (col("refund_amount") >= 0) &
        (col("compensation_paid") >= 0)
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
    TABLE_NAME,"cancel_id_not_null","null_check",
    int(validation_stats["failed_null_cancel"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"cancel_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"cancel_ts_not_null","null_check",
    int(validation_stats["failed_null_cancel_ts"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"refund_non_negative","range_check",
    int(validation_stats["failed_negative_refund"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"compensation_non_negative","range_check",
    int(validation_stats["failed_negative_comp"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"cancel_actor_domain_valid","domain_check",
    int(validation_stats["failed_actor_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"refund_reasonable","range_check",
    int(validation_stats["failed_high_refund"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"support_link_present","completeness_check",
    int(validation_stats["failed_missing_support_link"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Cancellations validation completed")