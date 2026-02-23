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

TABLE_NAME = "support_raw"
BRONZE_TABLE = "delivery_analytics.bronze.support_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.support_quarantine"

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
    "ticket_id","order_id","customer_id","created_ts",
    "issue_category","issue_subcategory","priority_level",
    "ticket_channel","agent_id","resolution_status",
    "resolution_ts","compensation_amount","csat_score",
    "reopened_flag","notes_text",
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
        # null ticket id
        F.sum(F.when(F.col("ticket_id").isNull(), 1).otherwise(0))
            .alias("failed_null_ticket"),

        # resolution before creation
        F.sum(
            F.when(
                (F.col("resolution_ts").isNotNull()) &
                (F.col("resolution_ts") < F.col("created_ts")),
                1
            ).otherwise(0)
        ).alias("failed_bad_resolution_time"),

        # csat bounds
        F.sum(
            F.when(
                (F.col("csat_score") < 1) |
                (F.col("csat_score") > 5),
                1
            ).otherwise(0)
        ).alias("failed_csat_bounds"),

        # invalid resolution status
        F.sum(
            F.when(
                ~F.col("resolution_status").isin(["Refunded","Resolved","Open"]),
                1
            ).otherwise(0)
        ).alias("failed_resolution_domain"),

        # invalid priority
        F.sum(
            F.when(
                ~F.col("priority_level").isin(["Low","Medium","High"]),
                1
            ).otherwise(0)
        ).alias("failed_priority_domain"),

        # missing agent (warning)
        F.sum(
            F.when(F.col("agent_id").isNull(), 1).otherwise(0)
        ).alias("failed_missing_agent"),

        # high compensation (>200)
        F.sum(
            F.when(F.col("compensation_amount") > 200, 1).otherwise(0)
        ).alias("failed_high_compensation"),

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
    working_df.groupBy("ticket_id")
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
        col("ticket_id").isNotNull() &
        col("created_ts").isNotNull() &
        (
            col("resolution_ts").isNull() |
            (col("resolution_ts") >= col("created_ts"))
        )
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
    TABLE_NAME,"ticket_id_not_null","null_check",
    int(validation_stats["failed_null_ticket"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"ticket_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"resolution_time_valid","timestamp_check",
    int(validation_stats["failed_bad_resolution_time"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"csat_bounds_valid","range_check",
    int(validation_stats["failed_csat_bounds"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"resolution_status_domain_valid","domain_check",
    int(validation_stats["failed_resolution_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"priority_domain_valid","domain_check",
    int(validation_stats["failed_priority_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"agent_present","completeness_check",
    int(validation_stats["failed_missing_agent"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"compensation_reasonable","range_check",
    int(validation_stats["failed_high_compensation"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Support validation completed")