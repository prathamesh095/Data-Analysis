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
from pyspark.sql.functions import col, count, current_date


# COMMAND ----------



# COMMAND ----------

TABLE_NAME = "riders_raw"
BRONZE_TABLE = "delivery_analytics.bronze.riders_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.riders_quarantine"

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
    "rider_id","rider_name","city","home_zone","join_date",
    "vehicle_type","license_verified_flag","background_check_flag",
    "employment_type","status","rating_avg",
    "total_deliveries_lifetime","last_active_ts",
    "device_os","device_model","bank_account_verified",
    "incentive_tier","ingestion_ts","source_file",
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
        # null rider id
        F.sum(F.when(F.col("rider_id").isNull(), 1).otherwise(0))
            .alias("failed_null_rider"),

        # rating bounds
        F.sum(
            F.when(
                (F.col("rating_avg") < 1) |
                (F.col("rating_avg") > 5),
                1
            ).otherwise(0)
        ).alias("failed_rating_bounds"),

        # future join date
        F.sum(
            F.when(F.col("join_date") > current_date(), 1).otherwise(0)
        ).alias("failed_future_join"),

        # invalid employment type
        F.sum(
            F.when(
                ~F.col("employment_type").isin(["Full-time","Gig"]),
                1
            ).otherwise(0)
        ).alias("failed_employment_domain"),

        # invalid status
        F.sum(
            F.when(
                ~F.col("status").isin(["Active","Inactive"]),
                1
            ).otherwise(0)
        ).alias("failed_status_domain"),

        # suspicious lifetime deliveries
        F.sum(
            F.when(F.col("total_deliveries_lifetime") < 0, 1).otherwise(0)
        ).alias("failed_negative_lifetime"),

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
    working_df.groupBy("rider_id")
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
        col("rider_id").isNotNull() &
        (col("rating_avg").between(1,5)) &
        (col("join_date") <= current_date())
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
    TABLE_NAME,"rider_id_not_null","null_check",
    int(validation_stats["failed_null_rider"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"rider_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"rating_bounds_valid","range_check",
    int(validation_stats["failed_rating_bounds"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"join_date_not_future","timestamp_check",
    int(validation_stats["failed_future_join"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"employment_type_domain_valid","domain_check",
    int(validation_stats["failed_employment_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"status_domain_valid","domain_check",
    int(validation_stats["failed_status_domain"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"lifetime_deliveries_non_negative","range_check",
    int(validation_stats["failed_negative_lifetime"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Riders validation completed")