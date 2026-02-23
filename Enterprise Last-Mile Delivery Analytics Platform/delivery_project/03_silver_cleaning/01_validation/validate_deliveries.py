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
from datetime import datetime

# COMMAND ----------

TABLE_NAME = "deliveries_raw"
BRONZE_TABLE = "delivery_analytics.bronze.deliveries_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.deliveries_quarantine"

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
# STEP 0 — SCHEMA DRIFT CHECK (CRITICAL)
# =====================================================
expected_cols = {
    "delivery_id","order_id","rider_id",
    "dispatch_ts","arrival_at_pickup_ts","pickup_ts",
    "departure_from_pickup_ts","arrival_at_drop_ts","drop_ts",
    "delivery_status","failure_reason",
    "distance_km_actual","duration_min_actual",
    "traffic_delay_min","weather_flag",
    "proof_of_delivery_flag","customer_rating","rider_rating",
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
# CHECK 0 — table not empty
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
        # null delivery id
        F.sum(F.when(F.col("delivery_id").isNull(), 1).otherwise(0))
            .alias("failed_null_delivery"),

        # pickup before dispatch
        F.sum(
            F.when(F.col("pickup_ts") < F.col("dispatch_ts"), 1).otherwise(0)
        ).alias("failed_pickup_order"),

        # drop before pickup (only when drop exists)
        F.sum(
            F.when(
                (F.col("drop_ts").isNotNull()) &
                (F.col("drop_ts") < F.col("pickup_ts")),
                1
            ).otherwise(0)
        ).alias("failed_drop_order"),

        # negative distance
        F.sum(
            F.when(F.col("distance_km_actual") <= 0, 1).otherwise(0)
        ).alias("failed_distance"),

        # duration unreasonable
        F.sum(
            F.when(
                (F.col("duration_min_actual") <= 0) |
                (F.col("duration_min_actual") > 180),
                1
            ).otherwise(0)
        ).alias("failed_duration"),

        # rating bounds
        F.sum(
            F.when(
                (F.col("customer_rating") < 1) |
                (F.col("customer_rating") > 5) |
                (F.col("rider_rating") < 1) |
                (F.col("rider_rating") > 5),
                1
            ).otherwise(0)
        ).alias("failed_rating_bounds"),

        # freshness
        F.max("bronze_ingestion_ts").alias("max_ingest")
    )
    .collect()[0]
)


# COMMAND ----------

# =====================================================
# FRESHNESS + VOLUME (from utils)
# =====================================================
failed_freshness = compute_freshness_failure(validation_stats["max_ingest"])
failed_volume = compute_volume_failure(total_rows)

# COMMAND ----------

# =====================================================
# DUPLICATE CHECK
# =====================================================
dup_df = (
    working_df.groupBy("delivery_id")
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
        col("delivery_id").isNotNull() &
        (col("distance_km_actual") > 0) &
        (col("duration_min_actual") > 0) &
        (col("pickup_ts") >= col("dispatch_ts"))
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
    TABLE_NAME,"delivery_id_not_null","null_check",
    int(validation_stats["failed_null_delivery"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"delivery_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"pickup_after_dispatch","timestamp_check",
    int(validation_stats["failed_pickup_order"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"drop_after_pickup","timestamp_check",
    int(validation_stats["failed_drop_order"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"distance_positive","range_check",
    int(validation_stats["failed_distance"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"duration_reasonable","range_check",
    int(validation_stats["failed_duration"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"rating_bounds_valid","range_check",
    int(validation_stats["failed_rating_bounds"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Deliveries validation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delivery_analytics.silver.bronze_validation_results
# MAGIC WHERE table_name = 'deliveries_raw'
# MAGIC ORDER BY run_ts DESC;