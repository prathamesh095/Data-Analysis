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

TABLE_NAME = "pings_raw"
BRONZE_TABLE = "delivery_analytics.bronze.pings_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.pings_quarantine"

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
    "ping_id","rider_id","delivery_id",
    "lat","lon","speed_kmh","heading",
    "accuracy_meters","battery_level",
    "network_type","ping_ts","gps_provider",
    "is_mock_location_flag",
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
        # null ping id
        F.sum(F.when(F.col("ping_id").isNull(), 1).otherwise(0))
            .alias("failed_null_ping"),

        # invalid latitude
        F.sum(
            F.when(
                (F.col("lat") < -90) | (F.col("lat") > 90),
                1
            ).otherwise(0)
        ).alias("failed_bad_lat"),

        # invalid longitude
        F.sum(
            F.when(
                (F.col("lon") < -180) | (F.col("lon") > 180),
                1
            ).otherwise(0)
        ).alias("failed_bad_lon"),

        # unreasonable speed (>120 km/h)
        F.sum(
            F.when(
                (F.col("speed_kmh") < 0) |
                (F.col("speed_kmh") > 120),
                1
            ).otherwise(0)
        ).alias("failed_speed_bounds"),

        # battery out of range
        F.sum(
            F.when(
                (F.col("battery_level") < 0) |
                (F.col("battery_level") > 100),
                1
            ).otherwise(0)
        ).alias("failed_battery_bounds"),

        # missing timestamp
        F.sum(
            F.when(F.col("ping_ts").isNull(), 1).otherwise(0)
        ).alias("failed_null_ping_ts"),

        # poor GPS accuracy (>100m)
        F.sum(
            F.when(F.col("accuracy_meters") > 100, 1).otherwise(0)
        ).alias("failed_poor_accuracy"),

        # mock location usage
        F.sum(
            F.when(F.col("is_mock_location_flag") == True, 1).otherwise(0)
        ).alias("failed_mock_location"),

        # invalid network type
        F.sum(
            F.when(
                ~F.col("network_type").isin(["4G","5G","WiFi"]),
                1
            ).otherwise(0)
        ).alias("failed_network_domain"),

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
    working_df.groupBy("ping_id")
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
        col("ping_id").isNotNull() &
        col("ping_ts").isNotNull() &
        col("lat").between(-90, 90) &
        col("lon").between(-180, 180)
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
    TABLE_NAME,"ping_id_not_null","null_check",
    int(validation_stats["failed_null_ping"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"ping_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"latitude_valid","range_check",
    int(validation_stats["failed_bad_lat"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"longitude_valid","range_check",
    int(validation_stats["failed_bad_lon"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"speed_reasonable","range_check",
    int(validation_stats["failed_speed_bounds"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"battery_range_valid","range_check",
    int(validation_stats["failed_battery_bounds"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"ping_ts_not_null","null_check",
    int(validation_stats["failed_null_ping_ts"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"gps_accuracy_reasonable","range_check",
    int(validation_stats["failed_poor_accuracy"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"mock_location_detected","fraud_check",
    int(validation_stats["failed_mock_location"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"network_type_domain_valid","domain_check",
    int(validation_stats["failed_network_domain"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Pings validation completed")