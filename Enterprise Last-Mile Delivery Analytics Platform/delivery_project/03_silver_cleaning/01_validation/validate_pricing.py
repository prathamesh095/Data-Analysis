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



# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, count

# COMMAND ----------

TABLE_NAME = "pricing_raw"
BRONZE_TABLE = "delivery_analytics.bronze.pricing_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.pricing_quarantine"

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

# STEP 0 — SCHEMA DRIFT CHECK
# =====================================================
expected_cols = {
    "pricing_event_id","order_id","pricing_ts",
    "base_fare","distance_fare","time_fare",
    "surge_multiplier","service_fee","platform_fee",
    "discount_amount","promo_code_id","promo_type",
    "company_subsidy","rider_bonus","final_customer_price",
    "pricing_version","ingestion_ts","source_file",
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
        # null pricing id
        F.sum(F.when(F.col("pricing_event_id").isNull(), 1).otherwise(0))
            .alias("failed_null_id"),

        # negative money fields
        F.sum(
            F.when(
                (F.col("base_fare") < 0) |
                (F.col("distance_fare") < 0) |
                (F.col("time_fare") < 0) |
                (F.col("service_fee") < 0) |
                (F.col("platform_fee") < 0),
                1
            ).otherwise(0)
        ).alias("failed_negative_money"),

        # surge multiplier invalid
        F.sum(
            F.when(
                (F.col("surge_multiplier") < 1.0) |
                (F.col("surge_multiplier") > 5.0),
                1
            ).otherwise(0)
        ).alias("failed_bad_surge"),

        # final price invalid
        F.sum(
            F.when(F.col("final_customer_price") <= 0, 1).otherwise(0)
        ).alias("failed_final_price"),

        # discount unusually high (>80% of order)
        F.sum(
            F.when(F.col("discount_amount") > 100, 1).otherwise(0)
        ).alias("failed_high_discount"),

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
    working_df.groupBy("pricing_event_id")
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
        col("pricing_event_id").isNotNull() &
        (col("final_customer_price") > 0) &
        (col("surge_multiplier") >= 1.0)
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
    TABLE_NAME,"pricing_event_id_not_null","null_check",
    int(validation_stats["failed_null_id"]),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"pricing_event_id_unique","duplicate_check",
    int(failed_duplicates),total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"monetary_fields_positive","range_check",
    int(validation_stats["failed_negative_money"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"surge_multiplier_valid","range_check",
    int(validation_stats["failed_bad_surge"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"final_price_positive","range_check",
    int(validation_stats["failed_final_price"]),total_rows,"ERROR"
)

log_validation_result(
    TABLE_NAME,"discount_reasonable","range_check",
    int(validation_stats["failed_high_discount"]),total_rows,"WARNING"
)

log_validation_result(
    TABLE_NAME,"data_freshness_within_threshold","freshness_check",
    failed_freshness,total_rows,"CRITICAL"
)

log_validation_result(
    TABLE_NAME,"volume_reasonable_min_threshold","volume_check",
    failed_volume,total_rows,"WARNING"
)

print("🏆 ELITE Pricing validation completed")