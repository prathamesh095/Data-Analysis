# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.shifts_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.shifts_quarantine"
TARGET_TABLE = "delivery_analytics.silver.shifts"

print(f"🚀 Starting Optimized Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering records from {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("shift_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "shift_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all shifts.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
standardized_df = (clean_bronze_df
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("shift_status", F.initcap(F.trim(F.col("shift_status"))))
    .withColumn("shift_type", F.initcap(F.trim(F.col("shift_type"))))
    # Numeric casting
    .withColumn("scheduled_hours", F.col("scheduled_hours").cast("double"))
    # Boolean casting
    .withColumn("late_start_flag", F.col("late_start_flag").cast("boolean"))
    .withColumn("early_logout_flag", F.col("early_logout_flag").cast("boolean"))
    .withColumn("overtime_flag", F.col("overtime_flag").cast("boolean"))
    # Audit Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION
# -----------------------------------------------------
window_spec = Window.partitionBy("shift_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "shift_id", "rider_id", "city", "shift_start_ts", "shift_end_ts",
        "scheduled_hours", "actual_login_ts", "actual_logout_ts",
        "shift_status", "late_start_flag", "early_logout_flag",
        "overtime_flag", "shift_type", "manager_id",
        "silver_ingestion_ts", "record_source"
    )
)

# -----------------------------------------------------
# 5. UPSERT (MERGE)
# -----------------------------------------------------
if not spark.catalog.tableExists(TARGET_TABLE):
    print(f"📦 Initializing {TARGET_TABLE}")
    (final_silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE))
else:
    print(f"🔄 Merging updates into {TARGET_TABLE}")
    target_delta = DeltaTable.forName(spark, TARGET_TABLE)
    (target_delta.alias("t")
        .merge(final_silver_df.alias("s"), "t.shift_id = s.shift_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (rider_id, shift_start_ts)")

print(f"✅ silver_shifts processing complete.")