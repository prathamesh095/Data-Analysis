# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# -----------------------------------------------------
# 1. CONFIGURATION & PATHING
# -----------------------------------------------------
# Source Tables (Bronze)
BRONZE_TABLE = "delivery_analytics.bronze.riders_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.riders_quarantine"

# Target Table (Silver) - Saved in the silver schema
TARGET_TABLE = "delivery_analytics.silver.riders"

print(f"🚀 Processing Silver Table: {TARGET_TABLE}")

# -----------------------------------------------------
# 2. READ BRONZE & FILTER QUARANTINE
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

# Dynamic check for Quarantine table existence
if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering records from {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("rider_id").distinct()
    # Left Anti Join removes any ID that exists in the quarantine table
    clean_bronze_df = bronze_df.join(quarantine_ids, "rider_id", "left_anti")
else:
    print(f"⚠️ No quarantine table found. Proceeding with all Bronze records.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & DATA CLEANING
# -----------------------------------------------------
# Matching your synthetic data schema columns exactly
standardized_df = (clean_bronze_df
    .withColumn("rider_name", F.initcap(F.trim(F.col("rider_name"))))
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("vehicle_type", F.lower(F.trim(F.col("vehicle_type"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    # Casting to match our SQL DDL definitions
    .withColumn("join_date", F.col("join_date").cast("date"))
    .withColumn("rating_avg", F.col("rating_avg").cast("double"))
    .withColumn("total_deliveries_lifetime", F.col("total_deliveries_lifetime").cast("int"))
    .withColumn("last_active_ts", F.col("last_active_ts").cast("timestamp"))
    .withColumn("bank_account_verified", F.col("bank_account_verified").cast("boolean"))
    # Audit Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION (State Management)
# -----------------------------------------------------
# We use ingestion_ts to ensure we only keep the absolute latest version of each rider
window_spec = Window.partitionBy("rider_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_number", F.row_number().over(window_spec))
    .filter("row_number == 1")
    .drop("row_number")
    # Selection to match the exact Silver table folder structure
    .select(
        "rider_id", "rider_name", "city", "home_zone", "join_date", 
        "vehicle_type", "license_verified_flag", "background_check_flag", 
        "employment_type", "status", "rating_avg", "total_deliveries_lifetime", 
        "last_active_ts", "device_os", "device_model", "bank_account_verified", 
        "incentive_tier", "silver_ingestion_ts", "record_source"
    )
)

# -----------------------------------------------------
# 5. UPSERT (MERGE) INTO SILVER FOLDER
# -----------------------------------------------------
# If the table doesn't exist, create it (should have been created by our DDL script)
if not spark.catalog.tableExists(TARGET_TABLE):
    print(f"📦 Initializing Target: {TARGET_TABLE}")
    (final_silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE))
else:
    print(f"🔄 Merging updates into {TARGET_TABLE}")
    target_delta = DeltaTable.forName(spark, TARGET_TABLE)
    
    (target_delta.alias("target")
        .merge(final_silver_df.alias("source"), "target.rider_id = source.rider_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. PHYSICAL OPTIMIZATION
# -----------------------------------------------------
# Optimizing the file structure inside the table folder
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (rider_id)")

print(f"✅ Silver table '{TARGET_TABLE}' updated successfully.")