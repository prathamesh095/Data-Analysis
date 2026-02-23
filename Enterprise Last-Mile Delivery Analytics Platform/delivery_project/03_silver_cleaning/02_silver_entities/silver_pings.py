# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.pings_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.pings_quarantine"
TARGET_TABLE = "delivery_analytics.silver.pings"

print(f"🚀 Starting Optimized Silver transformation for high-volume table: {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering telemetry records from {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("ping_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "ping_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all pings.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
standardized_df = (clean_bronze_df
    .withColumn("network_type", F.upper(F.trim(F.col("network_type"))))
    .withColumn("gps_provider", F.lower(F.trim(F.col("gps_provider"))))
    # Numeric precision for coordinates and sensors
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("speed_kmh", F.col("speed_kmh").cast("int"))
    .withColumn("heading", F.col("heading").cast("int"))
    .withColumn("accuracy_meters", F.col("accuracy_meters").cast("int"))
    .withColumn("battery_level", F.col("battery_level").cast("int"))
    .withColumn("is_mock_location_flag", F.col("is_mock_location_flag").cast("boolean"))
    # Audit Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION
# -----------------------------------------------------
# Pings are high frequency; we deduplicate by ping_id
window_spec = Window.partitionBy("ping_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "ping_id", "rider_id", "delivery_id", "lat", "lon", 
        "speed_kmh", "heading", "accuracy_meters", "battery_level", 
        "network_type", "ping_ts", "gps_provider", 
        "is_mock_location_flag", "silver_ingestion_ts", "record_source"
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
    print(f"🔄 Merging pings into {TARGET_TABLE}")
    target_delta = DeltaTable.forName(spark, TARGET_TABLE)
    
    # Merge on ping_id. Since this is partitioned by date, 
    # Delta Lake will use partition pruning for the merge if possible.
    (target_delta.alias("t")
        .merge(final_silver_df.alias("s"), "t.ping_id = s.ping_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
# Z-Ordering by rider_id and delivery_id is crucial for route reconstruction
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (rider_id, delivery_id)")

print(f"✅ silver_pings processing complete.")