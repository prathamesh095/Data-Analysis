# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.deliveries_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.deliveries_quarantine"
TARGET_TABLE = "delivery_analytics.silver.deliveries"

print(f"🚀 Starting Optimized Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering out records found in {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("delivery_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "delivery_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all records.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
# We ensure status is lowercase and numeric metrics are correctly typed
standardized_df = (clean_bronze_df
    .withColumn("delivery_status", F.lower(F.trim(F.col("delivery_status"))))
    .withColumn("failure_reason", F.trim(F.col("failure_reason")))
    # Precision casting for performance and schema matching
    .withColumn("distance_km_actual", F.col("distance_km_actual").cast("double"))
    .withColumn("duration_min_actual", F.col("duration_min_actual").cast("int"))
    .withColumn("traffic_delay_min", F.col("traffic_delay_min").cast("int"))
    .withColumn("customer_rating", F.col("customer_rating").cast("int"))
    .withColumn("rider_rating", F.col("rider_rating").cast("int"))
    # Boolean logic
    .withColumn("weather_flag", F.col("weather_flag").cast("boolean"))
    .withColumn("proof_of_delivery_flag", F.col("proof_of_delivery_flag").cast("boolean"))
    # Audit Columns
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION (Identity Management)
# -----------------------------------------------------
# Deduplicate using source ingestion_ts to pick the latest delivery state
window_spec = Window.partitionBy("delivery_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "delivery_id", "order_id", "rider_id", "dispatch_ts", 
        "arrival_at_pickup_ts", "pickup_ts", "departure_from_pickup_ts", 
        "arrival_at_drop_ts", "drop_ts", "delivery_status", "failure_reason", 
        "distance_km_actual", "duration_min_actual", "traffic_delay_min", 
        "weather_flag", "proof_of_delivery_flag", "customer_rating", 
        "rider_rating", "silver_ingestion_ts", "record_source"
    )
)

# -----------------------------------------------------
# 5. UPSERT (MERGE) INTO SILVER FOLDER
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
        .merge(final_silver_df.alias("s"), "t.delivery_id = s.delivery_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
# Z-Ordering by delivery_id and order_id for fast downstream joins
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (delivery_id, order_id)")

print(f"✅ silver_deliveries processing complete.")