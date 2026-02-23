# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.orders_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.orders_quarantine"
TARGET_TABLE = "delivery_analytics.silver.orders"

print(f"🚀 Starting Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering out records found in {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("order_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "order_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all records.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & ENRICHMENT
# -----------------------------------------------------
# We normalize strings and cast numeric/boolean types to match our DDL
standardized_df = (clean_bronze_df
    .withColumn("city", F.upper(F.trim(F.col("city"))))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
    .withColumn("payment_status", F.initcap(F.trim(F.col("payment_status"))))
    .withColumn("payment_method", F.initcap(F.trim(F.col("payment_method"))))
    # Ensure numeric types match SQL Schema
    .withColumn("order_value", F.col("order_value").cast("double"))
    .withColumn("tax_amount", F.col("tax_amount").cast("double"))
    .withColumn("tip_amount", F.col("tip_amount").cast("double"))
    # Handle the Boolean flag from the generator (priority_flag -> is_priority_order in SQL)
    .withColumn("priority_flag", F.col("priority_flag").cast("boolean"))
    # Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION
# -----------------------------------------------------
# Using ingestion_ts to get the latest state of the order
window_spec = Window.partitionBy("order_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "order_id", "customer_id", "city", "restaurant_id", "delivery_zone",
        "created_ts", "accepted_ts", "promised_ts", "estimated_pickup_ts",
        "estimated_drop_ts", "estimated_distance_km", "estimated_duration_min",
        "order_value", "tax_amount", "tip_amount", "payment_method",
        "payment_status", "order_status", "priority_flag", "device_type",
        "app_version", "customer_rating_expected", "silver_ingestion_ts", "record_source"
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
        .merge(final_silver_df.alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
# Z-Ordering by city and order_id improves spatial query performance
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (city, order_id)")

print(f"✅ silver_orders processing complete.")

# COMMAND ----------

