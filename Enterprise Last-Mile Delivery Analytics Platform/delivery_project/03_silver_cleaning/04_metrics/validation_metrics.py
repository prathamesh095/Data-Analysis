# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

# 1. CONFIGURATION
SOURCE_TABLE = "delivery_analytics.silver.fact_delivery_performance"
TARGET_TABLE = "delivery_analytics.silver.metric_data_health_score"

# 2. LOAD THE DATA
# Checking if the table exists first to avoid errors
if not spark.catalog.tableExists(SOURCE_TABLE):
    print(f"⚠️ Source table {SOURCE_TABLE} not found. Skipping validation.")
else:
    df = spark.table(SOURCE_TABLE)

    # 3. DEFINE VALIDATION LOGIC
    # Fixed: Removed 'order_value' and used 'order_id' + 'rider_id'
    # These are the columns that define a valid delivery record
    health_check = df.withColumn(
        "is_valid_row",
        F.when(
            (F.col("order_id").isNotNull()) & 
            (F.col("rider_id").isNotNull()) & 
            (F.col("city").isNotNull()) &
            (F.col("delivery_status").isin("completed", "cancelled", "delivered")), 1
        ).otherwise(0)
    )

    # 4. AGGREGATE SUMMARY
    health_summary = health_check.agg(
        F.count("*").alias("total_records"),
        F.sum("is_valid_row").alias("valid_records"),
        # Calculate Percentage
        F.round((F.sum("is_valid_row") / F.count("*") * 100), 2).alias("overall_health_pct")
    ).withColumn("processed_at", F.current_timestamp())

    # 5. WRITE RESULTS
    # We use 'append' here so you can see a history of your data health over time
    health_summary.write.format("delta").mode("append").saveAsTable(TARGET_TABLE)
    
    print(f"✅ Data Health Metrics updated for {datetime.now()}")