# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F

# 1. CONFIGURATION
GOLD_DB   = "delivery_analytics.gold"
SILVER_DB = "delivery_analytics.silver"

# UPDATED: Mapping to the correct columns found in your stacktrace
SOURCE_ORDERS = f"{SILVER_DB}.orders"
FACT_TABLE    = f"{SILVER_DB}.fact_delivery_performance"
GOLD_KPI      = f"{GOLD_DB}.gold_delivery_kpis"

TARGET_TABLE  = f"{GOLD_DB}.gold_pipeline_freshness"

def calculate_freshness():
    try:
        # 2. GET MAX TIMESTAMPS
        # Source: Using 'created_ts' based on your stacktrace
        source_ts = spark.table(SOURCE_ORDERS).select(F.max(F.col("created_ts"))).collect()[0][0]
        
        # Silver: Using 'fact_load_timestamp' 
        silver_ts = spark.table(FACT_TABLE).select(F.max(F.col("fact_load_timestamp"))).collect()[0][0]
        
        # Gold: Using 'gold_ingestion_ts'
        gold_ts   = spark.table(GOLD_KPI).select(F.max(F.col("gold_ingestion_ts"))).collect()[0][0]
        
        # 3. CALCULATE LATENCY
        # Difference in minutes between when data was created vs when it hit the Gold KPI
        if source_ts and gold_ts:
            latency_min = (gold_ts.timestamp() - source_ts.timestamp()) / 60
        else:
            latency_min = 0

        data = [(
            source_ts, 
            silver_ts, 
            gold_ts,
            round(latency_min, 2)
        )]
        
        # 4. SAVE TO GOLD
        df = spark.createDataFrame(data, ["last_source_event", "last_silver_update", "last_gold_update", "e2e_latency_min"])
        
        (df.withColumn("monitored_at", F.current_timestamp())
           .write.format("delta")
           .mode("overwrite")
           .saveAsTable(TARGET_TABLE))
           
        print(f"✅ Freshness Success! E2E Latency: {round(latency_min, 2)} minutes.")

    except Exception as e:
        print(f"❌ Error: {str(e)}")

# EXECUTE
calculate_freshness()