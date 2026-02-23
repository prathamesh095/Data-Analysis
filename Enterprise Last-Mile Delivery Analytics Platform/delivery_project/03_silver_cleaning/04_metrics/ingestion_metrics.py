# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F

TARGET_TABLE = "delivery_analytics.silver.metric_ingestion_latency"

# 1. LOAD FACT DATA
df = spark.table("delivery_analytics.silver.fact_delivery_performance")

# 2. CALCULATE LATENCY AT THE ROW LEVEL
latency_df = df.select(
    "city", "created_ts", "fact_load_timestamp"
).withColumn(
    "delay_sec", 
    (F.unix_timestamp("fact_load_timestamp") - F.unix_timestamp("created_ts"))
)

# 3. ADVANCED AGGREGATION (Percentile Calculation)
# This shows the "Experience" of 95% of the data flow
final_metrics = latency_df.groupBy("city").agg(
    F.avg("delay_sec").alias("avg_delay_sec"),
    F.percentile_approx("delay_sec", 0.95).alias("p95_delay_sec"), # Advanced: P95
    F.max("delay_sec").alias("max_spike_sec"),
    F.count("*").alias("sample_size")
).withColumn("metric_ingested_at", F.current_timestamp())

final_metrics.write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)