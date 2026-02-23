# Databricks notebook source
from pyspark.sql import functions as F

TARGET_TABLE = "delivery_analytics.silver.metric_pipeline_performance"
SOURCE_TABLE = "delivery_analytics.silver.fact_delivery_performance"

# Read only required column (projection pushdown)
df_run = spark.table(SOURCE_TABLE).select("fact_load_timestamp")

# Single-pass aggregation (most efficient)
agg_df = df_run.agg(
    F.count(F.lit(1)).alias("total_rows_processed"),
    F.min("fact_load_timestamp").alias("start_time"),
    F.max("fact_load_timestamp").alias("end_time")
)

# Compute metrics safely
run_stats = (
    agg_df
    .withColumn(
        "processing_duration_sec",
        F.greatest(
            F.unix_timestamp("end_time") - F.unix_timestamp("start_time"),
            F.lit(0)
        )
    )
    .withColumn(
        "rows_per_second",
        F.try_divide(
            F.col("total_rows_processed"),
            F.col("processing_duration_sec")
        )
    )
    .withColumn("metric_generated_ts", F.current_timestamp())
)

# Write optimized Delta table
(
    run_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)