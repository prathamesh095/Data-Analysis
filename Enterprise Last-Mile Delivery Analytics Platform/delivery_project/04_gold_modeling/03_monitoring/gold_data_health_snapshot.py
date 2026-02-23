# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
GOLD_DB   = "delivery_analytics.gold"
SILVER_DB = "delivery_analytics.silver"

# FIXED: Changed from metric_data_quality_scores to metric_data_health_score
DQ_METRICS = f"{SILVER_DB}.metric_data_health_score" 

TARGET_TABLE = f"{GOLD_DB}.gold_data_health_snapshot"

def transform_health_snapshot(df_dq: DataFrame) -> DataFrame:
    """Transform technical DQ metrics into a business-readable health score"""
    
    # Note: We use 'total_records' here because that's what you 
    # named it in the 04_validation_metrics notebook fix earlier.
    return (df_dq.select(
        F.to_date("processed_at").alias("report_date"),
        F.col("total_records").alias("total_rows"),
        "valid_records",
        "overall_health_pct"
    ).withColumn(
        "health_status",
        F.when(F.col("overall_health_pct") >= 98, "EXCELLENT")
         .when(F.col("overall_health_pct") >= 90, "WARNING")
         .otherwise("CRITICAL")
    ).withColumn("gold_ingestion_ts", F.current_timestamp())
    )

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building Data Health Snapshot: {TARGET_TABLE}")

# Ensure the source table exists before trying to read it
if spark.catalog.tableExists(DQ_METRICS):
    df_dq = spark.table(DQ_METRICS)
    final_health_df = transform_health_snapshot(df_dq)

    (final_health_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE))

    print(f"✅ Data Health Snapshot Build Successful.")
else:
    print(f"❌ Error: {DQ_METRICS} not found. Please run the validation metrics notebook first.")

# COMMAND ----------

