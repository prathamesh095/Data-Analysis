# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
GOLD_DB = "delivery_analytics.gold"
# Source: Using the Daily Summary we built earlier (Aggregating the Aggregate)
RIDER_SUMMARY = f"{GOLD_DB}.gold_rider_daily_summary"

TARGET_TABLE  = f"{GOLD_DB}.gold_rider_performance"

def transform_rider_performance(df_daily: DataFrame) -> DataFrame:
    """Create a high-level performance profile for every rider"""
    
    return (df_daily.groupBy("rider_id")
        .agg(
            F.count("delivery_date").alias("days_active"),
            F.sum("total_completed_deliveries").alias("total_lifetime_deliveries"),
            F.avg("deliveries_per_hour").alias("avg_dph"),
            F.avg("on_time_rate").alias("lifetime_otd_pct"),
            F.sum("total_km_driven").alias("total_distance_covered_km")
        )
        .withColumn(
            "rider_tier",
            F.when(F.col("total_lifetime_deliveries") > 500, "ELITE")
             .when(F.col("total_lifetime_deliveries") > 100, "PRO")
             .otherwise("ROOKIE")
        )
        .withColumn(
            "reliability_score",
            F.round((F.col("lifetime_otd_pct") * 0.7) + (F.col("avg_dph") * 0.3), 2)
        )
        .select(
            "rider_id",
            "rider_tier",
            "days_active",
            "total_lifetime_deliveries",
            F.round("avg_dph", 2).alias("avg_dph"),
            F.round("lifetime_otd_pct", 4).alias("lifetime_otd_pct"),
            "reliability_score",
            F.current_timestamp().alias("gold_ingestion_ts")
        )
    )

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building Rider Performance Profile: {TARGET_TABLE}")

df_daily_stats = spark.table(RIDER_SUMMARY)
final_performance_df = transform_rider_performance(df_daily_stats)

(final_performance_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE))

print(f"✅ Rider Performance Table Build Successful.")