# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
SILVER_DB = "delivery_analytics.silver"
GOLD_DB   = "delivery_analytics.gold"
TARGET_TABLE = f"{GOLD_DB}.gold_rider_daily_summary"

def get_source_data() -> tuple:
    """Load silver shifts and the gold delivery mart we just built"""
    shifts = spark.table(f"{SILVER_DB}.shifts").alias("s")
    # We use the gold_delivery_facts we just created for performance
    delivery_facts = spark.table(f"{GOLD_DB}.gold_delivery_facts").alias("df")
    return shifts, delivery_facts

def transform_rider_summary(shifts: DataFrame, delivery_facts: DataFrame) -> DataFrame:
    """Aggregate rider performance by day"""
    
    # Step A: Aggregate Deliveries per Rider per Day
    delivery_agg = (delivery_facts
        .groupBy("rider_id", "delivery_date")
        .agg(
            F.count("delivery_id").alias("total_completed_deliveries"),
            F.sum("is_on_time_flag").alias("total_on_time_deliveries"),
            F.avg("total_delivery_time_min").alias("avg_delivery_duration_min"),
            F.sum("distance_km_actual").alias("total_km_driven")
        ).alias("da")
    )
    
    # Step B: Join with Shifts to calculate Utilization
    return (shifts
        .join(delivery_agg, 
              (F.col("s.rider_id") == F.col("da.rider_id")) & 
              (F.to_date(F.col("s.shift_start_ts")) == F.col("da.delivery_date")), 
              "inner")
        .select(
            "s.rider_id",
            "da.delivery_date",
            "s.scheduled_hours",
            # Actual login time in hours
            ((F.unix_timestamp("s.actual_logout_ts") - F.unix_timestamp("s.actual_login_ts")) / 3600).alias("actual_online_hours"),
            "da.total_completed_deliveries",
            "da.total_km_driven",
            
            # --- PRODUCTIVITY KPIs ---
            # Deliveries per hour (DPH) - Industry Standard
            (F.col("da.total_completed_deliveries") / 
             ((F.unix_timestamp("s.actual_logout_ts") - F.unix_timestamp("s.actual_login_ts")) / 3600)
            ).alias("deliveries_per_hour"),
            
            # On-Time Rate
            (F.col("da.total_on_time_deliveries") / F.col("da.total_completed_deliveries")).alias("on_time_rate"),
            
            F.current_timestamp().alias("gold_ingestion_ts")
        )
    )

def save_gold_table(df: DataFrame, table_name: str):
    """Save and Optimize"""
    (df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("delivery_date")
        .saveAsTable(table_name))
    
    # ZORDER on rider_id for individual performance lookups
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (rider_id)")

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building Rider Daily Summary: {TARGET_TABLE}")

df_s, df_df = get_source_data()
final_summary_df = transform_rider_summary(df_s, df_df)
save_gold_table(final_summary_df, TARGET_TABLE)

print(f"✅ Successfully built {TARGET_TABLE}")