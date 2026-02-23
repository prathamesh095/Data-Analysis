# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
GOLD_DB = "delivery_analytics.gold"
SILVER_DB = "delivery_analytics.silver"

# Sources
ORDER_MART = f"{GOLD_DB}.gold_order_facts"
SHIFT_TBL  = f"{SILVER_DB}.shifts"

TARGET_TABLE = f"{GOLD_DB}.gold_city_operations"

def transform_city_ops(orders: DataFrame, shifts: DataFrame) -> DataFrame:
    """Calculate Marketplace Pressure (Demand vs Supply)"""
    
    # Step A: Demand - Orders per city per hour
    demand_df = (orders.groupBy("city", "order_date", "order_hour")
        .agg(F.count("order_id").alias("demand_order_count"))
    )
    
    # Step B: Supply - Riders active per city per hour
    # We check riders who were logged in during that specific hour
    supply_df = (shifts.withColumn("shift_date", F.to_date("actual_login_ts"))
        .withColumn("login_hour", F.hour("actual_login_ts"))
        .groupBy("city", "shift_date", "login_hour")
        .agg(F.countDistinct("rider_id").alias("supply_rider_count"))
    )
    
    # Step C: Combine to find the "Pressure Ratio"
    return (demand_df.join(
        supply_df,
        (demand_df.city == supply_df.city) & 
        (demand_df.order_date == supply_df.shift_date) & 
        (demand_df.order_hour == supply_df.login_hour),
        "inner"
    ).select(
        demand_df["city"],
        "order_date",
        "order_hour",
        "demand_order_count",
        "supply_rider_count",
        # Ratio: Orders per Rider
        F.round(F.col("demand_order_count") / F.col("supply_rider_count"), 2).alias("orders_per_rider_ratio"),
        # Marketplace Status logic
        F.when(F.col("demand_order_count") / F.col("supply_rider_count") > 5, "UNDER_SUPPLIED")
         .when(F.col("demand_order_count") / F.col("supply_rider_count") < 1, "OVER_SUPPLIED")
         .otherwise("BALANCED").alias("market_status"),
        
        F.current_timestamp().alias("gold_ingestion_ts")
    ))

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building City Operations Mart: {TARGET_TABLE}")

df_orders = spark.table(ORDER_MART)
df_shifts = spark.table(SHIFT_TBL)

final_ops_df = transform_city_ops(df_orders, df_shifts)

(final_ops_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE))

print(f"✅ City Operations Build Successful.")

# COMMAND ----------

