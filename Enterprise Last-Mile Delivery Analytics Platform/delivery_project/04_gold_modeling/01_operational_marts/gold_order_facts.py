# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
SILVER_DB = "delivery_analytics.silver"
GOLD_DB   = "delivery_analytics.gold"
TARGET_TABLE = f"{GOLD_DB}.gold_order_facts"

def get_silver_data() -> tuple:
    """Load required silver entities as aliases"""
    orders = spark.table(f"{SILVER_DB}.orders").alias("o")
    pricing = spark.table(f"{SILVER_DB}.pricing").alias("p")
    cancels = spark.table(f"{SILVER_DB}.cancellations").alias("c")
    return orders, pricing, cancels

def transform_order_mart(orders: DataFrame, pricing: DataFrame, cancels: DataFrame) -> DataFrame:
    """Apply business logic and denormalization"""
    return (orders
        .join(pricing, "order_id", "left")
        .join(cancels, "order_id", "left")
        .select(
            "o.order_id",
            "o.customer_id",
            "o.city",
            "o.order_status",
            # Time Attributes
            "o.created_ts",
            F.to_date("o.created_ts").alias("order_date"),
            F.hour("o.created_ts").alias("order_hour"),
            F.date_format("o.created_ts", "EEEE").alias("day_name"),
            
            # Financial Business Logic
            F.col("o.order_value").cast("double"),
            F.coalesce(F.col("p.service_fee"), F.lit(0)).alias("service_fee"),
            F.coalesce(F.col("p.discount_amount"), F.lit(0)).alias("discount_amount"),
            
            # Net Revenue = (Value + Fee) - Discount
            ((F.col("o.order_value") + F.coalesce(F.col("p.service_fee"), F.lit(0))) - 
             F.coalesce(F.col("p.discount_amount"), F.lit(0))).alias("net_revenue"),
            
            # Operational Flags (1/0 for BI tools)
            F.when(F.col("o.order_status") == 'success', 1).otherwise(0).alias("is_success_flag"),
            F.when(F.col("o.order_status") == 'cancel', 1).otherwise(0).alias("is_cancelled_flag"),
            "c.cancel_reason_code",
            
            F.current_timestamp().alias("gold_ingestion_ts")
        )
    )

def save_gold_table(df: DataFrame, table_name: str):
    """Save with production performance settings"""
    (df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("order_date")
        .saveAsTable(table_name))
    
    # Z-Order for high-speed filtering in Dashboards
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (city, order_status)")

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Starting Gold Build: {TARGET_TABLE}")

# Pre-flight: Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_DB}")

# Run pipeline
df_orders, df_pricing, df_cancels = get_silver_data()
final_gold_df = transform_order_mart(df_orders, df_pricing, df_cancels)
save_gold_table(final_gold_df, TARGET_TABLE)

print(f"✅ Successfully built {TARGET_TABLE}")