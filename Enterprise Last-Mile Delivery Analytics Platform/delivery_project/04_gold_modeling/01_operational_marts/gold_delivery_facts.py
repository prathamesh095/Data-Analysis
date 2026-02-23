# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
SILVER_DB = "delivery_analytics.silver"
GOLD_DB   = "delivery_analytics.gold"
TARGET_TABLE = f"{GOLD_DB}.gold_delivery_facts"

def get_silver_data() -> tuple:
    """Load required silver entities"""
    deliveries = spark.table(f"{SILVER_DB}.deliveries").alias("d")
    orders     = spark.table(f"{SILVER_DB}.orders").alias("o")
    riders     = spark.table(f"{SILVER_DB}.riders").alias("r")
    return deliveries, orders, riders

def transform_delivery_mart(deliveries: DataFrame, orders: DataFrame, riders: DataFrame) -> DataFrame:
    """Calculate speed, efficiency, and OTD metrics"""
    return (deliveries
        .join(orders, "order_id", "inner")
        .join(riders, "rider_id", "left")
        .select(
            "d.delivery_id",
            "d.order_id",
            "d.rider_id",
            "o.city",
            "r.vehicle_type",
            "d.delivery_status",
            
            # --- TIMING METRICS ---
            "o.created_ts",
            "o.promised_ts",
            "d.drop_ts",
            # Actual Time taken in minutes
            ((F.unix_timestamp("d.drop_ts") - F.unix_timestamp("o.created_ts")) / 60).alias("total_delivery_time_min"),
            
            # --- ON-TIME DELIVERY (OTD) LOGIC ---
            F.when(F.col("d.drop_ts") <= F.col("o.promised_ts"), 1)
             .when(F.col("d.drop_ts") > F.col("o.promised_ts"), 0)
             .otherwise(None).alias("is_on_time_flag"),
            
            # Delay in minutes (Positive = Late, Negative = Early)
            ((F.unix_timestamp("d.drop_ts") - F.unix_timestamp("o.promised_ts")) / 60).alias("delay_minutes"),
            
            # --- DISTANCE EFFICIENCY ---
            "o.estimated_distance_km",
            "d.distance_km_actual",
            (F.col("d.distance_km_actual") - F.col("o.estimated_distance_km")).alias("distance_variance"),
            
            # Date for partitioning
            F.to_date("o.created_ts").alias("delivery_date"),
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
    
    # ZORDER on City and Vehicle helps optimize speed/fleet analysis
    spark.sql(f"OPTIMIZE {table_name} ZORDER BY (city, vehicle_type)")

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building Delivery Fact Mart: {TARGET_TABLE}")

df_del, df_ord, df_rid = get_silver_data()
final_df = transform_delivery_mart(df_del, df_ord, df_rid)
save_gold_table(final_df, TARGET_TABLE)

print(f"✅ Successfully built {TARGET_TABLE}")