# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# 1. CONFIGURATION
# -----------------------------------------------------
GOLD_DB = "delivery_analytics.gold"
# Source: We use our already-built Foundation Marts
ORDER_MART    = f"{GOLD_DB}.gold_order_facts"
DELIVERY_MART = f"{GOLD_DB}.gold_delivery_facts"

TARGET_TABLE  = f"{GOLD_DB}.gold_delivery_kpis"

def get_foundation_marts() -> tuple:
    """Load the wide fact tables created in the previous step"""
    orders = spark.table(ORDER_MART).alias("om")
    deliveries = spark.table(DELIVERY_MART).alias("dm")
    return orders, deliveries

def transform_executive_kpis(orders: DataFrame, deliveries: DataFrame) -> DataFrame:
    """Roll up metrics to City + Date level"""
    
    # Step A: Aggregate Order/Financial Metrics
    order_kpi = (orders.groupBy("city", "order_date").agg(
        F.count("order_id").alias("total_orders_placed"),
        F.sum("is_success_flag").alias("total_completed_orders"),
        F.sum("is_cancelled_flag").alias("total_cancelled_orders"),
        F.sum("net_revenue").alias("daily_net_revenue"),
        F.avg("order_value").alias("avg_basket_value")
    ))
    
    # Step B: Aggregate Delivery Speed Metrics
    delivery_kpi = (deliveries.groupBy("city", "delivery_date").agg(
        F.avg("total_delivery_time_min").alias("avg_delivery_time"),
        # Success Rate: (On-Time / Total)
        (F.sum("is_on_time_flag") / F.count("delivery_id")).alias("on_time_delivery_rate")
    ))
    
    # Step C: Join both aggregates to create the Master KPI row
    return (order_kpi.join(
        delivery_kpi, 
        (order_kpi.city == delivery_kpi.city) & (order_kpi.order_date == delivery_kpi.delivery_date), 
        "inner"
    ).select(
        order_kpi["city"],
        order_kpi["order_date"].alias("report_date"),
        "total_orders_placed",
        "total_completed_orders",
        "total_cancelled_orders",
        # Calculate Cancellation Rate
        (F.col("total_cancelled_orders") / F.col("total_orders_placed")).alias("cancellation_rate"),
        F.round("daily_net_revenue", 2).alias("net_revenue"),
        F.round("on_time_delivery_rate", 4).alias("otd_pct"),
        F.round("avg_delivery_time", 2).alias("avg_delivery_min"),
        F.current_timestamp().alias("gold_ingestion_ts")
    ))

def save_gold_table(df: DataFrame, table_name: str):
    """Save with Overwrite. No need for Z-order as the table is very small."""
    (df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(table_name))

# 2. EXECUTION
# -----------------------------------------------------
print(f"🚀 Building Executive KPI Table: {TARGET_TABLE}")

df_om, df_dm = get_foundation_marts()
final_kpi_df = transform_executive_kpis(df_om, df_dm)
save_gold_table(final_kpi_df, TARGET_TABLE)

print(f"✅ Executive KPI Build Successful.")