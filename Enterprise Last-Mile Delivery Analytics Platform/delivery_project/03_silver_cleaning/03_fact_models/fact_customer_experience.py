# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. CONFIGURATION
# -----------------------------------------------------
CATALOG = "delivery_analytics"
SCHEMA  = "silver"

# Source Entities
ORDERS_TBL     = f"{CATALOG}.{SCHEMA}.orders"
DELIVERIES_TBL = f"{CATALOG}.{SCHEMA}.deliveries"
SUPPORT_TBL    = f"{CATALOG}.{SCHEMA}.support"

# Target Fact
TARGET_FACT    = f"{CATALOG}.{SCHEMA}.fact_customer_experience"

print(f"🚀 Starting Production Build for: {TARGET_FACT}")

# 2. PRE-FLIGHT CHECKS (Industry Standard)
# -----------------------------------------------------
# Ensure the silver schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Check if required source tables exist to avoid [TABLE_OR_VIEW_NOT_FOUND]
required_tables = [ORDERS_TBL, DELIVERIES_TBL, SUPPORT_TBL]
for tbl in required_tables:
    if not spark.catalog.tableExists(tbl):
        raise Exception(f"❌ Dependency Error: {tbl} not found. Please run your Silver Entity cleaning jobs first.")

# 3. DATA EXTRACTION
# -----------------------------------------------------
df_orders   = spark.table(ORDERS_TBL).alias("o")
df_deliver  = spark.table(DELIVERIES_TBL).alias("d")
df_support  = spark.table(SUPPORT_TBL).alias("s")

# 4. TRANSFORMATION & INDUSTRY KPI LOGIC
# -----------------------------------------------------
# We join Orders with Deliveries and Support to create a customer experience profile
fact_cx_df = (df_orders
    .join(df_deliver, "order_id", "left")
    .join(df_support, "order_id", "left")
    .select(
        "o.order_id",
        "o.customer_id",
        "o.city",
        "o.created_ts",
        # Synthetic Data Mapping
        "d.customer_rating",
        "d.delivery_status",
        "s.ticket_id",
        "s.issue_category",
        "s.compensation_amount",
        "s.csat_score",
        
        # PRO LOGIC: Binary Flags for easier Dashboarding
        F.when(F.col("s.ticket_id").isNotNull(), 1).otherwise(0).alias("has_support_ticket_flag"),
        
        # Logic: Friction event = (Low rating 1-2) OR (Support Ticket exists) OR (Cancellation)
        F.when(
            (F.col("d.customer_rating") <= 2) | 
            (F.col("s.ticket_id").isNotNull()) |
            (F.col("o.order_status") == 'cancel'), 1
        ).otherwise(0).alias("is_friction_event_flag"),
        
        # AUDIT METADATA
        F.current_timestamp().alias("fact_load_timestamp"),
        F.lit("SILVER_JOIN").alias("transformation_source")
    )
)

# 5. WRITING AS MANAGED TABLE
# -----------------------------------------------------
print(f"📦 Writing to Managed Table: {TARGET_FACT}")
(fact_cx_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_FACT))

# 6. PERFORMANCE OPTIMIZATION
# -----------------------------------------------------
print(f"⚡ Optimizing {TARGET_FACT}...")
spark.sql(f"OPTIMIZE {TARGET_FACT} ZORDER BY (city, created_ts)")

print(f"✅ Fact Build Successful: {TARGET_FACT}")