# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. CONFIGURATION
# -----------------------------------------------------
CATALOG = "delivery_analytics"
SCHEMA  = "silver"

# Source Entities
ORDERS_TBL  = f"{CATALOG}.{SCHEMA}.orders"
PRICING_TBL = f"{CATALOG}.{SCHEMA}.pricing"
SUPPORT_TBL = f"{CATALOG}.{SCHEMA}.support"

# Target Managed Table
TARGET_FACT = f"{CATALOG}.{SCHEMA}.fact_financials"

print(f"🚀 Building {TARGET_FACT}...")

# 2. DEPENDENCY VALIDATION
# -----------------------------------------------------
required_tables = [ORDERS_TBL, PRICING_TBL, SUPPORT_TBL]
for tbl in required_tables:
    if not spark.catalog.tableExists(tbl):
        raise Exception(f"❌ Dependency Error: {tbl} not found.")

# 3. DATA EXTRACTION
# -----------------------------------------------------
df_orders  = spark.table(ORDERS_TBL).alias("o")
df_pricing = spark.table(PRICING_TBL).alias("p")
# Aggregating support to get total compensation per order
df_support = spark.table(SUPPORT_TBL).groupBy("order_id").agg(
    F.sum("compensation_amount").alias("total_refunded_amount")
).alias("s")

# 4. TRANSFORMATION & FINANCIAL LOGIC
# -----------------------------------------------------
fact_financials_df = (df_orders
    .join(df_pricing, "order_id", "inner")
    .join(df_support, "order_id", "left")
    .select(
        "o.order_id",
        "o.city",
        "o.created_ts",
        
        # --- GROSS REVENUE ---
        "o.order_value",
        "p.base_fare",
        "p.surge_multiplier",
        "p.service_fee",
        
        # --- LOSSES & DISCOUNTS ---
        "p.discount_amount",
        F.coalesce(F.col("s.total_refunded_amount"), F.lit(0)).alias("total_refunded_amount"),
        
        # --- PRO NET REVENUE CALCULATION ---
        # Formula: (Order Value + Fees) - Discounts - Refunds
        ((F.col("o.order_value") + F.col("p.service_fee")) - 
         (F.col("p.discount_amount") + F.coalesce(F.col("s.total_refunded_amount"), F.lit(0)))
        ).alias("net_revenue"),
        
        # --- MARGIN PERC (Pro KPI) ---
        F.round(
            ((F.col("o.order_value") - F.coalesce(F.col("s.total_refunded_amount"), F.lit(0))) / 
             F.col("o.order_value")) * 100, 2
        ).alias("order_margin_percentage"),

        # AUDIT
        F.current_timestamp().alias("fact_load_timestamp")
    )
)

# 5. WRITING AS MANAGED TABLE
# -----------------------------------------------------
print(f"📦 Writing to Managed Table: {TARGET_FACT}")
(fact_financials_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_FACT))

# 6. OPTIMIZATION
# -----------------------------------------------------
print(f"⚡ Optimizing {TARGET_FACT}...")
spark.sql(f"OPTIMIZE {TARGET_FACT} ZORDER BY (city, created_ts)")

print(f"✅ Financial Fact Build Successful.")