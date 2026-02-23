# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. CONFIGURATION
# -----------------------------------------------------
CATALOG = "delivery_analytics"
SCHEMA  = "silver"

# Source Entities
ORDERS_TBL   = f"{CATALOG}.{SCHEMA}.orders"
DELIV_TBL    = f"{CATALOG}.{SCHEMA}.deliveries"
CANCEL_TBL   = f"{CATALOG}.{SCHEMA}.cancellations"

# Target Managed Table
TARGET_FACT  = f"{CATALOG}.{SCHEMA}.fact_delivery_performance"

print(f"🚀 Building {TARGET_FACT}...")

# 2. DEPENDENCY VALIDATION
# -----------------------------------------------------
required_tables = [ORDERS_TBL, DELIV_TBL, CANCEL_TBL]
for tbl in required_tables:
    if not spark.catalog.tableExists(tbl):
        raise Exception(f"❌ Dependency Error: {tbl} not found. Please run Silver Entity jobs.")

# 3. DATA EXTRACTION
# -----------------------------------------------------
df_orders = spark.table(ORDERS_TBL).alias("o")
df_deliv  = spark.table(DELIV_TBL).alias("d")
df_cancel = spark.table(CANCEL_TBL).alias("c")

# 4. TRANSFORMATION & KPI LOGIC
# -----------------------------------------------------
fact_performance_df = (df_orders
    .join(df_deliv, "order_id", "left")
    .join(df_cancel, "order_id", "left")
    .select(
        "o.order_id",
        "o.customer_id",
        "d.rider_id",
        "o.city",
        "o.created_ts",
        "o.promised_ts",
        "d.drop_ts",
        "o.order_status",
        "d.delivery_status",
        
        # --- DISTANCE PERFORMANCE ---
        "o.estimated_distance_km",
        "d.distance_km_actual",
        (F.col("d.distance_km_actual") - F.col("o.estimated_distance_km")).alias("distance_variance_km"),
        
        # --- TIME PERFORMANCE (Minutes) ---
        "o.estimated_duration_min",
        "d.duration_min_actual",
        ((F.unix_timestamp("d.drop_ts") - F.unix_timestamp("o.created_ts")) / 60).alias("actual_total_time_min"),
        
        # --- ON-TIME DELIVERY (OTD) LOGIC ---
        F.when(F.col("d.drop_ts") <= F.col("o.promised_ts"), 1)
         .when(F.col("d.drop_ts") > F.col("o.promised_ts"), 0)
         .otherwise(None).alias("is_on_time_flag"),
        
        # --- CANCELLATION INSIGHTS ---
        "c.cancel_reason_code",
        "c.cancel_actor",
        
        # AUDIT
        F.current_timestamp().alias("fact_load_timestamp")
    )
)

# 5. WRITING AS MANAGED TABLE
# -----------------------------------------------------
print(f"📦 Writing to Managed Table: {TARGET_FACT}")
(fact_performance_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_FACT))

# 6. OPTIMIZATION
# -----------------------------------------------------
print(f"⚡ Optimizing {TARGET_FACT}...")
spark.sql(f"OPTIMIZE {TARGET_FACT} ZORDER BY (city, order_status)")

print(f"✅ Fact Build Successful.")