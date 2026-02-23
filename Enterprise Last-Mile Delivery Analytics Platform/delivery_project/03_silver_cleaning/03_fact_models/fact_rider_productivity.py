# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# 1. CONFIGURATION
# -----------------------------------------------------
CATALOG = "delivery_analytics"
SCHEMA  = "silver"

# Source Entities
RIDERS_TBL = f"{CATALOG}.{SCHEMA}.riders"
SHIFTS_TBL = f"{CATALOG}.{SCHEMA}.shifts"
PINGS_TBL  = f"{CATALOG}.{SCHEMA}.pings"

# Target Managed Table
TARGET_FACT = f"{CATALOG}.{SCHEMA}.fact_rider_productivity"

print(f"🚀 Building {TARGET_FACT}...")

# 2. DEPENDENCY VALIDATION
# -----------------------------------------------------
required_tables = [RIDERS_TBL, SHIFTS_TBL, PINGS_TBL]
for tbl in required_tables:
    if not spark.catalog.tableExists(tbl):
        raise Exception(f"❌ Dependency Error: {tbl} not found.")

# 3. DATA EXTRACTION & AGGREGATION
# -----------------------------------------------------
df_riders = spark.table(RIDERS_TBL).alias("r")
df_shifts = spark.table(SHIFTS_TBL).alias("s")

# Aggregate Pings to get movement metrics per rider/shift
# Industry Pro: We calculate the max speed and ping count to verify "Actual Activity"
df_pings_agg = (spark.table(PINGS_TBL)
    .groupBy("rider_id", "delivery_id")
    .agg(
        F.count("ping_id").alias("total_pings"),
        F.max("speed_kmh").alias("max_recorded_speed_kmh"),
        F.avg("battery_level").alias("avg_battery_level")
    ).alias("p")
)

# 4. TRANSFORMATION & PRODUCTIVITY LOGIC
# -----------------------------------------------------
fact_rider_df = (df_shifts
    .join(df_riders, "rider_id", "inner")
    .join(df_pings_agg, "rider_id", "left")
    .select(
        "s.shift_id",
        "s.rider_id",
        "r.city",
        "r.vehicle_type",
        "s.shift_start_ts",
        "s.actual_login_ts",
        "s.actual_logout_ts",
        "s.scheduled_hours",
        
        # --- UTILIZATION METRICS ---
        # Calculate actual duration in hours
        ((F.unix_timestamp("s.actual_logout_ts") - F.unix_timestamp("s.actual_login_ts")) / 3600).alias("actual_hours_worked"),
        
        # Attendance Flags (using 1/0 for easy summation in BI)
        F.col("s.late_start_flag").cast("int").alias("is_late_start_flag"),
        F.col("s.overtime_flag").cast("int").alias("is_overtime_flag"),
        
        # Activity Validation
        F.when(F.col("p.total_pings") > 0, 1).otherwise(0).alias("has_gps_activity_flag"),
        "p.max_recorded_speed_kmh",
        
        # AUDIT
        F.current_timestamp().alias("fact_load_timestamp")
    )
)

# 5. WRITING AS MANAGED TABLE
# -----------------------------------------------------
print(f"📦 Writing to Managed Table: {TARGET_FACT}")
(fact_rider_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_FACT))

# 6. OPTIMIZATION
# -----------------------------------------------------
print(f"⚡ Optimizing {TARGET_FACT}...")
spark.sql(f"OPTIMIZE {TARGET_FACT} ZORDER BY (city, rider_id)")

print(f"✅ Rider Productivity Fact Build Successful.")