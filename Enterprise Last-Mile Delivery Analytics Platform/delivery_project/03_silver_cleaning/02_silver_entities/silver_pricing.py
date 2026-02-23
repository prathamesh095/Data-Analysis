# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.pricing_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.pricing_quarantine"
TARGET_TABLE = "delivery_analytics.silver.pricing"

print(f"🚀 Starting Optimized Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering out records found in {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("pricing_event_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "pricing_event_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all records.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
standardized_df = (clean_bronze_df
    .withColumn("promo_code_id", F.upper(F.trim(F.col("promo_code_id"))))
    .withColumn("promo_type", F.initcap(F.trim(F.col("promo_type"))))
    # Monetary casting (Double for currencies)
    .withColumn("base_fare", F.col("base_fare").cast("double"))
    .withColumn("distance_fare", F.col("distance_fare").cast("double"))
    .withColumn("time_fare", F.col("time_fare").cast("double"))
    .withColumn("surge_multiplier", F.col("surge_multiplier").cast("double"))
    .withColumn("service_fee", F.col("service_fee").cast("double"))
    .withColumn("platform_fee", F.col("platform_fee").cast("double"))
    .withColumn("discount_amount", F.col("discount_amount").cast("double"))
    .withColumn("company_subsidy", F.col("company_subsidy").cast("double"))
    .withColumn("rider_bonus", F.col("rider_bonus").cast("double"))
    .withColumn("final_customer_price", F.col("final_customer_price").cast("double"))
    # Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION (Latest Pricing Event per Order)
# -----------------------------------------------------
window_spec = Window.partitionBy("pricing_event_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "pricing_event_id", "order_id", "pricing_ts", "base_fare", 
        "distance_fare", "time_fare", "surge_multiplier", "service_fee", 
        "platform_fee", "discount_amount", "promo_code_id", "promo_type", 
        "company_subsidy", "rider_bonus", "final_customer_price", 
        "pricing_version", "silver_ingestion_ts", "record_source"
    )
)

# -----------------------------------------------------
# 5. UPSERT (MERGE)
# -----------------------------------------------------
if not spark.catalog.tableExists(TARGET_TABLE):
    print(f"📦 Initializing {TARGET_TABLE}")
    (final_silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(TARGET_TABLE))
else:
    print(f"🔄 Merging updates into {TARGET_TABLE}")
    target_delta = DeltaTable.forName(spark, TARGET_TABLE)
    (target_delta.alias("t")
        .merge(final_silver_df.alias("s"), "t.pricing_event_id = s.pricing_event_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (order_id)")

print(f"✅ silver_pricing processing complete.")