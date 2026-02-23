# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.cancellations_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.cancellations_quarantine"
TARGET_TABLE = "delivery_analytics.silver.cancellations"

print(f"🚀 Starting Optimized Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering records from {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("cancel_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "cancel_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all records.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
standardized_df = (clean_bronze_df
    .withColumn("cancel_stage", F.lower(F.trim(F.col("cancel_stage"))))
    .withColumn("cancel_actor", F.initcap(F.trim(F.col("cancel_actor"))))
    .withColumn("cancel_reason_code", F.upper(F.trim(F.col("cancel_reason_code"))))
    # Financial casting
    .withColumn("refund_amount", F.col("refund_amount").cast("double"))
    .withColumn("compensation_paid", F.col("compensation_paid").cast("double"))
    .withColumn("auto_cancel_flag", F.col("auto_cancel_flag").cast("boolean"))
    # Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION
# -----------------------------------------------------
window_spec = Window.partitionBy("cancel_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "cancel_id", "order_id", "cancel_ts", "cancel_stage", 
        "cancel_actor", "cancel_reason_code", "cancel_reason_text", 
        "refund_amount", "compensation_paid", "auto_cancel_flag", 
        "linked_support_ticket", "silver_ingestion_ts", "record_source"
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
        .merge(final_silver_df.alias("s"), "t.cancel_id = s.cancel_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (order_id, cancel_reason_code)")

print(f"✅ silver_cancellations processing complete.")