# Databricks notebook source
# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. CONFIGURATION
BRONZE_TABLE = "delivery_analytics.bronze.support_raw"
QUARANTINE_TABLE = "delivery_analytics.bronze.support_quarantine"
TARGET_TABLE = "delivery_analytics.silver.support"

print(f"🚀 Starting Optimized Silver transformation for {TARGET_TABLE}")

# -----------------------------------------------------
# 2. LOAD & DYNAMIC QUARANTINE FILTERING
# -----------------------------------------------------
bronze_df = spark.table(BRONZE_TABLE)

if spark.catalog.tableExists(QUARANTINE_TABLE):
    print(f"🛡️ Filtering records from {QUARANTINE_TABLE}")
    quarantine_ids = spark.table(QUARANTINE_TABLE).select("ticket_id").distinct()
    clean_bronze_df = bronze_df.join(quarantine_ids, "ticket_id", "left_anti")
else:
    print(f"⚠️ {QUARANTINE_TABLE} not found. Processing all tickets.")
    clean_bronze_df = bronze_df

# -----------------------------------------------------
# 3. STANDARDIZATION & CASTING
# -----------------------------------------------------
standardized_df = (clean_bronze_df
    .withColumn("issue_category", F.initcap(F.trim(F.col("issue_category"))))
    .withColumn("issue_subcategory", F.initcap(F.trim(F.col("issue_subcategory"))))
    .withColumn("resolution_status", F.initcap(F.trim(F.col("resolution_status"))))
    .withColumn("priority_level", F.upper(F.trim(F.col("priority_level"))))
    # Numeric & Boolean casting
    .withColumn("csat_score", F.col("csat_score").cast("int"))
    .withColumn("compensation_amount", F.col("compensation_amount").cast("double"))
    .withColumn("reopened_flag", F.col("reopened_flag").cast("boolean"))
    # Audit Metadata
    .withColumn("silver_ingestion_ts", F.current_timestamp())
    .withColumn("record_source", F.col("source_file"))
)

# -----------------------------------------------------
# 4. DEDUPLICATION
# -----------------------------------------------------
# Picking the latest version of a support ticket
window_spec = Window.partitionBy("ticket_id").orderBy(F.col("ingestion_ts").desc())

final_silver_df = (standardized_df
    .withColumn("row_num", F.row_number().over(window_spec))
    .filter("row_num == 1")
    .drop("row_num")
    .select(
        "ticket_id", "order_id", "customer_id", "created_ts", 
        "issue_category", "issue_subcategory", "priority_level", 
        "ticket_channel", "agent_id", "resolution_status", 
        "resolution_ts", "compensation_amount", "csat_score", 
        "reopened_flag", "notes_text", "silver_ingestion_ts", "record_source"
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
        .merge(final_silver_df.alias("s"), "t.ticket_id = s.ticket_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# -----------------------------------------------------
# 6. OPTIMIZE
# -----------------------------------------------------
spark.sql(f"OPTIMIZE {TARGET_TABLE} ZORDER BY (order_id, issue_category)")

print(f"✅ silver_support processing complete.")