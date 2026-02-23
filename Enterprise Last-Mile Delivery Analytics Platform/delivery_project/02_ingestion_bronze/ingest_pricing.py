# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, lit



# COMMAND ----------

# ---------------- CONFIG ----------------
TABLE_NAME = "pricing_raw"
SOURCE_PATH = "/Volumes/delivery_analytics/bronze/raw_landing/raw_source/pricing/"
TARGET_TABLE = "delivery_analytics.bronze.pricing_raw"
TRACKER_TABLE = "delivery_analytics.bronze.file_ingestion_tracker"

# COMMAND ----------

schema = StructType([
    StructField("pricing_event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("pricing_ts", TimestampType()),
    StructField("base_fare", DoubleType()),
    StructField("distance_fare", DoubleType()),
    StructField("time_fare", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("service_fee", DoubleType()),
    StructField("platform_fee", DoubleType()),
    StructField("discount_amount", DoubleType()),
    StructField("promo_code_id", StringType()),
    StructField("promo_type", StringType()),
    StructField("company_subsidy", DoubleType()),
    StructField("rider_bonus", DoubleType()),
    StructField("final_customer_price", DoubleType()),
    StructField("pricing_version", StringType()),
    StructField("ingestion_ts", TimestampType()),
    StructField("source_file", StringType()),
    StructField("bronze_ingestion_ts", TimestampType())  # ← ADD
])


# COMMAND ----------

# ---------------- STEP 0: Create Bronze table with schema if missing ----------------
if not spark.catalog.tableExists(TARGET_TABLE):
    print(f"🧱 Creating {TARGET_TABLE} with schema...")
    empty_df = spark.createDataFrame([], schema)
    empty_df.write.format("delta").saveAsTable(TARGET_TABLE)


# COMMAND ----------

# ---------------- STEP 1: Load tracker ----------------
try:
    ingested_files = (
        spark.table(TRACKER_TABLE)
        .filter(f"table_name = '{TABLE_NAME}' AND status = 'SUCCESS'")
        .select("file_path")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    ingested_set = set(ingested_files)
except:
    print("⚠️ Tracker table missing — assuming first run.")
    ingested_set = set()

# COMMAND ----------


# ---------------- STEP 2: List new files ----------------
try:
    all_files = [f.path for f in dbutils.fs.ls(SOURCE_PATH)]
    new_files = [f for f in all_files if f not in ingested_set]
except Exception as e:
    print(f"❌ Cannot access source path: {e}")
    new_files = []

# COMMAND ----------

# ---------------- STEP 3: Batch incremental ingestion ----------------
if new_files:
    try:
        print(f"🚀 Ingesting {len(new_files)} new files")

        df = (spark.read
              .option("header", True)
              .schema(schema)
              .csv(new_files))

        df_final = df.withColumn("bronze_ingestion_ts", current_timestamp())

        df_final.write.mode("append").format("delta").saveAsTable(TARGET_TABLE)

        tracker_df = (
            spark.createDataFrame(
                [(TABLE_NAME, f, "SUCCESS") for f in new_files],
                ["table_name", "file_path", "status"]
            )
            .withColumn("ingestion_ts", current_timestamp())
        )

        tracker_df.write.mode("append").format("delta").saveAsTable(TRACKER_TABLE)

        print(f"✅ Successfully ingested {len(new_files)} files")

    except Exception as e:
        print(f"❌ Batch ingestion failed: {e}")

        failure_df = (
            spark.createDataFrame(
                [(TABLE_NAME, "BATCH_FAILURE", "FAILED")],
                ["table_name", "file_path", "status"]
            )
            .withColumn("ingestion_ts", current_timestamp())
        )

        failure_df.write.mode("append").format("delta").saveAsTable(TRACKER_TABLE)

else:
    print("✨ No new files to ingest — Bronze is up to date.")
