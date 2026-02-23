# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

GOLD_DB = "delivery_analytics.gold"
SILVER_DB = "delivery_analytics.silver"
ORDER_MART  = f"{GOLD_DB}.gold_order_facts"
SUPPORT_TBL = f"{SILVER_DB}.support"
TARGET_TABLE = f"{GOLD_DB}.gold_customer_experience"


def transform_cx_kpis(orders: DataFrame, support: DataFrame) -> DataFrame:

    # -------------------------------------------------
    # Step A: Enrich support with city from orders
    # -------------------------------------------------
    support_enriched = (
        support.alias("s")
        .join(
            orders.select("order_id", "city").alias("o"),
            F.col("s.order_id") == F.col("o.order_id"),
            "left"
        )
    )

    # -------------------------------------------------
    # Step B: Aggregate Support
    # -------------------------------------------------
    support_agg = (
        support_enriched
        .groupBy(
            F.col("city"),
            F.to_date("created_ts").alias("report_date")
        )
        .agg(
            F.count("ticket_id").alias("total_support_tickets"),
            F.avg("csat_score").alias("avg_csat_score"),
            F.sum(F.when(F.col("issue_category") == "refund", 1).otherwise(0))
                .alias("total_refunds_issued")
        )
    )

    # -------------------------------------------------
    # Step C: Aggregate Orders
    # -------------------------------------------------
    order_agg = (
        orders
        .groupBy(
            "city",
            F.col("order_date").alias("report_date")
        )
        .agg(
            F.count("order_id").alias("total_orders")
        )
    )

    # -------------------------------------------------
    # Step D: Join KPIs
    # -------------------------------------------------
    joined = support_agg.join(
        order_agg,
        ["city", "report_date"],
        "inner"
    )

    # -------------------------------------------------
    # Step E: Final KPIs (ANSI safe)
    # -------------------------------------------------
    result = (
        joined
        .withColumn(
            "contact_rate_pct",
            F.round(
                F.try_divide(
                    F.col("total_support_tickets") * 100.0,
                    F.col("total_orders")
                ),
                2
            )
        )
        .withColumn(
            "refund_rate_pct",
            F.round(
                F.try_divide(
                    F.col("total_refunds_issued") * 100.0,
                    F.col("total_orders")
                ),
                2
            )
        )
        .select(
            "city",
            "report_date",
            F.round("avg_csat_score", 2).alias("avg_csat"),
            "total_support_tickets",
            "contact_rate_pct",
            "refund_rate_pct",
            F.current_timestamp().alias("gold_ingestion_ts")
        )
    )

    return result


# ================= EXECUTION =================
df_orders = spark.table(ORDER_MART)
df_support = spark.table(SUPPORT_TBL)

final_cx_df = transform_cx_kpis(df_orders, df_support)

(
    final_cx_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"✅ Success! Table {TARGET_TABLE} built.")

# COMMAND ----------

