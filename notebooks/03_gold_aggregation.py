# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Aggregation & Analytics
# MAGIC Builds two Gold tables from Silver: a daily sales summary and a customer lifetime summary.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "my_catalog"
SCHEMA  = "my_schema"

SILVER_TABLE          = f"{CATALOG}.{SCHEMA}.silver_orders"
GOLD_SALES_SUMMARY    = f"{CATALOG}.{SCHEMA}.gold_sales_summary"
GOLD_CUSTOMER_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_customer_summary"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver (completed orders only)

# COMMAND ----------

from pyspark.sql import functions as F

silver_df = spark.table(SILVER_TABLE).filter(F.col("status") == "completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Daily Sales Summary by Region and Category

# COMMAND ----------

sales_summary_df = (
    silver_df
    .groupBy("order_date", "region", "category")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("order_total"), 2).alias("total_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_order_value"),
    )
    .withColumnRenamed("order_date", "summary_date")
    .withColumn("_processed_at", F.current_timestamp())
    .orderBy("summary_date", "region", "category")
)

(
    sales_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SALES_SUMMARY)
)

print(f"Wrote {sales_summary_df.count()} rows to {GOLD_SALES_SUMMARY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Customer Lifetime Summary

# COMMAND ----------

all_orders_df = spark.table(SILVER_TABLE)

customer_summary_df = (
    all_orders_df
    .groupBy("customer_id", "customer_name")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.count(F.when(F.col("status") == "completed", 1)).alias("completed_orders"),
        F.round(F.sum(F.when(F.col("status") == "completed", F.col("order_total"))), 2).alias("total_spend"),
        F.round(F.avg(F.when(F.col("status") == "completed", F.col("order_total"))), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .withColumn("_processed_at", F.current_timestamp())
    .orderBy("total_spend", ascending=False)
)

(
    customer_summary_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER_SUMMARY)
)

print(f"Wrote {customer_summary_df.count()} rows to {GOLD_CUSTOMER_SUMMARY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

print("=== Daily Sales Summary ===")
display(spark.table(GOLD_SALES_SUMMARY).limit(10))

print("=== Customer Summary ===")
display(spark.table(GOLD_CUSTOMER_SUMMARY).limit(10))
