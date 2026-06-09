# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Transformation
# MAGIC Reads from bronze, applies data quality rules, casts types, and derives fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "my_catalog"
SCHEMA  = "my_schema"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_orders"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType

bronze_df = spark.table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cast types and derive fields

# COMMAND ----------

silver_df = (
    bronze_df
    # Drop duplicate order IDs — keep the most recently ingested record
    .dropDuplicates(["order_id"])
    # Cast numeric columns
    .withColumn("quantity",    F.col("quantity").cast(IntegerType()))
    .withColumn("unit_price",  F.col("unit_price").cast(DecimalType(10, 2)))
    # Cast date
    .withColumn("order_date",  F.to_date("order_date", "yyyy-MM-dd"))
    # Derive order total
    .withColumn("order_total", F.round(F.col("quantity") * F.col("unit_price"), 2))
    # Normalise status to lowercase
    .withColumn("status",      F.lower(F.trim(F.col("status"))))
    # Drop rows missing required fields
    .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
    # Add processing timestamp; carry forward ingestion timestamp
    .withColumn("_processed_at", F.current_timestamp())
    .select(
        "order_id", "customer_id", "customer_name",
        "product_id", "product_name", "category",
        "quantity", "unit_price", "order_date",
        "status", "region", "order_total",
        "_ingested_at", "_processed_at",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Silver table

# COMMAND ----------

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Wrote {silver_df.count()} rows to {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic data quality checks

# COMMAND ----------

null_check = silver_df.filter(
    F.col("quantity").isNull() | F.col("unit_price").isNull() | F.col("order_date").isNull()
).count()

print(f"Rows with null quantity / unit_price / order_date: {null_check}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

display(spark.table(SILVER_TABLE).limit(10))
