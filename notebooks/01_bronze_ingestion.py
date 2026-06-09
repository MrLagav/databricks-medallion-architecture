# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC Reads the source CSV and writes it as-is into the bronze Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "my_catalog"
SCHEMA  = "my_schema"
TABLE   = f"{CATALOG}.{SCHEMA}.bronze_orders"

SOURCE_PATH = "/Volumes/my_catalog/my_schema/raw/sample_data.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data

# COMMAND ----------

from pyspark.sql import functions as F

raw_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "false")   # keep all columns as strings in bronze
    .load(SOURCE_PATH)
)

bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp()) \
                  .withColumn("_source_file", F.lit(SOURCE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Bronze table

# COMMAND ----------

(
    bronze_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(TABLE)
)

print(f"Ingested {bronze_df.count()} rows into {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview

# COMMAND ----------

display(spark.table(TABLE).limit(10))
