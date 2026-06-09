# Databricks Medallion Architecture Test

This repository contains a simple test implementation of the **Medallion Architecture** using **Databricks**.

The goal of this project is to demonstrate how raw data can be ingested, cleaned, transformed, and prepared for analytics using the standard **Bronze**, **Silver**, and **Gold** layers.

## Architecture Overview

The Medallion Architecture is split into three main layers:

### Bronze Layer

The Bronze layer stores raw ingested data.

This layer is usually used for:

* Loading source data as-is
* Preserving original records
* Keeping audit/history data
* Handling schema evolution where needed

### Silver Layer

The Silver layer contains cleaned and validated data.

This layer is usually used for:

* Removing duplicates
* Fixing data types
* Handling null values
* Applying basic business rules
* Joining related datasets

### Gold Layer

The Gold layer contains business-ready data.

This layer is usually used for:

* Aggregations
* Reporting tables
* Dashboard-ready datasets
* Analytics and machine learning features

## Repository Structure

```text
.
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_aggregation.py
├── data/
│   └── sample_data.csv
├── sql/
│   └── create_tables.sql
└── README.md
```

## Requirements

* Databricks workspace
* Unity Catalog enabled
* Databricks SQL warehouse or cluster
* Delta Lake
* PySpark

## Example Workflow

1. Upload sample data into Databricks.
2. Run the Bronze ingestion notebook.
3. Run the Silver transformation notebook.
4. Run the Gold aggregation notebook.
5. Query the Gold tables for reporting or dashboarding.

## Example Layer Flow

```text
Raw CSV / Source Data
        ↓
Bronze Table
        ↓
Silver Table
        ↓
Gold Table
```

## Tables

Example table naming convention:

```text
catalog.schema.bronze_orders
catalog.schema.silver_orders
catalog.schema.gold_sales_summary
```

## Purpose

This repository is intended for learning and testing Databricks data engineering concepts, including:

* Medallion Architecture
* Delta tables
* PySpark transformations
* Unity Catalog table organisation
* Basic data quality checks
* Analytics-ready Gold tables

## Notes

This is a test project and should not be used as-is for production workloads.

For production use, consider adding:

* Data quality expectations
* Automated jobs
* CI/CD deployment
* Error handling
* Monitoring and alerting
* Incremental processing
* Proper access control through Unity Catalog

