# Databricks notebook source
# MAGIC %md
# MAGIC # CDC Tables Bronze Ingestion
# MAGIC 
# MAGIC This notebook uses Delta Live Tables (DLT) with Auto Loader to ingest
# MAGIC CDC (Change Data Capture) data from Unity Catalog Volume.
# MAGIC 
# MAGIC ## Source Tables
# MAGIC - customers, products, orders (business tables)
# MAGIC - dim_device, dim_location, dim_alert_threshold (dimension tables)
# MAGIC - alerts (fact table)
# MAGIC 
# MAGIC ## CDC Format
# MAGIC - Debezium format with `__op`, `__ts_ms`, `__deleted` fields

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog Volume path - data synced from S3 via Lambda
volume_path = "/Volumes/dev_catalog/landing/vol01"
checkpoint_path = f"{volume_path}/_checkpoints/cdc"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers Table

# COMMAND ----------

@dlt.table(
    name="cdc_customers",
    comment="Bronze layer: CDC events for customers table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_cdc_operation", "__op IN ('r', 'c', 'u', 'd')")
def bronze_cdc_customers():
    """Ingest CDC events for customers table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_customers/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/customers/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_cdc_timestamp", 
            when(col("__ts_ms").isNotNull(), 
                 from_unixtime(col("__ts_ms") / 1000))
            .otherwise(None))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Products Table

# COMMAND ----------

@dlt.table(
    name="cdc_products",
    comment="Bronze layer: CDC events for products table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
def bronze_cdc_products():
    """Ingest CDC events for products table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_products/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/products/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders Table

# COMMAND ----------

@dlt.table(
    name="cdc_orders",
    comment="Bronze layer: CDC events for orders table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
def bronze_cdc_orders():
    """Ingest CDC events for orders table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_orders/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/orders/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Devices Table

# COMMAND ----------

@dlt.table(
    name="cdc_devices",
    comment="Bronze layer: CDC events for devices table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_device_id", "device_id IS NOT NULL")
def bronze_cdc_devices():
    """Ingest CDC events for dim_device table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_dim_device/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/devices/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locations Table

# COMMAND ----------

@dlt.table(
    name="cdc_locations",
    comment="Bronze layer: CDC events for locations table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_location_id", "location_id IS NOT NULL")
def bronze_cdc_locations():
    """Ingest CDC events for dim_location table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_dim_location/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/locations/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerts Table

# COMMAND ----------

@dlt.table(
    name="cdc_alerts",
    comment="Bronze layer: CDC events for alerts table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_alert_id", "alert_id IS NOT NULL")
def bronze_cdc_alerts():
    """Ingest CDC events for alerts table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_alerts/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/alerts/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert Thresholds Table

# COMMAND ----------

@dlt.table(
    name="cdc_alert_thresholds",
    comment="Bronze layer: CDC events for alert thresholds table",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_threshold_id", "threshold_id IS NOT NULL")
def bronze_cdc_alert_thresholds():
    """Ingest CDC events for dim_alert_threshold table from Volume."""
    source_path = f"{volume_path}/cdc/cdc_dim_alert_threshold/"
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/thresholds/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )
