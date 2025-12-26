# Databricks notebook source
# MAGIC %md
# MAGIC # IoT Events Bronze Ingestion
# MAGIC 
# MAGIC This notebook uses Delta Live Tables (DLT) with Auto Loader to ingest
# MAGIC IoT sensor events from Unity Catalog Volume into streaming tables.
# MAGIC 
# MAGIC ## Source
# MAGIC - Volume: `/Volumes/dev_catalog/landing/vol01/iot_events/`
# MAGIC - Format: JSON (newline-delimited)
# MAGIC 
# MAGIC ## Target
# MAGIC - Bronze table: `iot_events` (managed by Unity Catalog)

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog Volume path - data synced from S3 via Lambda
# Volume path format: /Volumes/<catalog>/<schema>/<volume>/<path>
volume_path = "/Volumes/dev_catalog/landing/vol01"

# Source path for IoT events (nested structure from Kafka Connect)
source_path = f"{volume_path}/iot_events/iot_events/"

# Checkpoint location within the volume
checkpoint_path = f"{volume_path}/_checkpoints/iot_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

iot_events_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sensor_type", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("location_id", StringType(), True),
    StructField("quality_flag", StringType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("battery_level", DoubleType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Streaming Table - IoT Events

# COMMAND ----------

@dlt.table(
    name="iot_events",
    comment="Bronze layer: Raw IoT sensor events ingested from Volume via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true"
    },
    partition_cols=["_ingestion_date"]
)
@dlt.expect("valid_event_id", "event_id IS NOT NULL")
@dlt.expect("valid_device_id", "device_id IS NOT NULL")
@dlt.expect("valid_timestamp", "timestamp IS NOT NULL")
def bronze_iot_events():
    """
    Ingest IoT events from Unity Catalog Volume using Auto Loader (Structured Streaming).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
        .withColumn("_source_file", col("_metadata.file_path"))
    )
