-- =============================================================================
-- Bronze Layer: CDC Devices Ingestion
-- =============================================================================
-- Ingests CDC events for devices dimension table from Unity Catalog Volume
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_devices
(
  device_id STRING NOT NULL,
  device_name STRING,
  device_type STRING,
  manufacturer STRING,
  model STRING,
  location_id STRING,
  customer_id STRING,
  status STRING,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  CONSTRAINT valid_device_id EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for devices table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  device_id,
  device_name,
  device_type,
  manufacturer,
  model,
  location_id,
  customer_id,
  status,
  __op,
  __ts_ms,
  __deleted,
  current_timestamp() AS _ingested_at,
  current_date() AS _ingestion_date,
  _metadata.file_path AS _source_file,
  CASE 
    WHEN __ts_ms IS NOT NULL THEN from_unixtime(__ts_ms / 1000)
    ELSE NULL 
  END AS _cdc_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_dim_device/',
  format => 'json',
  inferColumnTypes => true
);
