-- =============================================================================
-- Bronze Layer: CDC Alerts Ingestion
-- =============================================================================
-- Ingests CDC events for alerts fact table from Unity Catalog Volume
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_alerts
(
  alert_id STRING NOT NULL,
  device_id STRING,
  threshold_id STRING,
  alert_type STRING,
  severity STRING,
  message STRING,
  status STRING,
  triggered_value STRING,
  created_at BIGINT,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  CONSTRAINT valid_alert_id EXPECT (alert_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for alerts table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  alert_id,
  device_id,
  threshold_id,
  alert_type,
  severity,
  message,
  status,
  triggered_value,
  created_at,
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
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_alerts/',
  format => 'json',
  inferColumnTypes => true
);
