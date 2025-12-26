-- =============================================================================
-- Bronze Layer: CDC Alert Thresholds Ingestion
-- =============================================================================
-- Ingests CDC events for alert thresholds dimension from Unity Catalog Volume
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_alert_thresholds
(
  threshold_id STRING NOT NULL,
  sensor_type STRING,
  min_value STRING,
  max_value STRING,
  severity STRING,
  alert_message STRING,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  CONSTRAINT valid_threshold_id EXPECT (threshold_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for alert thresholds table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  threshold_id,
  sensor_type,
  min_value,
  max_value,
  severity,
  alert_message,
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
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_dim_alert_threshold/',
  format => 'json',
  inferColumnTypes => true
);
