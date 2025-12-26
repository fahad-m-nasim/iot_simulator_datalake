-- =============================================================================
-- Bronze Layer: IoT Events Ingestion
-- =============================================================================
-- Ingests raw IoT sensor events from Unity Catalog Volume using Auto Loader
-- Source: /Volumes/dev_catalog/landing/vol01/iot_events/
-- Target: Streaming table with incremental processing
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE iot_events
(
  event_id STRING NOT NULL,
  device_id STRING NOT NULL,
  timestamp STRING,
  event_timestamp TIMESTAMP,
  sensor_type STRING,
  value DOUBLE,
  unit STRING,
  location_id STRING,
  quality_flag STRING,
  firmware_version STRING,
  battery_level DOUBLE,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  
  -- Data quality constraints
  CONSTRAINT valid_event_id EXPECT (event_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_device_id EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (timestamp IS NOT NULL OR event_timestamp IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_sensor_type EXPECT (sensor_type IN ('temperature', 'humidity', 'pressure', 'motion')) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: Raw IoT sensor events ingested from Volume via Auto Loader'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
)
AS SELECT
  event_id,
  device_id,
  timestamp,
  TRY_CAST(timestamp AS TIMESTAMP) AS event_timestamp,
  sensor_type,
  value,
  unit,
  location_id,
  quality_flag,
  firmware_version,
  battery_level,
  current_timestamp() AS _ingested_at,
  current_date() AS _ingestion_date,
  _metadata.file_path AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/landing/vol01/iot_events/iot_events/',
  format => 'json',
  inferColumnTypes => true,
  schemaEvolutionMode => 'addNewColumns'
);
