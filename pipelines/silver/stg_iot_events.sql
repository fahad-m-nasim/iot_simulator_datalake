-- =============================================================================
-- Silver Layer: Staging IoT Events
-- =============================================================================
-- Cleans and standardizes IoT events from Bronze layer
-- Applies transformations and data quality enrichment
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_iot_events
COMMENT 'Silver layer: Cleaned and standardized IoT sensor events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.iot_events
),

cleaned AS (
  SELECT
    -- Surrogate key for merge operations
    md5(event_id) AS _surrogate_key,
    
    -- Primary identifiers
    event_id,
    device_id,
    location_id,
    
    -- Event timestamp handling
    COALESCE(
      event_timestamp,
      TRY_CAST(timestamp AS TIMESTAMP),
      _ingested_at
    ) AS event_timestamp,
    DATE(COALESCE(
      event_timestamp,
      TRY_CAST(timestamp AS TIMESTAMP),
      _ingested_at
    )) AS event_date,
    
    -- Sensor data
    sensor_type,
    value AS sensor_value,
    unit AS sensor_unit,
    
    -- Quality indicators
    quality_flag,
    CASE 
      WHEN quality_flag = 'good' THEN TRUE
      ELSE FALSE
    END AS is_valid_reading,
    
    -- Device metadata
    firmware_version,
    battery_level,
    CASE
      WHEN battery_level < 20 THEN 'critical'
      WHEN battery_level < 40 THEN 'low'
      WHEN battery_level < 60 THEN 'medium'
      ELSE 'good'
    END AS battery_status,
    
    -- Audit columns
    _ingested_at,
    _source_file,
    current_timestamp() AS _transformed_at
    
  FROM source
  WHERE event_id IS NOT NULL
)

SELECT * FROM cleaned;
