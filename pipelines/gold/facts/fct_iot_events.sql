-- =============================================================================
-- Gold Layer: Fact - IoT Events
-- =============================================================================
-- Fact table for IoT sensor readings with dimension keys
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fct_iot_events
COMMENT 'Gold layer: IoT events fact table'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH events AS (
  SELECT * FROM LIVE.stg_iot_events
),

-- Get threshold info for breach detection
thresholds AS (
  SELECT * FROM LIVE.stg_alert_thresholds
  WHERE NOT _is_deleted
)

SELECT
  -- Fact key
  md5(e.event_id) AS event_key,
  e.event_id,
  
  -- Dimension keys
  COALESCE(md5(e.device_id), md5('UNKNOWN')) AS device_key,
  COALESCE(md5(e.location_id), md5('UNKNOWN')) AS location_key,
  md5(CAST(e.event_date AS STRING)) AS date_key,
  
  -- Degenerate dimensions
  e.device_id,
  e.location_id,
  e.sensor_type,
  e.sensor_unit,
  
  -- Time attributes
  e.event_timestamp,
  e.event_date,
  HOUR(e.event_timestamp) AS event_hour,
  
  -- Measures
  e.sensor_value,
  e.battery_level,
  
  -- Quality indicators
  e.quality_flag,
  e.is_valid_reading,
  e.battery_status,
  e.firmware_version,
  
  -- Threshold breach detection
  CASE
    WHEN t.threshold_id IS NOT NULL AND (
      e.sensor_value < t.min_value OR e.sensor_value > t.max_value
    ) THEN TRUE
    ELSE FALSE
  END AS is_threshold_breach,
  
  t.threshold_id,
  t.min_value AS threshold_min,
  t.max_value AS threshold_max,
  
  -- Audit
  e._ingested_at,
  current_timestamp() AS _transformed_at

FROM events e
LEFT JOIN thresholds t ON e.sensor_type = t.sensor_type;
