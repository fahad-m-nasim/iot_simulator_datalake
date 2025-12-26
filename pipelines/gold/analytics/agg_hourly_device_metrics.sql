-- =============================================================================
-- Gold Layer: Analytics - Hourly Device Metrics
-- =============================================================================
-- Near real-time hourly aggregations for operational dashboards
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW agg_hourly_device_metrics
COMMENT 'Gold layer: Hourly device metrics for operational monitoring'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH hourly_events AS (
  SELECT
    device_key,
    device_id,
    location_id,
    sensor_type,
    event_date AS metric_date,
    DATE_TRUNC('HOUR', event_timestamp) AS metric_hour,
    
    -- Event counts
    COUNT(*) AS event_count,
    COUNT(CASE WHEN is_valid_reading THEN 1 END) AS valid_readings,
    COUNT(CASE WHEN NOT is_valid_reading THEN 1 END) AS invalid_readings,
    COUNT(CASE WHEN is_threshold_breach THEN 1 END) AS breach_count,
    
    -- Sensor statistics
    AVG(sensor_value) AS avg_value,
    MIN(sensor_value) AS min_value,
    MAX(sensor_value) AS max_value,
    STDDEV(sensor_value) AS stddev_value,
    PERCENTILE_APPROX(sensor_value, 0.5) AS median_value,
    PERCENTILE_APPROX(sensor_value, 0.95) AS p95_value,
    
    -- Device health
    AVG(battery_level) AS avg_battery_level,
    MIN(battery_level) AS min_battery_level,
    
    -- Timing
    MIN(event_timestamp) AS first_event_at,
    MAX(event_timestamp) AS last_event_at,
    
    MAX(_ingested_at) AS _last_ingested_at
    
  FROM LIVE.fct_iot_events
  GROUP BY 
    device_key, device_id, location_id, sensor_type, 
    event_date, DATE_TRUNC('HOUR', event_timestamp)
),

devices AS (
  SELECT 
    device_key,
    device_name,
    device_type,
    customer_id
  FROM LIVE.dim_devices
),

locations AS (
  SELECT
    location_id,
    location_name,
    building,
    zone
  FROM LIVE.dim_locations
)

SELECT
  -- Surrogate key
  md5(CONCAT(h.device_id, '-', h.sensor_type, '-', CAST(h.metric_hour AS STRING))) AS hourly_metric_key,
  
  -- Dimension keys
  h.device_key,
  md5(CAST(h.metric_date AS STRING)) AS date_key,
  
  -- Time attributes
  h.metric_date,
  h.metric_hour,
  HOUR(h.metric_hour) AS hour_of_day,
  DAYOFWEEK(h.metric_date) AS day_of_week,
  CASE 
    WHEN DAYOFWEEK(h.metric_date) IN (1, 7) THEN 'weekend'
    ELSE 'weekday'
  END AS day_type,
  
  -- Device attributes
  h.device_id,
  d.device_name,
  d.device_type,
  d.customer_id,
  
  -- Location attributes
  h.location_id,
  l.location_name,
  l.building,
  l.zone,
  
  -- Sensor type
  h.sensor_type,
  
  -- Metrics
  h.event_count,
  h.valid_readings,
  h.invalid_readings,
  h.breach_count,
  
  -- Data quality score
  ROUND(h.valid_readings * 100.0 / NULLIF(h.event_count, 0), 2) AS data_quality_pct,
  
  -- Sensor statistics
  ROUND(h.avg_value, 4) AS avg_value,
  ROUND(h.min_value, 4) AS min_value,
  ROUND(h.max_value, 4) AS max_value,
  ROUND(h.stddev_value, 4) AS stddev_value,
  ROUND(h.median_value, 4) AS median_value,
  ROUND(h.p95_value, 4) AS p95_value,
  ROUND(h.max_value - h.min_value, 4) AS value_range,
  
  -- Device health
  ROUND(h.avg_battery_level, 2) AS avg_battery_level,
  h.min_battery_level,
  
  -- Activity metrics
  h.first_event_at,
  h.last_event_at,
  UNIX_TIMESTAMP(h.last_event_at) - UNIX_TIMESTAMP(h.first_event_at) AS active_seconds,
  
  -- Audit
  h._last_ingested_at,
  current_timestamp() AS _transformed_at

FROM hourly_events h
LEFT JOIN devices d ON h.device_key = d.device_key
LEFT JOIN locations l ON h.location_id = l.location_id;
