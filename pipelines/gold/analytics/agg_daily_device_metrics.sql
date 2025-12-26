-- =============================================================================
-- Gold Layer: Analytics - Daily Device Metrics
-- =============================================================================
-- Pre-aggregated daily metrics per device for dashboards
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW agg_daily_device_metrics
COMMENT 'Gold layer: Daily aggregated device metrics for operational dashboards'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH daily_events AS (
  SELECT
    device_key,
    device_id,
    event_date,
    sensor_type,
    
    COUNT(*) AS event_count,
    COUNT(CASE WHEN is_valid_reading THEN 1 END) AS valid_event_count,
    COUNT(CASE WHEN NOT is_valid_reading THEN 1 END) AS invalid_event_count,
    COUNT(CASE WHEN is_threshold_breach THEN 1 END) AS threshold_breach_count,
    
    AVG(sensor_value) AS avg_sensor_value,
    MIN(sensor_value) AS min_sensor_value,
    MAX(sensor_value) AS max_sensor_value,
    STDDEV(sensor_value) AS stddev_sensor_value,
    
    AVG(battery_level) AS avg_battery_level,
    MIN(battery_level) AS min_battery_level,
    
    MIN(event_timestamp) AS first_event_at,
    MAX(event_timestamp) AS last_event_at
    
  FROM LIVE.fct_iot_events
  GROUP BY device_key, device_id, event_date, sensor_type
),

daily_alerts AS (
  SELECT
    device_key,
    alert_date AS event_date,
    
    COUNT(*) AS alert_count,
    SUM(is_critical) AS critical_alert_count,
    SUM(is_open) AS open_alert_count,
    SUM(severity_weight) AS total_severity_score
    
  FROM LIVE.fct_alerts
  GROUP BY device_key, alert_date
),

devices AS (
  SELECT 
    device_key,
    device_id,
    device_name,
    device_type,
    customer_key,
    customer_name,
    location_key,
    location_name
  FROM LIVE.dim_devices
)

SELECT
  -- Keys
  md5(CONCAT(e.device_id, '-', e.event_date, '-', e.sensor_type)) AS metric_key,
  e.device_key,
  md5(CAST(e.event_date AS STRING)) AS date_key,
  
  -- Device attributes
  e.device_id,
  d.device_name,
  d.device_type,
  d.customer_key,
  d.customer_name,
  d.location_key,
  d.location_name,
  
  -- Time
  e.event_date,
  e.sensor_type,
  
  -- Event metrics
  e.event_count,
  e.valid_event_count,
  e.invalid_event_count,
  e.threshold_breach_count,
  
  -- Data quality
  ROUND(e.valid_event_count * 100.0 / NULLIF(e.event_count, 0), 2) AS data_quality_pct,
  
  -- Sensor statistics
  ROUND(e.avg_sensor_value, 4) AS avg_sensor_value,
  ROUND(e.min_sensor_value, 4) AS min_sensor_value,
  ROUND(e.max_sensor_value, 4) AS max_sensor_value,
  ROUND(e.stddev_sensor_value, 4) AS stddev_sensor_value,
  ROUND(e.max_sensor_value - e.min_sensor_value, 4) AS sensor_value_range,
  
  -- Battery metrics
  ROUND(e.avg_battery_level, 2) AS avg_battery_level,
  e.min_battery_level,
  
  -- Activity
  e.first_event_at,
  e.last_event_at,
  
  -- Alert metrics
  COALESCE(a.alert_count, 0) AS alert_count,
  COALESCE(a.critical_alert_count, 0) AS critical_alert_count,
  COALESCE(a.open_alert_count, 0) AS open_alert_count,
  COALESCE(a.total_severity_score, 0) AS total_severity_score,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM daily_events e
LEFT JOIN daily_alerts a ON e.device_key = a.device_key AND e.event_date = a.event_date
LEFT JOIN devices d ON e.device_key = d.device_key;
