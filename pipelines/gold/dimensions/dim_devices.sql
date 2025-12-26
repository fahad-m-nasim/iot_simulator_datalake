-- =============================================================================
-- Gold Layer: Dimension - Devices
-- =============================================================================
-- Conformed device dimension with enriched metrics
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_devices
COMMENT 'Gold layer: Device dimension table with health metrics'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH devices AS (
  SELECT * FROM LIVE.stg_devices
  WHERE NOT _is_deleted
),

-- Get latest event metrics per device
device_event_metrics AS (
  SELECT
    device_id,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN is_valid_reading THEN 1 END) AS valid_events,
    AVG(battery_level) AS avg_battery_level,
    MIN(battery_level) AS min_battery_level,
    MIN(event_timestamp) AS first_event_at,
    MAX(event_timestamp) AS last_event_at
  FROM LIVE.stg_iot_events
  GROUP BY device_id
),

-- Get alert metrics per device
device_alert_metrics AS (
  SELECT
    device_id,
    COUNT(*) AS total_alerts,
    COUNT(CASE WHEN severity = 'critical' THEN 1 END) AS critical_alerts,
    COUNT(CASE WHEN status = 'open' THEN 1 END) AS open_alerts
  FROM LIVE.stg_alerts
  WHERE NOT _is_deleted
  GROUP BY device_id
),

-- Get location details
locations AS (
  SELECT * FROM LIVE.stg_locations
  WHERE NOT _is_deleted
),

-- Get customer details
customers AS (
  SELECT customer_id, company_name FROM LIVE.stg_customers
  WHERE NOT _is_deleted
)

SELECT
  -- Surrogate key
  md5(d.device_id) AS device_key,
  
  -- Business key
  d.device_id,
  
  -- Device attributes
  d.device_name,
  d.device_type,
  d.manufacturer,
  d.model,
  d.status,
  
  -- Location foreign key and denormalized attributes
  md5(d.location_id) AS location_key,
  d.location_id,
  l.location_name,
  l.building,
  l.zone,
  
  -- Customer foreign key and denormalized attributes
  md5(d.customer_id) AS customer_key,
  d.customer_id,
  c.company_name AS customer_name,
  
  -- Event metrics
  COALESCE(e.total_events, 0) AS total_events,
  COALESCE(e.valid_events, 0) AS valid_events,
  ROUND(COALESCE(e.valid_events, 0) * 100.0 / NULLIF(e.total_events, 0), 2) AS data_quality_pct,
  
  -- Battery metrics
  ROUND(COALESCE(e.avg_battery_level, 0), 2) AS avg_battery_level,
  COALESCE(e.min_battery_level, 0) AS min_battery_level,
  
  -- Activity timestamps
  e.first_event_at,
  e.last_event_at,
  
  -- Alert metrics
  COALESCE(a.total_alerts, 0) AS total_alerts,
  COALESCE(a.critical_alerts, 0) AS critical_alerts,
  COALESCE(a.open_alerts, 0) AS open_alerts,
  
  -- Device health score (0-100)
  ROUND(
    (COALESCE(e.valid_events, 0) * 100.0 / NULLIF(e.total_events, 0)) * 0.4 +
    COALESCE(e.avg_battery_level, 50) * 0.3 +
    CASE 
      WHEN COALESCE(a.critical_alerts, 0) = 0 THEN 100
      WHEN COALESCE(a.critical_alerts, 0) < 5 THEN 70
      WHEN COALESCE(a.critical_alerts, 0) < 10 THEN 40
      ELSE 10
    END * 0.3
  , 2) AS health_score,
  
  -- Active status based on recent events
  CASE 
    WHEN d.status = 'active' AND e.last_event_at >= DATE_SUB(current_timestamp(), 1) THEN TRUE
    ELSE FALSE
  END AS is_active,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM devices d
LEFT JOIN device_event_metrics e ON d.device_id = e.device_id
LEFT JOIN device_alert_metrics a ON d.device_id = a.device_id
LEFT JOIN locations l ON d.location_id = l.location_id
LEFT JOIN customers c ON d.customer_id = c.customer_id;
