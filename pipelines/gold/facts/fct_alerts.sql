-- =============================================================================
-- Gold Layer: Fact - Alerts
-- =============================================================================
-- Fact table for device alerts with dimension keys
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fct_alerts
COMMENT 'Gold layer: Alerts fact table'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH alerts AS (
  SELECT * FROM LIVE.stg_alerts
  WHERE NOT _is_deleted
),

-- Get device info for customer key
devices AS (
  SELECT device_id, customer_id FROM LIVE.stg_devices
  WHERE NOT _is_deleted
)

SELECT
  -- Fact key
  md5(a.alert_id) AS alert_key,
  a.alert_id,
  
  -- Dimension keys
  COALESCE(md5(a.device_id), md5('UNKNOWN')) AS device_key,
  COALESCE(md5(d.customer_id), md5('UNKNOWN')) AS customer_key,
  md5(CAST(a.alert_date AS STRING)) AS date_key,
  
  -- Degenerate dimensions
  a.device_id,
  d.customer_id,
  a.threshold_id,
  
  -- Alert attributes
  a.alert_type,
  a.severity,
  a.message,
  a.status,
  a.triggered_value,
  
  -- Time attributes
  a.created_at AS alert_timestamp,
  a.alert_date,
  HOUR(a.created_at) AS alert_hour,
  
  -- Severity indicators
  CASE WHEN a.severity = 'critical' THEN 1 ELSE 0 END AS is_critical,
  CASE WHEN a.severity = 'high' THEN 1 ELSE 0 END AS is_high,
  CASE WHEN a.severity = 'medium' THEN 1 ELSE 0 END AS is_medium,
  CASE WHEN a.severity = 'low' THEN 1 ELSE 0 END AS is_low,
  
  -- Status indicators
  CASE WHEN a.status = 'open' THEN 1 ELSE 0 END AS is_open,
  CASE WHEN a.status = 'acknowledged' THEN 1 ELSE 0 END AS is_acknowledged,
  CASE WHEN a.status = 'resolved' THEN 1 ELSE 0 END AS is_resolved,
  
  -- Severity weight for aggregations
  CASE 
    WHEN a.severity = 'critical' THEN 4
    WHEN a.severity = 'high' THEN 3
    WHEN a.severity = 'medium' THEN 2
    WHEN a.severity = 'low' THEN 1
    ELSE 0
  END AS severity_weight,
  
  -- Audit
  a._ingested_at,
  current_timestamp() AS _transformed_at

FROM alerts a
LEFT JOIN devices d ON a.device_id = d.device_id;
