-- =============================================================================
-- Silver Layer: Staging Alerts (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of alerts
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_alerts
COMMENT 'Silver layer: Current state of alerts from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_alerts
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY alert_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(alert_id) AS _surrogate_key,
    
    alert_id,
    device_id,
    threshold_id,
    alert_type,
    severity,
    message,
    status,
    TRY_CAST(triggered_value AS DOUBLE) AS triggered_value,
    
    CASE 
      WHEN created_at IS NOT NULL THEN 
        from_unixtime(created_at / 1000)
      ELSE _cdc_timestamp
    END AS created_at,
    DATE(CASE 
      WHEN created_at IS NOT NULL THEN 
        from_unixtime(created_at / 1000)
      ELSE _cdc_timestamp
    END) AS alert_date,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE alert_id IS NOT NULL
)

SELECT * FROM transformed;
