-- =============================================================================
-- Silver Layer: Staging Alert Thresholds (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of alert thresholds
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_alert_thresholds
COMMENT 'Silver layer: Current state of alert thresholds from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_alert_thresholds
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY threshold_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(threshold_id) AS _surrogate_key,
    
    threshold_id,
    sensor_type,
    TRY_CAST(min_value AS DECIMAL(10,2)) AS min_value,
    TRY_CAST(max_value AS DECIMAL(10,2)) AS max_value,
    severity,
    alert_message,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE threshold_id IS NOT NULL
)

SELECT * FROM transformed;
