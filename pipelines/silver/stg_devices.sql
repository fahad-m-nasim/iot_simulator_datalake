-- =============================================================================
-- Silver Layer: Staging Devices (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of devices
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_devices
COMMENT 'Silver layer: Current state of devices from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_devices
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY device_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(device_id) AS _surrogate_key,
    
    device_id,
    device_name,
    device_type,
    manufacturer,
    model,
    location_id,
    customer_id,
    status,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE device_id IS NOT NULL
)

SELECT * FROM transformed;
