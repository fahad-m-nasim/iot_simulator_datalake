-- =============================================================================
-- Silver Layer: Staging Locations (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of locations
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_locations
COMMENT 'Silver layer: Current state of locations from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_locations
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY location_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(location_id) AS _surrogate_key,
    
    location_id,
    location_name,
    building,
    TRY_CAST(floor AS INT) AS floor,
    zone,
    TRY_CAST(latitude AS DECIMAL(10,6)) AS latitude,
    TRY_CAST(longitude AS DECIMAL(10,6)) AS longitude,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE location_id IS NOT NULL
)

SELECT * FROM transformed;
