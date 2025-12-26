-- =============================================================================
-- Gold Layer: Dimension - Locations
-- =============================================================================
-- Conformed location dimension
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_locations
COMMENT 'Gold layer: Location dimension table'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH locations AS (
  SELECT * FROM LIVE.stg_locations
  WHERE NOT _is_deleted
),

-- Count devices per location
device_counts AS (
  SELECT
    location_id,
    COUNT(DISTINCT device_id) AS device_count,
    COUNT(DISTINCT CASE WHEN status = 'active' THEN device_id END) AS active_device_count
  FROM LIVE.stg_devices
  WHERE NOT _is_deleted
  GROUP BY location_id
)

SELECT
  -- Surrogate key
  md5(l.location_id) AS location_key,
  
  -- Business key
  l.location_id,
  
  -- Location attributes
  l.location_name,
  l.building,
  l.floor,
  l.zone,
  l.latitude,
  l.longitude,
  
  -- Device metrics
  COALESCE(d.device_count, 0) AS total_devices,
  COALESCE(d.active_device_count, 0) AS active_devices,
  
  -- Geographic classification
  CASE
    WHEN l.zone IS NOT NULL THEN CONCAT(l.building, '-', l.zone)
    ELSE l.building
  END AS full_location_path,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM locations l
LEFT JOIN device_counts d ON l.location_id = d.location_id;
