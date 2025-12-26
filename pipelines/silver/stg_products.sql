-- =============================================================================
-- Silver Layer: Staging Products (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of products
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_products
COMMENT 'Silver layer: Current state of products from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_products
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(product_id) AS _surrogate_key,
    
    product_id,
    product_name,
    category,
    TRY_CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
    description,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE product_id IS NOT NULL
)

SELECT * FROM transformed;
