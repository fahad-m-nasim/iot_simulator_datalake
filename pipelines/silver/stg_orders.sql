-- =============================================================================
-- Silver Layer: Staging Orders (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of orders
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_orders
COMMENT 'Silver layer: Current state of orders from CDC events'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_orders
),

ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    md5(order_id) AS _surrogate_key,
    
    order_id,
    customer_id,
    product_id,
    device_id,
    
    TRY_CAST(quantity AS INT) AS quantity,
    TRY_CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
    order_status,
    
    CASE 
      WHEN order_date IS NOT NULL THEN 
        DATE(from_unixtime(order_date / 1000))
      ELSE DATE(_cdc_timestamp)
    END AS order_date,
    
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE order_id IS NOT NULL
)

SELECT * FROM transformed;
