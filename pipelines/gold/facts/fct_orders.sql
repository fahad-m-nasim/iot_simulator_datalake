-- =============================================================================
-- Gold Layer: Fact - Orders
-- =============================================================================
-- Fact table for customer orders with dimension keys
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW fct_orders
COMMENT 'Gold layer: Orders fact table'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH orders AS (
  SELECT * FROM LIVE.stg_orders
  WHERE NOT _is_deleted
),

products AS (
  SELECT product_id, product_name, category, unit_price
  FROM LIVE.stg_products
  WHERE NOT _is_deleted
)

SELECT
  -- Fact key
  md5(o.order_id) AS order_key,
  o.order_id,
  
  -- Dimension keys
  COALESCE(md5(o.customer_id), md5('UNKNOWN')) AS customer_key,
  COALESCE(md5(o.product_id), md5('UNKNOWN')) AS product_key,
  COALESCE(md5(o.device_id), md5('UNKNOWN')) AS device_key,
  md5(CAST(o.order_date AS STRING)) AS date_key,
  
  -- Degenerate dimensions
  o.customer_id,
  o.product_id,
  o.device_id,
  
  -- Product denormalization
  p.product_name,
  p.category AS product_category,
  p.unit_price AS product_unit_price,
  
  -- Order attributes
  o.order_status,
  o.order_date,
  
  -- Measures
  o.quantity,
  o.total_amount,
  
  -- Calculated measures
  CASE 
    WHEN o.quantity > 0 THEN o.total_amount / o.quantity
    ELSE 0
  END AS unit_price,
  
  -- Status indicators
  CASE WHEN o.order_status = 'completed' THEN 1 ELSE 0 END AS is_completed,
  CASE WHEN o.order_status = 'pending' THEN 1 ELSE 0 END AS is_pending,
  CASE WHEN o.order_status = 'cancelled' THEN 1 ELSE 0 END AS is_cancelled,
  
  -- Audit
  o._ingested_at,
  current_timestamp() AS _transformed_at

FROM orders o
LEFT JOIN products p ON o.product_id = p.product_id;
