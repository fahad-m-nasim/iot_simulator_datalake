-- =============================================================================
-- Gold Layer: Dimension - Customers
-- =============================================================================
-- Conformed customer dimension for the data warehouse
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_customers
COMMENT 'Gold layer: Customer dimension table'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH customers AS (
  SELECT * FROM LIVE.stg_customers
  WHERE NOT _is_deleted
),

-- Aggregate device and order metrics per customer
device_metrics AS (
  SELECT
    customer_id,
    COUNT(DISTINCT device_id) AS device_count,
    COUNT(DISTINCT CASE WHEN status = 'active' THEN device_id END) AS active_device_count
  FROM LIVE.stg_devices
  WHERE NOT _is_deleted
  GROUP BY customer_id
),

order_metrics AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount) AS total_revenue,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date
  FROM LIVE.stg_orders
  WHERE NOT _is_deleted
  GROUP BY customer_id
)

SELECT
  -- Surrogate key
  md5(c.customer_id) AS customer_key,
  
  -- Business key
  c.customer_id,
  
  -- Customer attributes
  c.company_name,
  c.contact_name,
  c.email,
  c.phone,
  c.industry,
  c.subscription_tier,
  
  -- Timestamps
  c.created_at,
  c.updated_at,
  
  -- Device metrics
  COALESCE(d.device_count, 0) AS total_devices,
  COALESCE(d.active_device_count, 0) AS active_devices,
  
  -- Order metrics
  COALESCE(o.total_orders, 0) AS total_orders,
  COALESCE(o.total_revenue, 0) AS total_revenue,
  o.first_order_date,
  o.last_order_date,
  
  -- Customer value tier
  CASE
    WHEN COALESCE(o.total_revenue, 0) >= 100000 THEN 'platinum'
    WHEN COALESCE(o.total_revenue, 0) >= 50000 THEN 'gold'
    WHEN COALESCE(o.total_revenue, 0) >= 10000 THEN 'silver'
    ELSE 'bronze'
  END AS customer_value_tier,
  
  -- Activity status
  CASE
    WHEN o.last_order_date >= DATE_SUB(current_date(), 90) THEN 'active'
    WHEN o.last_order_date >= DATE_SUB(current_date(), 180) THEN 'at_risk'
    WHEN o.last_order_date IS NOT NULL THEN 'churned'
    ELSE 'new'
  END AS activity_status,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM customers c
LEFT JOIN device_metrics d ON c.customer_id = d.customer_id
LEFT JOIN order_metrics o ON c.customer_id = o.customer_id;
