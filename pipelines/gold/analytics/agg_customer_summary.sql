-- =============================================================================
-- Gold Layer: Analytics - Customer Summary
-- =============================================================================
-- Executive summary of customer health, devices, and revenue
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW agg_customer_summary
COMMENT 'Gold layer: Executive customer summary for reporting'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH customers AS (
  SELECT * FROM LIVE.dim_customers
),

-- Order metrics
order_metrics AS (
  SELECT
    customer_key,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    COUNT(DISTINCT order_date) AS order_days,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    COUNT(DISTINCT product_id) AS unique_products_ordered,
    SUM(is_completed) AS completed_orders,
    SUM(is_cancelled) AS cancelled_orders
  FROM LIVE.fct_orders
  GROUP BY customer_key
),

-- Device metrics
device_metrics AS (
  SELECT
    customer_key,
    COUNT(DISTINCT device_key) AS total_devices,
    COUNT(DISTINCT CASE WHEN is_active THEN device_key END) AS active_devices,
    AVG(health_score) AS avg_device_health,
    SUM(total_events) AS total_device_events,
    SUM(total_alerts) AS total_device_alerts,
    SUM(critical_alerts) AS total_critical_alerts
  FROM LIVE.dim_devices
  GROUP BY customer_key
),

-- Recent alert metrics (last 30 days)
recent_alerts AS (
  SELECT
    customer_key,
    COUNT(*) AS recent_alert_count,
    SUM(is_critical) AS recent_critical_alerts,
    SUM(is_open) AS open_alerts
  FROM LIVE.fct_alerts
  WHERE alert_date >= DATE_SUB(current_date(), 30)
  GROUP BY customer_key
)

SELECT
  -- Keys
  c.customer_key,
  c.customer_id,
  
  -- Customer info
  c.company_name,
  c.contact_name,
  c.email,
  c.industry,
  c.subscription_tier,
  c.customer_value_tier,
  c.activity_status,
  
  -- Device summary
  COALESCE(d.total_devices, 0) AS total_devices,
  COALESCE(d.active_devices, 0) AS active_devices,
  ROUND(COALESCE(d.avg_device_health, 0), 2) AS avg_device_health,
  COALESCE(d.total_device_events, 0) AS total_device_events,
  
  -- Order summary
  COALESCE(o.total_orders, 0) AS total_orders,
  COALESCE(o.completed_orders, 0) AS completed_orders,
  COALESCE(o.cancelled_orders, 0) AS cancelled_orders,
  ROUND(COALESCE(o.total_revenue, 0), 2) AS total_revenue,
  ROUND(COALESCE(o.avg_order_value, 0), 2) AS avg_order_value,
  o.first_order_date,
  o.last_order_date,
  COALESCE(o.unique_products_ordered, 0) AS unique_products,
  
  -- Order frequency
  CASE 
    WHEN o.order_days > 0 THEN ROUND(o.total_orders * 1.0 / o.order_days, 2)
    ELSE 0
  END AS orders_per_active_day,
  
  -- Alert summary
  COALESCE(d.total_device_alerts, 0) AS total_alerts,
  COALESCE(d.total_critical_alerts, 0) AS critical_alerts,
  COALESCE(r.recent_alert_count, 0) AS alerts_last_30_days,
  COALESCE(r.recent_critical_alerts, 0) AS critical_alerts_last_30_days,
  COALESCE(r.open_alerts, 0) AS open_alerts,
  
  -- Customer health score (0-100)
  ROUND(
    -- Device health contribution (40%)
    COALESCE(d.avg_device_health, 50) * 0.4 +
    -- Revenue contribution (30%)
    CASE 
      WHEN COALESCE(o.total_revenue, 0) >= 100000 THEN 100
      WHEN COALESCE(o.total_revenue, 0) >= 50000 THEN 80
      WHEN COALESCE(o.total_revenue, 0) >= 10000 THEN 60
      WHEN COALESCE(o.total_revenue, 0) > 0 THEN 40
      ELSE 20
    END * 0.3 +
    -- Activity contribution (30%)
    CASE
      WHEN c.activity_status = 'active' THEN 100
      WHEN c.activity_status = 'at_risk' THEN 50
      WHEN c.activity_status = 'new' THEN 70
      ELSE 20
    END * 0.3
  , 2) AS customer_health_score,
  
  -- Timestamps
  c.created_at AS customer_since,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM customers c
LEFT JOIN order_metrics o ON c.customer_key = o.customer_key
LEFT JOIN device_metrics d ON c.customer_key = d.customer_key
LEFT JOIN recent_alerts r ON c.customer_key = r.customer_key;
