{{/*
  Analytics: Customer Summary
  
  Executive summary of customer health and activity.
*/}}

{{
  config(
    materialized='table',
    tags=['analytics', 'gold', 'executive']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
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
        COUNT(DISTINCT product_id) AS unique_products_ordered
    FROM {{ ref('fct_orders') }}
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
        SUM(total_alerts) AS total_device_alerts
    FROM {{ ref('dim_devices') }}
    GROUP BY customer_key
),

-- Recent alert metrics (last 30 days)
recent_alerts AS (
    SELECT
        customer_key,
        COUNT(*) AS recent_alert_count,
        SUM(is_critical) AS recent_critical_alerts,
        SUM(is_open) AS open_alerts
    FROM {{ ref('fct_alerts') }}
    WHERE alert_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY customer_key
),

final AS (
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
        c.customer_segment,
        c.activity_status,
        
        -- Order metrics
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.total_revenue, 0) AS total_revenue,
        COALESCE(o.avg_order_value, 0) AS avg_order_value,
        o.first_order_date,
        o.last_order_date,
        COALESCE(o.unique_products_ordered, 0) AS unique_products,
        
        -- Device metrics
        COALESCE(d.total_devices, 0) AS total_devices,
        COALESCE(d.active_devices, 0) AS active_devices,
        ROUND(COALESCE(d.avg_device_health, 0), 1) AS avg_device_health,
        COALESCE(d.total_device_events, 0) AS total_events,
        COALESCE(d.total_device_alerts, 0) AS total_alerts,
        
        -- Recent activity
        COALESCE(ra.recent_alert_count, 0) AS alerts_last_30_days,
        COALESCE(ra.recent_critical_alerts, 0) AS critical_alerts_last_30_days,
        COALESCE(ra.open_alerts, 0) AS open_alerts,
        
        -- Customer health score (0-100)
        ROUND(
            CASE
                WHEN c.activity_status = 'Churned' THEN 20
                WHEN c.activity_status = 'At Risk' THEN 50
                ELSE 100
            END
            - LEAST(COALESCE(ra.open_alerts, 0) * 2, 30)
            - CASE WHEN COALESCE(d.avg_device_health, 100) < 50 THEN 20 ELSE 0 END
            + CASE WHEN c.subscription_tier = 'enterprise' THEN 10 ELSE 0 END,
        0) AS customer_health_score,
        
        -- Tier value
        CASE c.subscription_tier
            WHEN 'enterprise' THEN 3
            WHEN 'premium' THEN 2
            ELSE 1
        END AS tier_value,
        
        -- Audit
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM customers c
    LEFT JOIN order_metrics o ON c.customer_key = o.customer_key
    LEFT JOIN device_metrics d ON c.customer_key = d.customer_key
    LEFT JOIN recent_alerts ra ON c.customer_key = ra.customer_key
)

SELECT * FROM final
