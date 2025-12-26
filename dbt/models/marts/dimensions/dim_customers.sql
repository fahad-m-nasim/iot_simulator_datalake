{{/*
  Dimension: Customers
  
  Gold Layer dimension table for customers.
  Enriched with derived attributes.
*/}}

{{
  config(
    materialized='table',
    tags=['dimension', 'gold', 'customers']
  )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
    WHERE NOT _is_deleted
),

-- Calculate customer metrics from orders
customer_orders AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_amount) AS total_revenue,
        AVG(total_amount) AS avg_order_value,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date
    FROM {{ ref('stg_orders') }}
    WHERE NOT _is_deleted
    GROUP BY customer_id
),

-- Calculate device count per customer
customer_devices AS (
    SELECT
        customer_id,
        COUNT(DISTINCT device_id) AS device_count
    FROM {{ ref('stg_devices') }}
    WHERE NOT _is_deleted
    GROUP BY customer_id
),

final AS (
    SELECT
        -- Dimension key
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_key,
        
        -- Business key
        c.customer_id,
        
        -- Attributes
        c.company_name,
        c.contact_name,
        c.email,
        c.phone,
        c.industry,
        c.subscription_tier,
        
        -- Derived attributes
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.total_revenue, 0) AS lifetime_revenue,
        COALESCE(o.avg_order_value, 0) AS avg_order_value,
        o.first_order_date,
        o.last_order_date,
        COALESCE(d.device_count, 0) AS device_count,
        
        -- Customer segment
        CASE
            WHEN c.subscription_tier = 'enterprise' THEN 'Enterprise'
            WHEN COALESCE(o.total_revenue, 0) > 10000 THEN 'High Value'
            WHEN COALESCE(o.total_orders, 0) > 10 THEN 'Frequent'
            ELSE 'Standard'
        END AS customer_segment,
        
        -- Activity status
        CASE
            WHEN o.last_order_date >= DATE_SUB(CURRENT_DATE(), 30) THEN 'Active'
            WHEN o.last_order_date >= DATE_SUB(CURRENT_DATE(), 90) THEN 'At Risk'
            WHEN o.last_order_date IS NOT NULL THEN 'Churned'
            ELSE 'New'
        END AS activity_status,
        
        -- Timestamps
        c.created_at,
        c.updated_at,
        
        -- Audit
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM customers c
    LEFT JOIN customer_orders o ON c.customer_id = o.customer_id
    LEFT JOIN customer_devices d ON c.customer_id = d.customer_id
)

SELECT * FROM final
