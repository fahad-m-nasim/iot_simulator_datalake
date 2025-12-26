-- Integration Test: Customer revenue consistency
-- Verifies that customer lifetime_revenue matches sum of orders

WITH customer_order_totals AS (
    SELECT
        customer_key,
        SUM(total_amount) AS calculated_revenue
    FROM {{ ref('fct_orders') }}
    GROUP BY customer_key
),

comparison AS (
    SELECT
        c.customer_key,
        c.customer_id,
        c.lifetime_revenue AS dim_revenue,
        COALESCE(o.calculated_revenue, 0) AS fact_revenue,
        ABS(c.lifetime_revenue - COALESCE(o.calculated_revenue, 0)) AS revenue_diff
    FROM {{ ref('dim_customers') }} c
    LEFT JOIN customer_order_totals o ON c.customer_key = o.customer_key
)

SELECT *
FROM comparison
WHERE revenue_diff > 0.01  -- Allow for small rounding differences
