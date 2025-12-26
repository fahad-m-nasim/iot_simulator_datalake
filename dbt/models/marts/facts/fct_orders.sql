{{/*
  Fact: Orders
  
  Gold Layer fact table for customer orders.
*/}}

{{
  config(
    materialized='incremental',
    unique_key='order_key',
    incremental_strategy='merge',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "month"
    },
    tags=['fact', 'gold', 'orders']
  )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE NOT _is_deleted
    {% if is_incremental() %}
    AND _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

customers AS (
    SELECT customer_key, customer_id
    FROM {{ ref('dim_customers') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
    WHERE NOT _is_deleted
),

devices AS (
    SELECT device_key, device_id
    FROM {{ ref('dim_devices') }}
),

final AS (
    SELECT
        -- Fact key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_key,
        o.order_id,
        
        -- Dimension keys
        c.customer_key,
        {{ dbt_utils.generate_surrogate_key(['o.product_id']) }} AS product_key,
        d.device_key,
        {{ dbt_utils.generate_surrogate_key(['o.order_date']) }} AS date_key,
        
        -- Degenerate dimensions
        o.customer_id,
        o.product_id,
        o.device_id,
        o.order_status,
        
        -- Product attributes (for analysis)
        p.product_name,
        p.category AS product_category,
        p.unit_price,
        
        -- Measures
        o.quantity,
        o.total_amount,
        COALESCE(o.total_amount / NULLIF(o.quantity, 0), 0) AS unit_amount,
        
        -- Timestamps
        o.order_date,
        
        -- Audit
        o._ingested_at,
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
    LEFT JOIN products p ON o.product_id = p.product_id
    LEFT JOIN devices d ON o.device_id = d.device_id
)

SELECT * FROM final
