{{/*
  Staging Model: Orders (CDC)
*/}}

{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "month"
    },
    tags=['staging', 'cdc', 'orders', 'silver']
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cdc_orders') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
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
        {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS _surrogate_key,
        
        order_id,
        customer_id,
        product_id,
        device_id,
        
        CAST(quantity AS INT) AS quantity,
        CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
        order_status,
        
        CASE 
            WHEN order_date IS NOT NULL THEN 
                DATE(TIMESTAMP_MILLIS(CAST(order_date AS BIGINT)))
            ELSE DATE(_cdc_timestamp)
        END AS order_date,
        
        __op AS _cdc_operation,
        COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
        _cdc_timestamp,
        
        _ingested_at,
        CURRENT_TIMESTAMP() AS _transformed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM latest_records
    WHERE order_id IS NOT NULL
)

SELECT * FROM transformed
