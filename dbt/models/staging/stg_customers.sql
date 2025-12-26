{{/*
  Staging Model: Customers (CDC)
  
  Purpose: Apply SCD Type 1 logic to get current state of customers
  Source: bronze.cdc_customers
  Target: silver.stg_customers
  
  CDC Logic:
  - r = snapshot read (initial load)
  - c = create (insert)
  - u = update
  - d = delete
*/}}

{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    merge_update_columns=['company_name', 'contact_name', 'email', 'phone', 
                          'industry', 'subscription_tier', 'updated_at',
                          '_is_deleted', '_cdc_timestamp', '_transformed_at'],
    tags=['staging', 'cdc', 'customers', 'silver']
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cdc_customers') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

-- Get the latest CDC event per customer
ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
        ) AS _rn
    FROM source
),

latest_records AS (
    SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} AS _surrogate_key,
        
        -- Business keys
        customer_id,
        
        -- Attributes
        company_name,
        contact_name,
        email,
        phone,
        industry,
        subscription_tier,
        
        -- Timestamps
        CASE 
            WHEN created_at IS NOT NULL THEN 
                TIMESTAMP_MILLIS(CAST(created_at AS BIGINT))
            ELSE _cdc_timestamp
        END AS created_at,
        CASE 
            WHEN updated_at IS NOT NULL THEN 
                TIMESTAMP_MILLIS(CAST(updated_at AS BIGINT))
            ELSE _cdc_timestamp
        END AS updated_at,
        
        -- CDC metadata
        __op AS _cdc_operation,
        COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
        _cdc_timestamp,
        
        -- Audit columns
        _ingested_at,
        CURRENT_TIMESTAMP() AS _transformed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM latest_records
    WHERE customer_id IS NOT NULL
)

SELECT * FROM transformed
