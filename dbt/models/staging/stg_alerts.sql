{{/*
  Staging Model: Alerts (CDC)
*/}}

{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    partition_by={
      "field": "alert_date",
      "data_type": "date",
      "granularity": "day"
    },
    tags=['staging', 'cdc', 'alerts', 'silver']
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cdc_alerts') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY alert_id 
            ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
        ) AS _rn
    FROM source
),

latest_records AS (
    SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['alert_id']) }} AS _surrogate_key,
        
        alert_id,
        device_id,
        threshold_id,
        alert_type,
        severity,
        message,
        status,
        triggered_value,
        
        CASE 
            WHEN created_at IS NOT NULL THEN 
                TIMESTAMP_MILLIS(CAST(created_at AS BIGINT))
            ELSE _cdc_timestamp
        END AS created_at,
        DATE(CASE 
            WHEN created_at IS NOT NULL THEN 
                TIMESTAMP_MILLIS(CAST(created_at AS BIGINT))
            ELSE _cdc_timestamp
        END) AS alert_date,
        
        __op AS _cdc_operation,
        COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
        _cdc_timestamp,
        
        _ingested_at,
        CURRENT_TIMESTAMP() AS _transformed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM latest_records
    WHERE alert_id IS NOT NULL
)

SELECT * FROM transformed
