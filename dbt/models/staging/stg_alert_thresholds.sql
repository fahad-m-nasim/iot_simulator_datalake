{{/*
  Staging Model: Alert Thresholds (CDC)
*/}}

{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    tags=['staging', 'cdc', 'thresholds', 'silver']
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'cdc_alert_thresholds') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY threshold_id 
            ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
        ) AS _rn
    FROM source
),

latest_records AS (
    SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['threshold_id']) }} AS _surrogate_key,
        
        threshold_id,
        sensor_type,
        CAST(min_value AS DECIMAL(10,2)) AS min_value,
        CAST(max_value AS DECIMAL(10,2)) AS max_value,
        severity,
        alert_message,
        
        __op AS _cdc_operation,
        COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
        _cdc_timestamp,
        
        _ingested_at,
        CURRENT_TIMESTAMP() AS _transformed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM latest_records
    WHERE threshold_id IS NOT NULL
)

SELECT * FROM transformed
