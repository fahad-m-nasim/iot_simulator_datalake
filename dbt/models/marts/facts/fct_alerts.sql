{{/*
  Fact: Alerts
  
  Gold Layer fact table for device alerts.
*/}}

{{
  config(
    materialized='incremental',
    unique_key='alert_key',
    incremental_strategy='merge',
    partition_by={
      "field": "alert_date",
      "data_type": "date",
      "granularity": "day"
    },
    tags=['fact', 'gold', 'alerts']
  )
}}

WITH alerts AS (
    SELECT * FROM {{ ref('stg_alerts') }}
    WHERE NOT _is_deleted
    {% if is_incremental() %}
    AND _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

devices AS (
    SELECT 
        device_key, 
        device_id,
        customer_key,
        location_key
    FROM {{ ref('dim_devices') }}
),

thresholds AS (
    SELECT * FROM {{ ref('stg_alert_thresholds') }}
    WHERE NOT _is_deleted
),

final AS (
    SELECT
        -- Fact key
        {{ dbt_utils.generate_surrogate_key(['a.alert_id']) }} AS alert_key,
        a.alert_id,
        
        -- Dimension keys
        d.device_key,
        d.customer_key,
        d.location_key,
        {{ dbt_utils.generate_surrogate_key(['a.alert_date']) }} AS date_key,
        
        -- Degenerate dimensions
        a.device_id,
        a.threshold_id,
        a.alert_type,
        a.severity,
        a.status AS alert_status,
        a.message,
        
        -- Threshold context
        t.sensor_type AS threshold_sensor_type,
        t.min_value AS threshold_min,
        t.max_value AS threshold_max,
        
        -- Measures
        CAST(a.triggered_value AS DOUBLE) AS triggered_value,
        
        -- Severity weight for aggregations
        CASE a.severity
            WHEN 'critical' THEN 4
            WHEN 'high' THEN 3
            WHEN 'medium' THEN 2
            WHEN 'low' THEN 1
            ELSE 0
        END AS severity_weight,
        
        -- Status flags
        CASE WHEN a.status = 'open' THEN 1 ELSE 0 END AS is_open,
        CASE WHEN a.status = 'resolved' THEN 1 ELSE 0 END AS is_resolved,
        CASE WHEN a.severity = 'critical' THEN 1 ELSE 0 END AS is_critical,
        
        -- Timestamps
        a.created_at AS alert_timestamp,
        a.alert_date,
        HOUR(a.created_at) AS alert_hour,
        
        -- Audit
        a._ingested_at,
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM alerts a
    LEFT JOIN devices d ON a.device_id = d.device_id
    LEFT JOIN thresholds t ON a.threshold_id = t.threshold_id
)

SELECT * FROM final
