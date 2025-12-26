{{/*
  Fact: IoT Events
  
  Gold Layer fact table for sensor readings.
  Incremental with daily partitioning.
*/}}

{{
  config(
    materialized='incremental',
    unique_key='event_key',
    incremental_strategy='merge',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['device_key', 'sensor_type'],
    tags=['fact', 'gold', 'iot']
  )
}}

WITH events AS (
    SELECT * FROM {{ ref('stg_iot_events') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

devices AS (
    SELECT device_key, device_id
    FROM {{ ref('dim_devices') }}
),

locations AS (
    SELECT location_key, location_id
    FROM {{ ref('dim_locations') }}
),

thresholds AS (
    SELECT * FROM {{ ref('stg_alert_thresholds') }}
    WHERE NOT _is_deleted
),

final AS (
    SELECT
        -- Fact key
        {{ dbt_utils.generate_surrogate_key(['e.event_id']) }} AS event_key,
        e.event_id,
        
        -- Dimension keys
        COALESCE(d.device_key, {{ dbt_utils.generate_surrogate_key(["'UNKNOWN'"]) }}) AS device_key,
        COALESCE(l.location_key, {{ dbt_utils.generate_surrogate_key(["'UNKNOWN'"]) }}) AS location_key,
        {{ dbt_utils.generate_surrogate_key(['e.event_date']) }} AS date_key,
        
        -- Degenerate dimensions
        e.device_id,
        e.location_id,
        e.sensor_type,
        e.sensor_unit,
        e.firmware_version,
        e.quality_flag,
        
        -- Measures
        e.sensor_value,
        e.battery_level,
        
        -- Flags
        e.is_valid_reading,
        
        -- Threshold comparison
        CASE
            WHEN t.threshold_id IS NOT NULL 
                AND (e.sensor_value < t.min_value OR e.sensor_value > t.max_value)
            THEN TRUE
            ELSE FALSE
        END AS is_threshold_breach,
        
        t.threshold_id,
        t.min_value AS threshold_min,
        t.max_value AS threshold_max,
        
        -- Timestamps
        e.event_timestamp,
        e.event_date,
        HOUR(e.event_timestamp) AS event_hour,
        
        -- Audit
        e._ingested_at,
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM events e
    LEFT JOIN devices d ON e.device_id = d.device_id
    LEFT JOIN locations l ON e.location_id = l.location_id
    LEFT JOIN thresholds t ON e.sensor_type = t.sensor_type
)

SELECT * FROM final
