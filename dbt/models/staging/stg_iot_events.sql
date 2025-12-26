{{/*
  Staging Model: IoT Events
  
  Purpose: Clean and standardize IoT events from Bronze layer
  Source: bronze.iot_events_validated (DLT with quality checks)
  Target: silver.stg_iot_events
  
  Transformations:
  - Parse timestamps
  - Standardize column names
  - Add surrogate key
  - Filter out deleted/invalid records
*/}}

{{
  config(
    materialized='incremental',
    unique_key='_surrogate_key',
    incremental_strategy='merge',
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['device_id', 'sensor_type'],
    tags=['staging', 'iot', 'silver']
  )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'iot_events') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT COALESCE(MAX(_ingested_at), '1900-01-01') FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Surrogate key for merge operations
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} AS _surrogate_key,
        
        -- Primary identifiers
        event_id,
        device_id,
        location_id,
        
        -- Event timestamp handling
        COALESCE(
            event_timestamp,
            TRY_CAST(timestamp AS TIMESTAMP),
            _ingested_at
        ) AS event_timestamp,
        DATE(COALESCE(
            event_timestamp,
            TRY_CAST(timestamp AS TIMESTAMP),
            _ingested_at
        )) AS event_date,
        
        -- Sensor data
        sensor_type,
        value AS sensor_value,
        unit AS sensor_unit,
        
        -- Quality indicators
        quality_flag,
        CASE 
            WHEN quality_flag = 'good' THEN TRUE
            ELSE FALSE
        END AS is_valid_reading,
        
        -- Device metadata
        firmware_version,
        battery_level,
        CASE
            WHEN battery_level < 20 THEN 'critical'
            WHEN battery_level < 40 THEN 'low'
            WHEN battery_level < 60 THEN 'medium'
            ELSE 'good'
        END AS battery_status,
        
        -- Audit columns
        _ingested_at,
        _source_file,
        CURRENT_TIMESTAMP() AS _transformed_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM source
    WHERE event_id IS NOT NULL
)

SELECT * FROM cleaned
