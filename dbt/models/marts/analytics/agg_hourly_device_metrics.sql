{{/*
  Analytics: Hourly Device Metrics
  
  Near real-time hourly aggregations for operational dashboards.
  Supports drill-down from daily metrics and trend analysis.
*/}}

{{
  config(
    materialized='incremental',
    unique_key='hourly_metric_key',
    incremental_strategy='merge',
    partition_by={
      "field": "metric_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['device_id', 'sensor_type'],
    tags=['analytics', 'gold', 'hourly', 'operational']
  )
}}

WITH hourly_events AS (
    SELECT
        device_key,
        device_id,
        location_id,
        sensor_type,
        event_date AS metric_date,
        DATE_TRUNC('HOUR', event_timestamp) AS metric_hour,
        
        -- Event counts
        COUNT(*) AS event_count,
        COUNT(CASE WHEN is_valid_reading THEN 1 END) AS valid_readings,
        COUNT(CASE WHEN NOT is_valid_reading THEN 1 END) AS invalid_readings,
        COUNT(CASE WHEN is_threshold_breach THEN 1 END) AS breach_count,
        
        -- Sensor statistics
        AVG(sensor_value) AS avg_value,
        MIN(sensor_value) AS min_value,
        MAX(sensor_value) AS max_value,
        STDDEV(sensor_value) AS stddev_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sensor_value) AS median_value,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sensor_value) AS p95_value,
        
        -- Device health
        AVG(battery_level) AS avg_battery_level,
        MIN(battery_level) AS min_battery_level,
        
        -- Timing
        MIN(event_timestamp) AS first_event_at,
        MAX(event_timestamp) AS last_event_at,
        TIMESTAMPDIFF(SECOND, MIN(event_timestamp), MAX(event_timestamp)) AS active_seconds,
        
        MAX(_ingested_at) AS _last_ingested_at
        
    FROM {{ ref('fct_iot_events') }}
    {% if is_incremental() %}
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), 2)
    {% endif %}
    GROUP BY 
        device_key, device_id, location_id, sensor_type, 
        event_date, DATE_TRUNC('HOUR', event_timestamp)
),

devices AS (
    SELECT 
        device_key,
        device_name,
        device_type,
        customer_id
    FROM {{ ref('dim_devices') }}
),

locations AS (
    SELECT
        location_id,
        location_name,
        building,
        zone
    FROM {{ ref('dim_locations') }}
),

final AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['h.device_id', 'h.sensor_type', 'h.metric_hour']) }} AS hourly_metric_key,
        
        -- Dimension keys
        h.device_key,
        {{ dbt_utils.generate_surrogate_key(['h.metric_date']) }} AS date_key,
        
        -- Time attributes
        h.metric_date,
        h.metric_hour,
        HOUR(h.metric_hour) AS hour_of_day,
        DAYOFWEEK(h.metric_date) AS day_of_week,
        CASE 
            WHEN DAYOFWEEK(h.metric_date) IN (1, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,
        
        -- Device attributes
        h.device_id,
        d.device_name,
        d.device_type,
        d.customer_id,
        
        -- Location attributes
        h.location_id,
        l.location_name,
        l.building,
        l.zone,
        
        -- Sensor type
        h.sensor_type,
        
        -- Metrics
        h.event_count,
        h.valid_readings,
        h.invalid_readings,
        h.breach_count,
        
        -- Data quality score
        ROUND(h.valid_readings * 100.0 / NULLIF(h.event_count, 0), 2) AS data_quality_pct,
        
        -- Sensor statistics
        ROUND(h.avg_value, 4) AS avg_value,
        ROUND(h.min_value, 4) AS min_value,
        ROUND(h.max_value, 4) AS max_value,
        ROUND(h.stddev_value, 4) AS stddev_value,
        ROUND(h.median_value, 4) AS median_value,
        ROUND(h.p95_value, 4) AS p95_value,
        ROUND(h.max_value - h.min_value, 4) AS value_range,
        
        -- Device health
        ROUND(h.avg_battery_level, 2) AS avg_battery_level,
        h.min_battery_level,
        
        -- Activity metrics
        h.first_event_at,
        h.last_event_at,
        h.active_seconds,
        ROUND(h.event_count * 3600.0 / NULLIF(h.active_seconds, 0), 2) AS events_per_hour_normalized,
        
        -- Audit
        h._last_ingested_at,
        CURRENT_TIMESTAMP() AS _transformed_at
        
    FROM hourly_events h
    LEFT JOIN devices d ON h.device_key = d.device_key
    LEFT JOIN locations l ON h.location_id = l.location_id
)

SELECT * FROM final
