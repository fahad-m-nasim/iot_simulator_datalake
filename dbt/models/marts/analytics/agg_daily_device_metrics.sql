{{/*
  Analytics: Daily Device Metrics
  
  Aggregated daily metrics per device for dashboards and reporting.
*/}}

{{
  config(
    materialized='table',
    tags=['analytics', 'gold', 'daily']
  )
}}

WITH daily_events AS (
    SELECT
        device_key,
        device_id,
        event_date,
        sensor_type,
        
        COUNT(*) AS event_count,
        COUNT(CASE WHEN is_valid_reading THEN 1 END) AS valid_event_count,
        COUNT(CASE WHEN NOT is_valid_reading THEN 1 END) AS invalid_event_count,
        COUNT(CASE WHEN is_threshold_breach THEN 1 END) AS threshold_breach_count,
        
        AVG(sensor_value) AS avg_sensor_value,
        MIN(sensor_value) AS min_sensor_value,
        MAX(sensor_value) AS max_sensor_value,
        STDDEV(sensor_value) AS stddev_sensor_value,
        
        AVG(battery_level) AS avg_battery_level,
        MIN(battery_level) AS min_battery_level,
        
        MIN(event_timestamp) AS first_event_at,
        MAX(event_timestamp) AS last_event_at
        
    FROM {{ ref('fct_iot_events') }}
    GROUP BY device_key, device_id, event_date, sensor_type
),

daily_alerts AS (
    SELECT
        device_key,
        alert_date AS event_date,
        
        COUNT(*) AS alert_count,
        SUM(is_critical) AS critical_alert_count,
        SUM(is_open) AS open_alert_count,
        SUM(severity_weight) AS total_severity_score
        
    FROM {{ ref('fct_alerts') }}
    GROUP BY device_key, alert_date
),

devices AS (
    SELECT 
        device_key,
        device_id,
        device_name,
        device_type,
        location_key,
        location_name,
        customer_key,
        customer_name
    FROM {{ ref('dim_devices') }}
),

final AS (
    SELECT
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['e.device_key', 'e.event_date', 'e.sensor_type']) }} AS metric_key,
        e.device_key,
        e.event_date,
        e.sensor_type,
        
        -- Device context
        d.device_id,
        d.device_name,
        d.device_type,
        d.location_key,
        d.location_name,
        d.customer_key,
        d.customer_name,
        
        -- Event metrics
        e.event_count,
        e.valid_event_count,
        e.invalid_event_count,
        e.threshold_breach_count,
        
        -- Sensor statistics
        e.avg_sensor_value,
        e.min_sensor_value,
        e.max_sensor_value,
        e.stddev_sensor_value,
        
        -- Battery
        e.avg_battery_level,
        e.min_battery_level,
        
        -- Activity
        e.first_event_at,
        e.last_event_at,
        TIMESTAMPDIFF(MINUTE, e.first_event_at, e.last_event_at) AS active_minutes,
        
        -- Alert metrics
        COALESCE(a.alert_count, 0) AS alert_count,
        COALESCE(a.critical_alert_count, 0) AS critical_alert_count,
        COALESCE(a.open_alert_count, 0) AS open_alert_count,
        COALESCE(a.total_severity_score, 0) AS alert_severity_score,
        
        -- Quality metrics
        ROUND(e.valid_event_count * 100.0 / NULLIF(e.event_count, 0), 2) AS data_quality_pct,
        ROUND(e.threshold_breach_count * 100.0 / NULLIF(e.event_count, 0), 2) AS breach_rate_pct,
        
        -- Health indicators
        CASE
            WHEN COALESCE(a.critical_alert_count, 0) > 0 THEN 'Critical'
            WHEN COALESCE(a.alert_count, 0) > 5 THEN 'Warning'
            WHEN e.invalid_event_count > e.valid_event_count * 0.1 THEN 'Data Quality Issue'
            WHEN e.min_battery_level < 20 THEN 'Low Battery'
            ELSE 'Healthy'
        END AS health_status,
        
        -- Audit
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM daily_events e
    LEFT JOIN devices d ON e.device_key = d.device_key
    LEFT JOIN daily_alerts a ON e.device_key = a.device_key AND e.event_date = a.event_date
)

SELECT * FROM final
