{{/*
  Dimension: Devices
  
  Gold Layer dimension table for IoT devices.
  Enriched with operational metrics.
*/}}

{{
  config(
    materialized='table',
    tags=['dimension', 'gold', 'devices']
  )
}}

WITH devices AS (
    SELECT * FROM {{ ref('stg_devices') }}
    WHERE NOT _is_deleted
),

locations AS (
    SELECT * FROM {{ ref('stg_locations') }}
    WHERE NOT _is_deleted
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
    WHERE NOT _is_deleted
),

-- Device event statistics
device_stats AS (
    SELECT
        device_id,
        COUNT(*) AS total_events,
        MIN(event_timestamp) AS first_event_at,
        MAX(event_timestamp) AS last_event_at,
        AVG(battery_level) AS avg_battery_level,
        SUM(CASE WHEN NOT is_valid_reading THEN 1 ELSE 0 END) AS invalid_reading_count,
        COUNT(DISTINCT DATE(event_timestamp)) AS active_days
    FROM {{ ref('stg_iot_events') }}
    GROUP BY device_id
),

-- Alert counts per device
device_alerts AS (
    SELECT
        device_id,
        COUNT(*) AS total_alerts,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_alerts,
        SUM(CASE WHEN status = 'open' THEN 1 ELSE 0 END) AS open_alerts
    FROM {{ ref('stg_alerts') }}
    WHERE NOT _is_deleted
    GROUP BY device_id
),

final AS (
    SELECT
        -- Dimension key
        {{ dbt_utils.generate_surrogate_key(['d.device_id']) }} AS device_key,
        
        -- Business key
        d.device_id,
        
        -- Device attributes
        d.device_name,
        d.device_type,
        d.manufacturer,
        d.model,
        d.status AS device_status,
        
        -- Location denormalization
        d.location_id,
        l.location_name,
        l.building,
        l.floor,
        l.zone,
        l.latitude,
        l.longitude,
        
        -- Customer denormalization
        d.customer_id,
        c.company_name AS customer_name,
        c.subscription_tier AS customer_tier,
        
        -- Operational metrics
        COALESCE(s.total_events, 0) AS total_events,
        s.first_event_at,
        s.last_event_at,
        COALESCE(s.avg_battery_level, 0) AS avg_battery_level,
        COALESCE(s.invalid_reading_count, 0) AS invalid_reading_count,
        COALESCE(s.active_days, 0) AS active_days,
        
        -- Alert metrics
        COALESCE(a.total_alerts, 0) AS total_alerts,
        COALESCE(a.critical_alerts, 0) AS critical_alerts,
        COALESCE(a.open_alerts, 0) AS open_alerts,
        
        -- Health score (0-100)
        CASE
            WHEN d.status = 'decommissioned' THEN 0
            WHEN d.status = 'inactive' THEN 25
            WHEN COALESCE(a.open_alerts, 0) > 5 THEN 40
            WHEN COALESCE(s.avg_battery_level, 100) < 20 THEN 50
            WHEN COALESCE(a.critical_alerts, 0) > 0 THEN 60
            WHEN COALESCE(s.invalid_reading_count, 0) > s.total_events * 0.1 THEN 70
            WHEN d.status = 'maintenance' THEN 80
            ELSE 100
        END AS health_score,
        
        -- Is active flag
        CASE
            WHEN d.status IN ('active', 'maintenance') 
                 AND s.last_event_at >= DATE_SUB(CURRENT_TIMESTAMP(), 24)
            THEN TRUE
            ELSE FALSE
        END AS is_active,
        
        -- Audit
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM devices d
    LEFT JOIN locations l ON d.location_id = l.location_id
    LEFT JOIN customers c ON d.customer_id = c.customer_id
    LEFT JOIN device_stats s ON d.device_id = s.device_id
    LEFT JOIN device_alerts a ON d.device_id = a.device_id
)

SELECT * FROM final
