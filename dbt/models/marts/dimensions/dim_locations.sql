{{/*
  Dimension: Locations
*/}}

{{
  config(
    materialized='table',
    tags=['dimension', 'gold', 'locations']
  )
}}

WITH locations AS (
    SELECT * FROM {{ ref('stg_locations') }}
    WHERE NOT _is_deleted
),

-- Device counts per location
location_devices AS (
    SELECT
        location_id,
        COUNT(DISTINCT device_id) AS device_count,
        COUNT(DISTINCT CASE WHEN status = 'active' THEN device_id END) AS active_device_count
    FROM {{ ref('stg_devices') }}
    WHERE NOT _is_deleted
    GROUP BY location_id
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['l.location_id']) }} AS location_key,
        
        l.location_id,
        l.location_name,
        l.building,
        l.floor,
        l.zone,
        l.latitude,
        l.longitude,
        
        -- Derived
        COALESCE(d.device_count, 0) AS device_count,
        COALESCE(d.active_device_count, 0) AS active_device_count,
        
        CONCAT(l.building, ' - ', l.zone) AS building_zone,
        
        CURRENT_TIMESTAMP() AS _loaded_at,
        '{{ invocation_id }}' AS _dbt_invocation_id
        
    FROM locations l
    LEFT JOIN location_devices d ON l.location_id = d.location_id
)

SELECT * FROM final
