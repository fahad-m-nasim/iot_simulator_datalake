-- Integration Test: Device event counts
-- Verifies device total_events matches fact table counts

WITH fact_counts AS (
    SELECT
        device_key,
        COUNT(*) AS fact_event_count
    FROM {{ ref('fct_iot_events') }}
    GROUP BY device_key
),

comparison AS (
    SELECT
        d.device_key,
        d.device_id,
        d.total_events AS dim_count,
        COALESCE(f.fact_event_count, 0) AS fact_count,
        ABS(d.total_events - COALESCE(f.fact_event_count, 0)) AS count_diff
    FROM {{ ref('dim_devices') }} d
    LEFT JOIN fact_counts f ON d.device_key = f.device_key
    WHERE d.total_events > 0
)

SELECT *
FROM comparison
WHERE count_diff > 0
