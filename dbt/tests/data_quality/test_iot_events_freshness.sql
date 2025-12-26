-- Data Quality Test: Check for data freshness
-- Ensures IoT events are being processed within expected timeframe

{% set freshness_threshold_hours = 6 %}

WITH latest_data AS (
    SELECT
        MAX(event_timestamp) AS latest_event,
        MAX(_ingested_at) AS latest_ingestion
    FROM {{ ref('fct_iot_events') }}
)

SELECT *
FROM latest_data
WHERE 
    -- Alert if no events in last N hours
    TIMESTAMPDIFF(HOUR, latest_event, CURRENT_TIMESTAMP()) > {{ freshness_threshold_hours }}
    OR latest_event IS NULL
