-- Data Quality Test: Check for duplicate events
-- Ensures no duplicate event_ids in fact table

WITH duplicates AS (
    SELECT
        event_id,
        COUNT(*) AS occurrence_count
    FROM {{ ref('fct_iot_events') }}
    GROUP BY event_id
    HAVING COUNT(*) > 1
)

SELECT * FROM duplicates
