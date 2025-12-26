{{/*
  Custom Test: Assert no orphaned records
  
  Tests that all records have a valid relationship to parent table
*/}}

{% test no_orphan_records(model, column_name, to, field) %}

WITH parent AS (
    SELECT DISTINCT {{ field }} AS parent_key
    FROM {{ to }}
),

child AS (
    SELECT DISTINCT {{ column_name }} AS child_key
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL
)

SELECT child_key
FROM child
WHERE child_key NOT IN (SELECT parent_key FROM parent)

{% endtest %}
