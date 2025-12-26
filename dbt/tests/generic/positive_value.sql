{{/*
  Custom Test: Assert positive values
  
  Tests that a column contains only positive values (> 0)
*/}}

{% test positive_value(model, column_name) %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} <= 0
  AND {{ column_name }} IS NOT NULL

{% endtest %}
