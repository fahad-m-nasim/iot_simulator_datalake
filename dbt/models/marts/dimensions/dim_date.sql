{{/*
  Dimension: Date
  
  Standard date dimension for analytics.
*/}}

{{
  config(
    materialized='table',
    tags=['dimension', 'gold', 'date']
  )
}}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

final AS (
    SELECT
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} AS date_key,
        date_day AS date_actual,
        
        -- Date parts
        YEAR(date_day) AS year,
        MONTH(date_day) AS month,
        DAY(date_day) AS day,
        QUARTER(date_day) AS quarter,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEKOFYEAR(date_day) AS week_of_year,
        
        -- Date names
        DATE_FORMAT(date_day, 'EEEE') AS day_name,
        DATE_FORMAT(date_day, 'MMMM') AS month_name,
        DATE_FORMAT(date_day, 'MMM') AS month_short,
        
        -- Formatted dates
        DATE_FORMAT(date_day, 'yyyy-MM') AS year_month,
        DATE_FORMAT(date_day, 'yyyy-Qq') AS year_quarter,
        
        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- Period comparisons
        DATE_SUB(date_day, 7) AS same_day_last_week,
        ADD_MONTHS(date_day, -1) AS same_day_last_month,
        ADD_MONTHS(date_day, -12) AS same_day_last_year,
        
        -- First/Last of period
        DATE_TRUNC('MONTH', date_day) AS first_of_month,
        LAST_DAY(date_day) AS last_of_month,
        DATE_TRUNC('QUARTER', date_day) AS first_of_quarter,
        DATE_TRUNC('YEAR', date_day) AS first_of_year
        
    FROM date_spine
)

SELECT * FROM final
