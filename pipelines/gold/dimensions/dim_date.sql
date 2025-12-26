-- =============================================================================
-- Gold Layer: Dimension - Date
-- =============================================================================
-- Standard date dimension for time-based analysis
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW dim_date
COMMENT 'Gold layer: Date dimension table for time intelligence'
TBLPROPERTIES (
  'quality' = 'gold',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH date_spine AS (
  -- Generate dates for 5 years back and 2 years forward
  SELECT explode(sequence(
    DATE('2020-01-01'),
    DATE('2027-12-31'),
    INTERVAL 1 DAY
  )) AS date_day
)

SELECT
  -- Surrogate key
  md5(CAST(date_day AS STRING)) AS date_key,
  
  -- Date value
  date_day AS date_day,
  
  -- Date components
  YEAR(date_day) AS year,
  QUARTER(date_day) AS quarter,
  MONTH(date_day) AS month,
  WEEKOFYEAR(date_day) AS week_of_year,
  DAYOFYEAR(date_day) AS day_of_year,
  DAYOFMONTH(date_day) AS day_of_month,
  DAYOFWEEK(date_day) AS day_of_week,
  
  -- Date names
  DATE_FORMAT(date_day, 'MMMM') AS month_name,
  DATE_FORMAT(date_day, 'MMM') AS month_name_short,
  DATE_FORMAT(date_day, 'EEEE') AS day_name,
  DATE_FORMAT(date_day, 'EEE') AS day_name_short,
  
  -- Fiscal calendar (assuming fiscal year starts in April)
  CASE 
    WHEN MONTH(date_day) >= 4 THEN YEAR(date_day)
    ELSE YEAR(date_day) - 1
  END AS fiscal_year,
  CASE 
    WHEN MONTH(date_day) >= 4 THEN QUARTER(date_day) - 1
    WHEN QUARTER(date_day) = 1 THEN 4
    ELSE QUARTER(date_day) + 3
  END AS fiscal_quarter,
  
  -- Period keys for easy joining
  CONCAT(YEAR(date_day), '-Q', QUARTER(date_day)) AS year_quarter,
  DATE_FORMAT(date_day, 'yyyy-MM') AS year_month,
  CONCAT(YEAR(date_day), '-W', LPAD(WEEKOFYEAR(date_day), 2, '0')) AS year_week,
  
  -- Boolean flags
  CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN DAYOFWEEK(date_day) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,
  
  -- Relative date flags (these will be accurate at query time)
  CASE WHEN date_day = current_date() THEN TRUE ELSE FALSE END AS is_today,
  CASE WHEN date_day = DATE_SUB(current_date(), 1) THEN TRUE ELSE FALSE END AS is_yesterday,
  CASE WHEN date_day >= DATE_TRUNC('WEEK', current_date()) THEN TRUE ELSE FALSE END AS is_current_week,
  CASE WHEN date_day >= DATE_TRUNC('MONTH', current_date()) THEN TRUE ELSE FALSE END AS is_current_month,
  CASE WHEN date_day >= DATE_TRUNC('QUARTER', current_date()) THEN TRUE ELSE FALSE END AS is_current_quarter,
  CASE WHEN date_day >= DATE_TRUNC('YEAR', current_date()) THEN TRUE ELSE FALSE END AS is_current_year,
  
  -- Days ago calculation
  DATEDIFF(current_date(), date_day) AS days_ago,
  
  -- Audit
  current_timestamp() AS _transformed_at

FROM date_spine;
