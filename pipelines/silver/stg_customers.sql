-- =============================================================================
-- Silver Layer: Staging Customers (CDC SCD Type 1)
-- =============================================================================
-- Applies SCD Type 1 logic to get current state of customers
-- Processes CDC events: r=snapshot, c=create, u=update, d=delete
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW stg_customers
COMMENT 'Silver layer: Current state of customers from CDC events (SCD Type 1)'
TBLPROPERTIES (
  'quality' = 'silver',
  'pipelines.autoOptimize.managed' = 'true'
)
AS 
WITH source AS (
  SELECT * FROM LIVE.cdc_customers
),

-- Get the latest CDC event per customer
ranked AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id 
      ORDER BY COALESCE(_cdc_timestamp, _ingested_at) DESC
    ) AS _rn
  FROM source
),

latest_records AS (
  SELECT * FROM ranked WHERE _rn = 1
),

transformed AS (
  SELECT
    -- Surrogate key
    md5(customer_id) AS _surrogate_key,
    
    -- Business keys
    customer_id,
    
    -- Attributes
    company_name,
    contact_name,
    email,
    phone,
    industry,
    subscription_tier,
    
    -- Timestamps
    CASE 
      WHEN created_at IS NOT NULL THEN 
        from_unixtime(created_at / 1000)
      ELSE _cdc_timestamp
    END AS created_at,
    CASE 
      WHEN updated_at IS NOT NULL THEN 
        from_unixtime(updated_at / 1000)
      ELSE _cdc_timestamp
    END AS updated_at,
    
    -- CDC metadata
    __op AS _cdc_operation,
    COALESCE(__deleted = 'true', __op = 'd', FALSE) AS _is_deleted,
    _cdc_timestamp,
    
    -- Audit columns
    _ingested_at,
    current_timestamp() AS _transformed_at
    
  FROM latest_records
  WHERE customer_id IS NOT NULL
)

SELECT * FROM transformed;
