-- =============================================================================
-- Bronze Layer: CDC Customers Ingestion
-- =============================================================================
-- Ingests CDC events for customers table from Unity Catalog Volume
-- CDC Format: Debezium with __op, __ts_ms, __deleted fields
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_customers
(
  customer_id STRING NOT NULL,
  company_name STRING,
  contact_name STRING,
  email STRING,
  phone STRING,
  industry STRING,
  subscription_tier STRING,
  created_at BIGINT,
  updated_at BIGINT,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  -- Data quality constraints
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_cdc_operation EXPECT (__op IN ('r', 'c', 'u', 'd')) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for customers table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  customer_id,
  company_name,
  contact_name,
  email,
  phone,
  industry,
  subscription_tier,
  created_at,
  updated_at,
  __op,
  __ts_ms,
  __deleted,
  current_timestamp() AS _ingested_at,
  current_date() AS _ingestion_date,
  _metadata.file_path AS _source_file,
  CASE 
    WHEN __ts_ms IS NOT NULL THEN from_unixtime(__ts_ms / 1000)
    ELSE NULL 
  END AS _cdc_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_customers/',
  format => 'json',
  inferColumnTypes => true
);
