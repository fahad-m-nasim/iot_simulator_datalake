-- =============================================================================
-- Bronze Layer: CDC Orders Ingestion
-- =============================================================================
-- Ingests CDC events for orders table from Unity Catalog Volume
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_orders
(
  order_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  device_id STRING,
  quantity STRING,
  total_amount STRING,
  order_status STRING,
  order_date BIGINT,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for orders table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  order_id,
  customer_id,
  product_id,
  device_id,
  quantity,
  total_amount,
  order_status,
  order_date,
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
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_orders/',
  format => 'json',
  inferColumnTypes => true
);
