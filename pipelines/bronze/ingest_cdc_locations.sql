-- =============================================================================
-- Bronze Layer: CDC Locations Ingestion
-- =============================================================================
-- Ingests CDC events for locations dimension table from Unity Catalog Volume
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE cdc_locations
(
  location_id STRING NOT NULL,
  location_name STRING,
  building STRING,
  floor STRING,
  zone STRING,
  latitude STRING,
  longitude STRING,
  __op STRING,
  __ts_ms BIGINT,
  __deleted STRING,
  _ingested_at TIMESTAMP,
  _ingestion_date DATE,
  _source_file STRING,
  _cdc_timestamp TIMESTAMP,
  
  CONSTRAINT valid_location_id EXPECT (location_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Bronze layer: CDC events for locations table'
PARTITIONED BY (_ingestion_date)
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS SELECT
  location_id,
  location_name,
  building,
  floor,
  zone,
  latitude,
  longitude,
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
  '/Volumes/${catalog}/landing/vol01/cdc/cdc_dim_location/',
  format => 'json',
  inferColumnTypes => true
);
