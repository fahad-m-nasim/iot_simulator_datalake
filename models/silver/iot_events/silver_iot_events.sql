{{ config(
    materialized='streaming_table',
    meta = {
        'target_schema': 'silver'
    }
) }}

-- Streaming table: incrementally processes changes from bronze
-- Powered by Delta Live Tables (DLT) serverless pipeline behind the scenes
with src_iot_events as (
   select * from STREAM({{ ref('bronze_iot_events') }})
)
select
    device_id
    , location_id
    , cast(timestamp as timestamp) as timestamp
    , sensor_type
    , quality_flag
    , unit
    , value
from src_iot_events
where lower(trim(quality_flag)) in ('good', 'suspect')
;
