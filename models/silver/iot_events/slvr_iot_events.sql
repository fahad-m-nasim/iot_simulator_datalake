with src_iot_events as (
   select * from {{ ref('brnz_ingest_iot_events') }}
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
;
