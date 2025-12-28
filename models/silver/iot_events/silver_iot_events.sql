{{ config(
      target_schema="silver"
) }}

with src_iot_events as (
   select * from {{ ref('bronze_iot_events') }}
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
