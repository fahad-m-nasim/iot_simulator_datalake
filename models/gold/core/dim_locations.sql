{{ config(
      target_schema="gold"
) }}

with iot_events as (
   select * from {{ ref('silver_iot_events') }}
)
select
    location_id
from iot_events
group by location_id;
