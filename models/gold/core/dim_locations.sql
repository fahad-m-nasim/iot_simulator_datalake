with iot_events as (
   select * from {{ ref('slvr_iot_events') }}
)
select
    location_id
from iot_events
group by location_id;
