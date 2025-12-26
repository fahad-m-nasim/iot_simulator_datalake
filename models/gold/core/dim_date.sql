with iot_events as (
   select * from {{ ref('slvr_iot_events') }}
)
select
    distinct
    cast( timestamp as date) as date
    , year(timestamp) as year
    , month(timestamp) as month
    , day(timestamp) as day
from iot_events
group by cast( timestamp as date)
;
