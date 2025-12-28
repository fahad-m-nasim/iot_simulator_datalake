{{ config(meta = {
    'target_schema':'gold'
}) }}


with iot_events as (
   select * from {{ ref('silver_iot_events') }}
),
dim_locations as (
   select * from {{ ref('dim_locations') }}
),
dim_date as (
   select * from {{ ref('dim_date') }}
)
select
    evt.location_id
    , sensor_type
    , quality_flag
    , d.year
    , d.month
    , avg(value) as avg_value
from iot_events evt
    left outer join dim_locations l on evt.location_id = l.location_id
    left outer join dim_date d on cast(evt.timestamp as date) = d.date
group by evt.location_id, sensor_type, quality_flag, d.year, d.month;
