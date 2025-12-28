select
    device_id,
    sum(value) as value
from {{ ref('silver_iot_events') }}
group by 1
having value < 0