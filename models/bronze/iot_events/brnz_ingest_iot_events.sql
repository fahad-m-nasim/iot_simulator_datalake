select
    *
from read_files(
    {{ var("iot_events_path") }},
    FORMAT => 'JSON'
)
;