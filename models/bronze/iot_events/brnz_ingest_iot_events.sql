select
    *
from read_files(
    '/Volumes/dev_catalog/landing/vol01/iot_events/iot_events/',
    FORMAT => 'JSON'
)
;