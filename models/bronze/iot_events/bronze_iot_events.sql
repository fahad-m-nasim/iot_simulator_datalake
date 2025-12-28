{{ config(
    materialized='streaming_table',
    meta = {
        'target_schema': 'bronze'
    }
) }}

-- Streaming table with Auto Loader: automatically processes new files as they arrive
-- Powered by Delta Live Tables (DLT) serverless pipeline behind the scenes
select
    *
from STREAM read_files(
    '{{ var("iot_events_path") }}',
    format => 'json'
)
;