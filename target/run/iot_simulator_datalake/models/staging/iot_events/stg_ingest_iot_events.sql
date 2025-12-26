
  
    
        create or replace table `dev_catalog`.`bronze`.`stg_ingest_iot_events`
      
      
    using delta
  
      
      
      
      
      
      
      
      as
      select
    *
from read_files(
    '/Volumes/dev_catalog/landing/vol01/iot_events/iot_events/',
    FORMAT => 'JSON'
)
LIMIT 2
  