{{ config(materialized='table') }}

with distinct_locations as (
    select distinct
        location_name
      , latitude
      , longitude
      , timezone
    from {{ source('silver', 'hourly_weather') }}
)

select
    md5(location_name) as location_id
  , location_name
  , latitude
  , longitude
  , timezone
from distinct_locations
