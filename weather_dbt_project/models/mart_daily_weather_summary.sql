{{ config(materialized='table') }}

with joined_weather as (
    select
        fact_weather_hourly.*
      , dim_locations.location_name
    from {{ ref('fact_weather_hourly') }} as fact_weather_hourly
    left join {{ ref('dim_locations') }} as dim_locations
      on fact_weather_hourly.location_id = dim_locations.location_id
)
select
    location_name
  , cast(time_utc as date) as weather_date
  , round(avg(temperature_2m), 3) as avg_temperature
  , round(max(temperature_2m), 3) as max_temperature
  , round(min(temperature_2m), 3) as min_temperature
  , round(avg(relative_humidity_2m), 3) as avg_relative_humidity
  , round(sum(precipitation), 3) as total_precipitation
  , round(max(wind_speed_10m), 3) as max_wind_speed
  , round(avg(shortwave_radiation), 3) as avg_solar_radiation
from joined_weather
group by location_name, weather_date
