{{ config(
    materialized='incremental',
    unique_key='weather_event_id',
    incremental_strategy='merge'
) }}

with raw_hourly_weather as (
    select *
    from {{ source('silver', 'hourly_weather') }}

    {% if is_incremental() %}
        where ingested_at_utc > (select max(ingested_at_utc) from {{ this }})
    {% endif %}
),
weather_with_location as (
    select
        raw_hourly_weather.*,
        dim_locations.location_id
    from raw_hourly_weather
    left join {{ ref('dim_locations') }} as dim_locations
      on raw_hourly_weather.location_name = dim_locations.location_name
)
select
    md5(cast(location_id as string) || '-' || cast(time_utc as string)) as weather_event_id
    , location_id
    , time_utc
    , time_local
    , temperature_2m
    , relative_humidity_2m
    , surface_pressure
    , precipitation
    , rain
    , wind_speed_10m
    , wind_direction_10m
    , shortwave_radiation
    , ingested_at_utc
from weather_with_location

