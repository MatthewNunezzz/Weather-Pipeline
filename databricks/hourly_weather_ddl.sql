CREATE TABLE IF NOT EXISTS capstoneweatherproject1669.silver.hourly_weather (
    location_name         STRING    COMMENT 'City name, e.g. Dallas, TX',
    time_utc              TIMESTAMP COMMENT 'Hour of the observation in UTC',
    timezone              STRING    COMMENT 'IANA timezone of the city, e.g. America/Chicago',
    latitude              DOUBLE,
    longitude             DOUBLE,
    temperature_2m        DOUBLE    COMMENT 'Air temperature at 2 m, °C',
    relative_humidity_2m  DOUBLE    COMMENT 'Relative humidity at 2 m, %',
    surface_pressure      DOUBLE    COMMENT 'Surface pressure, hPa',
    precipitation         DOUBLE    COMMENT 'Total precipitation (rain + showers + snow), mm',
    rain                  DOUBLE    COMMENT 'Rain, mm',
    wind_speed_10m        DOUBLE    COMMENT 'Wind speed at 10 m, km/h',
    wind_direction_10m    DOUBLE    COMMENT 'Wind direction at 10 m, degrees',
    shortwave_radiation   DOUBLE    COMMENT 'Shortwave solar radiation (GHI), W/m²',
    ingested_at_utc       TIMESTAMP COMMENT 'When the envelope was fetched, UTC',
    time_local            TIMESTAMP COMMENT 'Local wall-clock time in the city, read together with timezone'
)
USING delta
COMMENT 'One row per location per hour';