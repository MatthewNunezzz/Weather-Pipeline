import json
from datetime import UTC, datetime

import requests


def fetch_raw_weather(locations, yesterday):
    """Fetch yesterday's hourly weather data from Open-Meteo."""
    hourly_variables = [
        "temperature_2m",
        "relative_humidity_2m",
        "surface_pressure",
        "precipitation",
        "rain",
        "wind_speed_10m",
        "wind_direction_10m",
        "shortwave_radiation",
    ]

    params = {
        "latitude": ",".join(str(loc["lat"]) for loc in locations),
        "longitude": ",".join(str(loc["lon"]) for loc in locations),
        "hourly": ",".join(hourly_variables),
        "timezone": "GMT",
        "start_hour": f"{yesterday}T00:00",
        "end_hour": f"{yesterday}T23:00",
    }

    print("Fetching weather data ...")
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params=params,
    )
    response.raise_for_status()
    return response.json()


def upload_to_azure(data, locations, yesterday, hook):
    """Upload a weather payload to Azure through an Azure Data Lake hook."""
    file_name = f"bronze/weather_data_{yesterday}.json"
    container_name = "weather-data"

    file_system_client = hook.get_file_system(container_name)
    file_client = file_system_client.get_file_client(file_name)

    envelope = {
        "ingested_at_utc": datetime.now(UTC).isoformat(),
        "locations": locations,
        "payload": data,
    }
    json_bytes = json.dumps(envelope).encode("utf-8")

    print(f"Uploading file to file system '{container_name}' as '{file_name}'...")
    file_client.upload_data(json_bytes, overwrite=True)
    print("Upload completed successfully!")

    props = file_client.get_file_properties()
    print(
        f"Confirmed: {props.name}, {props.size} bytes, modified {props.last_modified}"
    )

    return file_name
