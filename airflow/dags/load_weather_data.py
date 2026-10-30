from datetime import datetime, timezone, timedelta
import pandas as pd
import io
import json
import requests
from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()

locations = [
    {"name": "Dallas, TX",        "lat": 32.7767, "lon": -96.7970,  "timezone": "America/Chicago"},
    {"name": "Chicago, IL",       "lat": 41.8781, "lon": -87.6298,  "timezone": "America/Chicago"},
    {"name": "Denver, CO",        "lat": 39.7392, "lon": -104.9903, "timezone": "America/Denver"},
    {"name": "Phoenix, AZ",       "lat": 33.4484, "lon": -112.0740, "timezone": "America/Phoenix"},
    {"name": "Los Angeles, CA",   "lat": 34.0522, "lon": -118.2437, "timezone": "America/Los_Angeles"},
    {"name": "Anchorage, AK",     "lat": 61.2181, "lon": -149.9003, "timezone": "America/Anchorage"},
    {"name": "Honolulu, HI",      "lat": 21.3099, "lon": -157.8581, "timezone": "Pacific/Honolulu"},
    {"name": "New York City, NY", "lat": 40.7128, "lon": -74.0060,  "timezone": "America/New_York"},
    {"name": "Las Vegas, NV",     "lat": 36.1699, "lon": -115.1398, "timezone": "America/Los_Angeles"},
    {"name": "Boston, MA",        "lat": 42.3601, "lon": -71.0589,  "timezone": "America/New_York"},
]

@dag(
    dag_id="load_weather_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
)
def load_weather_data():
    @task
    def fetch_raw_weather():
        """Fetches yesterday's hourly weather data from Open-Meteo API."""
        hourly_variables = [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "precipitation",        # total: rain + showers + snow
            "rain",
            "wind_speed_10m",
            "wind_direction_10m",
            "shortwave_radiation",  # Shortwave Solar Radiation (GHI)
        ]

        api_url = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": ",".join(str(loc["lat"]) for loc in locations),
            "longitude": ",".join(str(loc["lon"]) for loc in locations),
            "hourly": ",".join(hourly_variables),
            "timezone": "GMT",
            "start_hour": f"{yesterday}T00:00",
            "end_hour": f"{yesterday}T23:00",
        }

        print(f"Fetching weather data ...")
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        return response.json()

    @task
    def upload_to_azure(data):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id="weather_adls")

        # Generate a unique file name using UTC date to avoid overwriting files
        file_name = f"bronze/weather_data_{yesterday}.json"
        container_name = "weather-data"

        # Get a client for the target file inside the file system
        file_system_client = hook.get_file_system(container_name)
        file_client = file_system_client.get_file_client(file_name)

        # Wrap payload in an envelope with context
        envelope = {
            "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            "locations": locations,
            "payload": data,
        }
        json_bytes = json.dumps(envelope).encode("utf-8")

        # Upload json string data to Azure
        print(f"Uploading file to file system '{container_name}' as '{file_name}'...")
        file_client.upload_data(json_bytes, overwrite=True)
        print("Upload completed successfully!")

        props = file_client.get_file_properties()
        print(f"Confirmed: {props.name}, {props.size} bytes, modified {props.last_modified}")

        return file_name

    data = fetch_raw_weather()
    file_path = upload_to_azure(data)

    merge_to_silver = DatabricksRunNowOperator(
        task_id="merge_to_silver",
        databricks_conn_id="databricks_default",
        job_id=109920245107190,
        python_params=["--file_path", file_path],
    )

load_weather_data()