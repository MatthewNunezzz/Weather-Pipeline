from datetime import datetime, timezone
import json
import requests
from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook


@dag(
    dag_id="upload_raw_weather_data",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def upload_raw_weather_data():
    @task
    def fetch_weather_data():
        """Fetches current weather and hourly forecasts from Open-Meteo API."""
        LATITUDE = 29.4241
        LONGITUDE = -98.4936
        API_URL = "https://api.open-meteo.com/v1/forecast"

        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": (
                "temperature_2m,relative_humidity_2m,apparent_temperature,wind_speed_10m"
            ),
            "hourly": "temperature_2m,precipitation_probability,precipitation",
            "timezone": "auto",
        }

        print(f"Fetching weather data for coordinates ({LATITUDE}, {LONGITUDE})...")
        response = requests.get(API_URL, params=params)
        response.raise_for_status()

        return response.json()

    @task
    def upload_to_azure(data):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id="weather_adls")

        # Generate a unique file name using UTC timestamp to avoid overwriting files
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        blob_name = f"bronze/weather_data_{timestamp}.json"
        container_name = "weather-data"

        data_json = json.dumps(data, indent=2)

        # Get a client for the target file inside the container
        file_system_client = hook.get_file_system(container_name)
        file_client = file_system_client.get_file_client(blob_name)

        # Upload data to Azure
        print(f"Uploading file to container '{container_name}' as '{blob_name}'...")
        file_client.upload_data(data_json, overwrite=True)
        print("Upload completed successfully!")


    data = fetch_weather_data()
    upload_to_azure(data)


upload_raw_weather_data()