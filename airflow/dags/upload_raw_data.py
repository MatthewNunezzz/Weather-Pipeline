from datetime import datetime, timezone
import pandas as pd
import io
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
        file_name = f"bronze/weather_data_{timestamp}.csv"
        container_name = "weather-data"

        df = pd.DataFrame(data)

        # Save the CSV directly to an in-memory string variable
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_string = csv_buffer.getvalue()

        # Get a client for the target file inside the file system
        file_system_client = hook.get_file_system(container_name)
        file_client = file_system_client.get_file_client(file_name)

        # Upload CSV string data to Azure (converted to bytes)
        print(f"Uploading file to file system '{container_name}' as '{file_name}'...")
        file_client.upload_data(csv_string.encode('utf-8'), overwrite=True)
        print("Upload completed successfully!")


    data = fetch_weather_data()
    upload_to_azure(data)


upload_raw_weather_data()