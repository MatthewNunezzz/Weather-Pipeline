from datetime import UTC, datetime, timedelta

from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook
from airflow.sdk import dag, task
from weather_ingestion import (
    fetch_raw_weather as fetch_weather_data,
)
from weather_ingestion import (
    upload_to_azure as upload_weather_to_azure,
)

yesterday = (datetime.now(UTC) - timedelta(days=1)).date()

locations = [
    {
        "name": "Dallas, TX",
        "lat": 32.7767,
        "lon": -96.7970,
        "timezone": "America/Chicago",
    },
    {
        "name": "Chicago, IL",
        "lat": 41.8781,
        "lon": -87.6298,
        "timezone": "America/Chicago",
    },
    {
        "name": "Denver, CO",
        "lat": 39.7392,
        "lon": -104.9903,
        "timezone": "America/Denver",
    },
    {
        "name": "Phoenix, AZ",
        "lat": 33.4484,
        "lon": -112.0740,
        "timezone": "America/Phoenix",
    },
    {
        "name": "Los Angeles, CA",
        "lat": 34.0522,
        "lon": -118.2437,
        "timezone": "America/Los_Angeles",
    },
    {
        "name": "Anchorage, AK",
        "lat": 61.2181,
        "lon": -149.9003,
        "timezone": "America/Anchorage",
    },
    {
        "name": "Honolulu, HI",
        "lat": 21.3099,
        "lon": -157.8581,
        "timezone": "Pacific/Honolulu",
    },
    {
        "name": "New York City, NY",
        "lat": 40.7128,
        "lon": -74.0060,
        "timezone": "America/New_York",
    },
    {
        "name": "Las Vegas, NV",
        "lat": 36.1699,
        "lon": -115.1398,
        "timezone": "America/Los_Angeles",
    },
    {
        "name": "Boston, MA",
        "lat": 42.3601,
        "lon": -71.0589,
        "timezone": "America/New_York",
    },
]


@dag(
    dag_id="load_weather_data",
    start_date=datetime(2026, 1, 1),
    schedule="0 14 * * *",  # Minute 0, Hour 14 (2:00 PM UTC / 8:00 AM CST) every day
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
)
def load_weather_data():
    @task
    def fetch_raw_weather():
        return fetch_weather_data(locations, yesterday)

    @task
    def upload_to_azure(data):
        hook = AzureDataLakeStorageV2Hook(adls_conn_id="weather_adls")
        return upload_weather_to_azure(
            data,
            locations,
            yesterday,
            hook,
        )

    data = fetch_raw_weather()
    file_path = upload_to_azure(data)

    DatabricksRunNowOperator(
        task_id="build_datamart",
        databricks_conn_id="databricks_default",
        job_id=109920245107190,
        python_params=["--file_path", file_path],
    )


load_weather_data()
