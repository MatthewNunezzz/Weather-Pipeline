import json
from datetime import UTC, datetime
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import requests_mock
from weather_ingestion import fetch_weather_data, upload_weather_to_azure


def test_fetch_raw_weather_success():
    mock_locations = [{"lat": 32.75, "lon": -97.33}]
    mock_yesterday = "2026-11-01"

    expected_mock_response = {
        "hourly": {
            "temperature_2m": [26.4, 24.4],
            "time": ["2026-11-01T00:00", "2026-11-01T23:00"],
        },
        "latitude": 32.75,
        "timezone": "GMT",
        "elevation": 137.0,
        "longitude": -97.33,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "\u00b0C",
        },
        "generationtime_ms": 0.11050701141357422,
        "utc_offset_seconds": 0,
        "timezone_abbreviation": "GMT",
    }

    with requests_mock.Mocker() as mock:
        mock.get(
            "https://api.open-meteo.com/v1/forecast",
            json=expected_mock_response,
            status_code=200,
        )

        result = fetch_weather_data(mock_locations, mock_yesterday)

        assert result == expected_mock_response
        query = parse_qs(urlparse(mock.last_request.url).query)
        assert query["latitude"] == ["32.75"]
        assert query["longitude"] == ["-97.33"]
        assert query["start_hour"] == ["2026-11-01T00:00"]
        assert query["end_hour"] == ["2026-11-01T23:00"]


def test_fetch_raw_weather_api_failure():
    with requests_mock.Mocker() as mock:
        mock.get("https://api.open-meteo.com/v1/forecast", status_code=502)

        with pytest.raises(requests.exceptions.HTTPError):
            fetch_weather_data([{"lat": 0, "lon": 0}], "2026-11-01")


def test_upload_to_azure_success():
    locations = [{"name": "Dallas, TX", "lat": 32.7767, "lon": -96.7970}]
    payload = {"hourly": {"temperature_2m": [20.5]}}
    file_client = FakeFileClient()
    hook = FakeAzureHook(file_client)

    result = upload_weather_to_azure(payload, locations, "2026-11-01", hook)

    assert result == "bronze/weather_data_2026-11-01.json"
    assert hook.container_name == "weather-data"
    assert file_client.file_name == result
    assert file_client.overwrite is True

    envelope = json.loads(file_client.data.decode("utf-8"))
    assert envelope["locations"] == locations
    assert envelope["payload"] == payload
    assert datetime.fromisoformat(envelope["ingested_at_utc"]).tzinfo == UTC


def test_upload_to_azure_failure_propagates():
    file_client = FakeFileClient(upload_error=RuntimeError("Azure upload failed"))
    hook = FakeAzureHook(file_client)

    with pytest.raises(RuntimeError, match="Azure upload failed"):
        upload_weather_to_azure({}, [], "2026-11-01", hook)


class FakeAzureHook:
    def __init__(self, file_client):
        self.file_client = file_client
        self.container_name = None

    def get_file_system(self, container_name):
        self.container_name = container_name
        return self

    def get_file_client(self, file_name):
        self.file_client.file_name = file_name
        return self.file_client


class FakeFileClient:
    def __init__(self, upload_error=None):
        self.upload_error = upload_error
        self.data = None
        self.overwrite = None
        self.file_name = None

    def upload_data(self, data, overwrite):
        if self.upload_error:
            raise self.upload_error
        self.data = data
        self.overwrite = overwrite

    def get_file_properties(self):
        return SimpleNamespace(
            name=self.file_name,
            size=len(self.data),
            last_modified="2026-11-02T00:00:00Z",
        )
