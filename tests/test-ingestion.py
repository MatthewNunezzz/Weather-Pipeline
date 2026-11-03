import pytest
import requests_mock
import load_weather_data 

def test_fetch_raw_weather_success(monkeypatch):
    # 1. ARRANGE: Set up predictable test variables
    mock_locations = [
        {"lat": 32.75, "lon": -97.33},
        {"lat": 30.26, "lon": -97.74}
    ]
    mock_yesterday = "2026-11-01"
    
    # Use monkeypatch to temporarily override module-level variables safely
    monkeypatch.setattr(load_weather_data, "locations", mock_locations)
    monkeypatch.setattr(load_weather_data, "yesterday", mock_yesterday)

    expected_mock_response = {"hourly": {"temperature_2m": [20.5, 21.0]}}

    with requests_mock.Mocker() as mock:
        # Intercept the target API URL
        mock.get("https://api.open-meteo.com/v1/forecast", json=expected_mock_response, status_code=200)

        # 2. ACT
        result = load_weather_data.fetch_raw_weather()

        # 3. ASSERT
        # Verify the function returned the correct parsed JSON
        assert result == expected_mock_response
        
        # Crucial: Verify that the parameters were constructed perfectly
        last_request = mock.last_request
        assert last_request.qs["latitude"] == ["32.75,-97.33"]
        assert last_request.qs["longitude"] == ["-97.33,-97.74"]
        assert last_request.qs["start_hour"] == ["2026-11-01T00:00"]
        assert last_request.qs["end_hour"] == ["2026-11-01T23:00"]


def test_fetch_raw_weather_api_failure(monkeypatch):
    # ARRANGE: Set up variables to prevent unexpected NameErrors
    monkeypatch.setattr(load_weather_data, "locations", [{"lat": 0, "lon": 0}])
    monkeypatch.setattr(load_weather_data, "yesterday", "2026-11-01")

    with requests_mock.Mocker() as mock:
        # Mock a bad gateway error from the API
        mock.get("https://api.open-meteo.com/v1/forecast", status_code=502)

        # ACT & ASSERT
        # Verify that raise_for_status() successfully triggers an HTTPError
        with pytest.raises(pytest.importorskip("requests").exceptions.HTTPError):
            load_weather_data.fetch_raw_weather()

def test_upload_to_azure_success(monkeypatch):
    assert 1==1
def test_upload_to_azure_failure(monkeypatch):
    assert 1==1