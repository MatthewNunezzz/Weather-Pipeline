# tests/test_process_silver.py
import json

import pytest
from pyspark.errors import AnalysisException

from load_raw_daily import METRICS, unpack_hourly, validate_hourly


def make_envelope(times=("2026-10-01T00:00", "2026-10-01T01:00"),
                  cities=("dallas",), drop_metric=None):
    payload = []
    for _ in cities:
        hourly = {"time": list(times)}
        for m in METRICS:
            if m != drop_metric:
                hourly[m] = [1.0] * len(times)
        payload.append({"latitude": 32.78, "longitude": -96.8, "hourly": hourly})
    return {
        "ingested_at_utc": "2026-10-02T01:00:00Z",
        "locations": [{"name": c, "timezone": "America/Chicago"} for c in cities],
        "payload": payload,
    }


def read_envelope(spark, tmp_path, envelope):
    p = tmp_path / "envelope.json"
    p.write_text(json.dumps(envelope))
    return spark.read.option("multiLine", True).json(str(p))   # same read as production


def test_valid_input_unpacks_to_one_row_per_hour(spark, tmp_path):
    df = unpack_hourly(read_envelope(spark, tmp_path, make_envelope()))
    validate_hourly(df)  # should not raise
    rows = df.orderBy("time_utc").collect()
    assert len(rows) == 2
    assert rows[0]["location_name"] == "dallas"
    assert rows[0]["temperature_2m"] == 1.0
    assert rows[0]["time_local"].hour == 19   # 00:00 UTC -> 19:00 CDT the day before
    assert {"time_utc", "time_local", "ingested_at_utc"} <= set(df.columns)


def test_zero_hourly_records_raises(spark, tmp_path):
    env = make_envelope(times=())
    df = unpack_hourly(read_envelope(spark, tmp_path, env))
    with pytest.raises(ValueError, match="No rows unpacked"):
        validate_hourly(df)


def test_missing_metric_column_raises(spark, tmp_path):
    env = make_envelope(drop_metric="rain")
    with pytest.raises(AnalysisException):
        unpack_hourly(read_envelope(spark, tmp_path, env))


def test_duplicate_location_hour_raises(spark, tmp_path):
    env = make_envelope(cities=("dallas", "dallas"))
    df = unpack_hourly(read_envelope(spark, tmp_path, env))
    with pytest.raises(ValueError, match="duplicate"):
        validate_hourly(df)