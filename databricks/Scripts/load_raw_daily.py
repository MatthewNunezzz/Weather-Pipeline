import argparse
from datetime import UTC, datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process bronze weather JSON to silver Delta table."
    )
    parser.add_argument(
        "--file_path",
        type=str,
        default="",
        help="Relative path to the bronze JSON file in ABFSS storage.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    file_path = args.file_path

    # Initialize Spark Session
    spark = SparkSession.builder.appName("ProcessSilverHourlyWeather").getOrCreate()

    # If run manually or file_path is empty, default to yesterday's file
    if not file_path:
        yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
        file_path = f"bronze/weather_data_{yesterday}.json"

    path = f"abfss://weather-data@weatherdata2012.dfs.core.windows.net/{file_path}"
    table_name = "capstoneweatherproject1669.silver.hourly_weather"

    # Define list of weather metrics
    metrics = [
        "temperature_2m",
        "relative_humidity_2m",
        "surface_pressure",
        "precipitation",
        "rain",
        "wind_speed_10m",
        "wind_direction_10m",
        "shortwave_radiation",
    ]

    # Set the Spark session timezone to UTC
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    print(f"Reading data from: {path}")

    # Read the JSON file into a Spark DataFrame
    raw_df = spark.read.option("multiLine", True).json(path)

    # Unpack city data into individual rows
    cities = raw_df.select(
        F.to_timestamp("ingested_at_utc").alias("ingested_at_utc"),
        F.explode(F.arrays_zip("locations", "payload")).alias("city"),
    )

    # Unpack hourly data into individual rows
    hours = cities.select(
        "ingested_at_utc",
        F.col("city.locations.name").alias("location_name"),
        F.col("city.locations.timezone").alias("timezone"),
        F.col("city.payload.latitude").cast("double").alias("latitude"),
        F.col("city.payload.longitude").cast("double").alias("longitude"),
        F.explode(
            F.arrays_zip(
                F.col("city.payload.hourly.time").alias("time"),
                *[F.col(f"city.payload.hourly.{m}").alias(m) for m in metrics],
            )
        ).alias("h"),
    )

    # Construct the final DataFrame
    hourly_df = hours.select(
        "location_name",
        F.to_timestamp("h.time").alias("time_utc"),
        "timezone",
        "latitude",
        "longitude",
        *[F.col(f"h.{m}").cast("double").alias(m) for m in metrics],
        "ingested_at_utc",
    ).withColumn("time_local", F.from_utc_timestamp("time_utc", F.col("timezone")))

    # Check for empty DataFrame
    if hourly_df.count() == 0:
        raise ValueError("No rows unpacked from the envelope; aborting merge")

    # Check for duplicate keys
    dup_keys = (
        hourly_df.groupBy("location_name", "time_utc")
        .count()
        .filter("count > 1")
        .count()
    )
    if dup_keys > 0:
        raise ValueError(f"{dup_keys} duplicate location/hour keys; aborting merge")

    print(f"Executing Delta merge into target table: {table_name}")

    # Write the DataFrame to silver.hourly_weather table
    (
        DeltaTable.forName(spark, table_name)
        .alias("t")
        .merge(
            hourly_df.alias("s"),
            "t.location_name = s.location_name AND t.time_utc = s.time_utc",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Display Merge operation metrics
    history = DeltaTable.forName(spark, table_name).history(1)
    m = history.select("operationMetrics").collect()[0][0]
    print("--- Delta Merge Metrics ---")
    print("source rows:", m.get("numSourceRows"))
    print("inserted:   ", m.get("numTargetRowsInserted"))
    print("updated:    ", m.get("numTargetRowsUpdated"))


if __name__ == "__main__":
    main()
