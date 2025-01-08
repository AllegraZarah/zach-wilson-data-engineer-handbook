from chispa.dataframe_comparer import *
from ..jobs.user_devices_cummulated_job import do_user_devices_cummulated_transformation
from collections import namedtuple
# from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

Events = namedtuple("Events", "user_id device_id event_time")
Devices = namedtuple("Devices", "device_id browser_type")
UserDevices = namedtuple("UserDevices", "user_id device_id browser_type device_activity_datelist date")

def test_user_devices_cummulated(spark):
    # Sample data
    events_data = [
        Events(1, 1001, "2021-01-01 10:00:00"),
        Events(1, 1001, "2021-01-02 10:00:00"),
        Events(1, 1001, "2021-01-03 10:00:00"),
        Events(1, 1002, "2021-01-01 10:00:00"),
        Events(1, 1002, "2021-01-02 10:00:00"),
        Events(2, 1001, "2021-01-01 10:00:00"),
        Events(2, 1001, "2021-01-02 10:00:00")
    ]

    devices_data = [
        Devices(1001, "Chrome"),
        Devices(1001, "Firefox"),
        Devices(1002, "Firefox"),
        Devices(1002, "Chrome"),
        Devices(1002, "Safari"),
    ]

    # Create DataFrames
    events_df = spark.createDataFrame(events_data)
    devices_df = spark.createDataFrame(devices_data)

    # Register DataFrame as a temporary view
    events_df.createOrReplaceTempView("events_df")
    devices_df.createOrReplaceTempView("devices_df")

    # Print input DataFrame for debugging
    events_df.show()
    devices_df.show()


    # Run the transformation
    actual_df = do_user_devices_cummulated_transformation(spark, events_df, devices_df)

     # Print actual DataFrame for debugging
    print(len(actual_df))
    actual_df.show()

    # Expected output after transformation
    expected_output = [
        UserDevices(1, 1001, "Chrome", ["2021-01-01", "2021-01-02", "2021-01-03"], "2021-01-03"),
        UserDevices(1, 1002, "Firefox", ["2021-01-01", "2021-01-02"], "2021-01-02"),
        UserDevices(2, 1001, "Chrome", ["2021-01-01", "2021-01-02"], "2021-01-02")
    ]

    expected_df = spark.createDataFrame(expected_output)

    # Convert the expected DataFrame columns to match the actual DataFrame schema
    expected_df = expected_df.withColumn("device_activity_datelist", col("device_activity_datelist").cast("array<date>"))
    expected_df = expected_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

    # Sort both DataFrames before comparison
    # actual_df_sorted = actual_df.orderBy("user_id", "device_id", "date")
    # expected_df_sorted = expected_df.orderBy("user_id", "device_id", "date")

    # Assert the results
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)