from chispa.dataframe_comparer import assert_df_equality
from ..jobs.incremental_update_actors_scd_job import do_incremental_update_actors_scd_transformation
from collections import namedtuple
from pyspark.sql import functions as F

ActorHistoryScd = namedtuple("ActorHistoryScd", "actor_id actor quality_class is_active start_date end_date current_year")
Actor = namedtuple("Actor", "actor_id actor quality_class is_active year")

def test_incremental_update_actors_scd(spark):
    # 2020 historical data
    historical_data = [
        ActorHistoryScd(1, "Tom Hanks", "A", True, 2020, 2020, 2020),    # Will remain unchanged
        ActorHistoryScd(2, "Tom Cruise", "B", True, 2020, 2020, 2020),   # Will change quality_class
        ActorHistoryScd(3, "Brad Pitt", "C", True, 2017, 2020, 2020),    # Will change is_active
        # Historical record from before 2020
        ActorHistoryScd(1, "Tom Hanks", "B", True, 2018, 2019, 2020)
    ]

    # 2021 current data
    current_data = [
        Actor(1, "Tom Hanks", "A", True, 2021),      # Unchanged
        Actor(2, "Tom Cruise", "A", True, 2021),     # Changed quality_class from B to A
        Actor(3, "Brad Pitt", "C", False, 2021),     # Changed is_active from True to False
        Actor(4, "Leonardo DiCaprio", "A", True, 2021)  # New record
    ]

    # Create DataFrames
    historical_df = spark.createDataFrame(historical_data)
    current_df = spark.createDataFrame(current_data)

    # Run the transformation
    actual_df = do_incremental_update_actors_scd_transformation(spark, historical_df, current_df)


    # Expected output after SCD transformation
    expected_output = [
        # Historical record (preserved)
        ActorHistoryScd(1, "Tom Hanks", "B", True, 2018, 2019, 2021),
        
        # Unchanged record
        ActorHistoryScd(1, "Tom Hanks", "A", True, 2020, 2021, 2021),
        
        # Changed records (old records preserved with end_date=2020)
        ActorHistoryScd(2, "Tom Cruise", "B", True, 2020, 2020, 2021),
        ActorHistoryScd(2, "Tom Cruise", "A", True, 2021, 2021, 2021),
        
        ActorHistoryScd(3, "Brad Pitt", "C", True, 2017, 2020, 2021),
        ActorHistoryScd(3, "Brad Pitt", "C", False, 2021, 2021, 2021),
        
        # New record
        ActorHistoryScd(4, "Leonardo DiCaprio", "A", True, 2021, 2021, 2021)
    ]

    expected_df = spark.createDataFrame(expected_output).withColumn("current_year", F.col("current_year").cast("int"))

    # Sort both DataFrames before comparison
    actual_df_sorted = actual_df.orderBy("actor_id", "start_date", "end_date")
    expected_df_sorted = expected_df.orderBy("actor_id", "start_date", "end_date")

    # Assert the results
    assert_df_equality(actual_df_sorted, expected_df_sorted, ignore_nullable=True)