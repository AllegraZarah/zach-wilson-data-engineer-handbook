from pyspark.sql import SparkSession

query = """
       WITH dates AS (
            SELECT explode(sequence(to_date('2023-01-01'), to_date('2023-01-31'), interval 1 day)) AS date
        ),

        row_numbered_events AS (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY user_id, device_id, date(event_time) ORDER BY event_time) AS row_number
            FROM events
            WHERE user_id IS NOT NULL
        ),

        deduped_events AS (
            SELECT *
            FROM row_numbered_events
            WHERE row_number = 1
        ),

        row_numbered_devices AS (
            SELECT *,
                ROW_NUMBER() OVER(PARTITION BY device_id, browser_type ORDER BY device_id) AS row_number
            FROM devices
        ),

        deduped_devices AS (
            SELECT *
            FROM row_numbered_devices
            WHERE row_number = 1
        ),

        events_start_date AS (
            SELECT user_id, 
                device_id, 
                MIN(event_time) AS first_date
            FROM deduped_events
            GROUP BY user_id, device_id
        ),

        events_and_dates AS (
            SELECT *
            FROM events_start_date
            CROSS JOIN dates
            WHERE date(events_start_date.first_date) <= date(dates.date)
        ),

        windowed AS (
            SELECT ed.user_id, 
                ed.device_id,
                d.browser_type,
                array_remove(collect_list(
                    CASE WHEN e.user_id IS NOT NULL AND e.device_id IS NOT NULL
                            THEN date(e.event_time)
                    END
                ) OVER (PARTITION BY ed.user_id, ed.device_id, d.browser_type ORDER BY ed.date), NULL) AS device_activity_datelist,
                date(ed.date) AS date
                
            FROM events_and_dates ed
            LEFT JOIN deduped_events e
            ON ed.user_id = e.user_id
            AND ed.device_id = e.device_id
            AND date(ed.date) = date(e.event_time)
            JOIN deduped_devices d  
            ON ed.device_id = d.device_id
        )

        SELECT *
        FROM windowed
        ORDER BY date DESC, user_id, device_id, browser_type
"""

def do_user_devices_cummulated_transformation(spark, dataframe1, dataframe2):
    dataframe1.createOrReplaceTempView("events")
    dataframe2.createOrReplaceTempView("devices")

    return spark.sql(query)

def main():
    spark = SparkSession.builder \
            .master("local") \
            .appName("user_devices_cumulated") \
            .getOrCreate()

    output_df = do_user_devices_cummulated_transformation(spark, spark.table("events"), spark.table("devices"))
    output_df.write.mode("overwrite").insertInto("user_devices_cumulated")