from pyspark.sql import SparkSession

query = """
WITH dates AS (
    SELECT explode(sequence(to_date('2023-01-01'), to_date('2023-01-31'), interval 1 day)) AS date
),

row_numbered_events AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY host, user_id, event_time ORDER BY event_time) AS row_number
    FROM events
    WHERE user_id IS NOT NULL
),

deduped_events AS (
    SELECT *
    FROM row_numbered_events
    WHERE row_number = 1
),

host_start_date AS (
    SELECT host, 
           MIN(event_time) AS first_date
    FROM deduped_events
    GROUP BY host
),

hosts_and_dates AS (
    SELECT *
    FROM host_start_date
    CROSS JOIN dates
    WHERE date(host_start_date.first_date) <= date(dates.date)
),

daily_aggregates AS (
    SELECT hd.host,
           date(hd.date) AS date,
           date_format(hd.date, 'MMM') AS month,
           COUNT(e.event_time) AS number_of_hits,
           COUNT(DISTINCT e.user_id) AS number_of_unique_visitors
    FROM hosts_and_dates hd
    LEFT JOIN deduped_events e
    ON hd.host = e.host
       AND date(hd.date) = date(e.event_time)
    GROUP BY hd.host, hd.date
)

SELECT month,
       host,
       collect_list(COALESCE(number_of_hits, 0)) AS hit_array,
       collect_list(COALESCE(number_of_unique_visitors, 0)) AS unique_visitors_array
FROM daily_aggregates
GROUP BY month, host
"""

def do_host_activity_reduced_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")

    return spark.sql(query)


def main():
    spark = SparkSession.builder \
            .master("local") \
            .appName("host_activity_reduced") \
            .getOrCreate()

    output_df = do_host_activity_reduced_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("host_activity_reduced")