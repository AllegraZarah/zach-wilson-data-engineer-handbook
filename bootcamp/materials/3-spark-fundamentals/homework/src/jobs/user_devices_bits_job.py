query = """
        WITH user_devices AS (
            SELECT *
            FROM user_devices_cumulated
            WHERE date = '2023-01-31'
        ),
        dates AS (
            SELECT explode(sequence(
                to_date('2023-01-01'), 
                to_date('2023-01-31'), 
                interval 1 day
            )) as series_date
        ),
        starter AS (
            SELECT 
                array_contains(udc.device_activity_datelist, date_format(d.series_date, 'yyyy-MM-dd')) AS device_is_active,
                datediff(to_date(udc.date), d.series_date) AS days_since,
                udc.user_id,
                udc.device_id,
                udc.browser_type,
                d.series_date
            FROM user_devices udc
            CROSS JOIN dates d
        ),
        bits AS (
            SELECT 
                user_id,
                device_id,
                browser_type,
                cast(sum(
                    case when device_is_active 
                    then power(2, 32 - days_since) 
                    else 0 
                    end
                ) as bigint) as datelist_int,
                date_format(series_date, 'yyyy-MM-dd') as date
            FROM starter
            GROUP BY user_id, device_id, browser_type, series_date
        ),
        binary_bits AS (
            SELECT 
                user_id,
                device_id,
                browser_type,
                lpad(bin(datelist_int), 32, '0') as datelist_int,
                date
            FROM bits
        )

        SELECT *
        FROM binary_bits
        ORDER BY date
    """

def do_user_devices_bits_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("user_devices_cumulated")
    return spark.sql(query)

def main():
    spark = SparkSession.builder\
        .master("local") \
        .appName("user_devices_bits") \
        .getOrCreate()
    output_df = do_user_devices_bits_transformation(spark, spark.table("user_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto("user_devices_bits")