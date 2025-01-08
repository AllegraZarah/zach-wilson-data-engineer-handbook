from pyspark.sql import SparkSession

query = """

        WITH last_year_scd AS (
            SELECT *
            FROM actors_history_scd
            WHERE current_year = 2020 AND end_date = 2020
        ),

        historical_scd AS (
            SELECT actor_id,
                actor,
                quality_class,
                is_active,
                start_date,
                end_date
            FROM actors_history_scd
            WHERE current_year = 2020 AND end_date < 2020
        ),

        this_year_data AS (
            SELECT *
            FROM actors
            WHERE year = 2021
        ),

        unchanged_records AS (
            SELECT ts.actor_id,
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_date,
                ts.year as end_date
            FROM this_year_data ts
            LEFT JOIN last_year_scd ls 
            ON ts.actor_id = ls.actor_id

            WHERE ts.quality_class = ls.quality_class
            AND ts.is_active = ls.is_active
        ),

        changed_records AS (
        SELECT 
            ls.actor_id,
            ls.actor,
            ls.quality_class as old_quality_class,
            ls.is_active as old_is_active,
            ls.start_date as old_start_date,
            ls.end_date as old_end_date
        FROM last_year_scd ls
        INNER JOIN this_year_data ts ON ls.actor_id = ts.actor_id
        WHERE (ts.quality_class <> ls.quality_class OR ts.is_active <> ls.is_active)
        
        UNION ALL
        
        SELECT 
            ts.actor_id,
            ts.actor,
            ts.quality_class as old_quality_class,
            ts.is_active as old_is_active,
            ts.year as old_start_date,
            ts.year as old_end_date
        FROM this_year_data ts
        INNER JOIN last_year_scd ls ON ls.actor_id = ts.actor_id
        WHERE (ts.quality_class <> ls.quality_class OR ts.is_active <> ls.is_active)
        ),


        new_records AS (
            SELECT ts.actor_id,
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.year AS start_date,
                ts.year AS end_date
            FROM this_year_data ts
            LEFT JOIN last_year_scd ls
            ON ts.actor_id = ls.actor_id
            
            WHERE ls.actor_id IS NULL
        ),


        final AS (
            SELECT *, CAST(2021 AS INT) AS current_year
            FROM (
                SELECT * FROM historical_scd

                UNION ALL

                SELECT * FROM unchanged_records
                
                UNION ALL
                
                SELECT 
                    actor_id,
                    actor,
                    old_quality_class as quality_class,
                    old_is_active as is_active,
                    old_start_date as start_date,
                    old_end_date as end_date
                FROM changed_records
                
                UNION ALL
                
                SELECT * FROM new_records
            ) unionized
        )

        SELECT * FROM final
"""

def do_incremental_update_actors_scd_transformation(spark, dataframe1, dataframe2):
    dataframe1.createOrReplaceTempView("actors_history_scd")
    dataframe2.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd") \
        .getOrCreate()
    
    output_df = do_incremental_update_actors_scd_transformation(spark, spark.table("actors_history_scd"), spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")
