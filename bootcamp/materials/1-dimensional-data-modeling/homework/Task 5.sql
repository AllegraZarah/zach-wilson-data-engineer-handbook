CREATE TYPE actor_scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
	);


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
        SELECT ts.actor_id, ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_date,
                        ls.end_date
                        )::actor_scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.year,
                        ts.year
                        )::actor_scd_type
                ]) as records
	
        FROM this_year_data ts
        LEFT JOIN last_year_scd ls
        ON ls.actor_id = ts.actor_id
        
		WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
	
unnested_changed_records AS (
	SELECT actor_id, actor,
                (records::actor_scd_type).quality_class,
                (records::actor_scd_type).is_active,
                (records::actor_scd_type).start_date,
                (records::actor_scd_type).end_date
        
		FROM changed_records
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

	SELECT *, 
			2021 AS current_year 
		
	FROM (SELECT * FROM historical_scd
		
			UNION ALL
		
			SELECT * FROM unchanged_records
			
			UNION ALL
		
			SELECT * FROM unnested_changed_records
			
			UNION ALL
		
			SELECT * FROM new_records
	       ) unionized
	)

SELECT *
FROM final