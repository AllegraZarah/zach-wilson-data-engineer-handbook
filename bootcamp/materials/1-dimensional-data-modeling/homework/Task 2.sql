INSERT INTO actors

WITH years AS (
	SELECT *
	
	FROM GENERATE_SERIES(1970, 2021) AS "year"
	),

actors_start_year AS (
    SELECT actor, 
			actorid actor_id,
        	MIN(year) AS first_year
	
    FROM actor_films
    GROUP BY actorid, actor
	),

actors_and_years AS (
    SELECT *
	
    FROM actors_start_year 
    JOIN years
    ON actors_start_year.first_year <= years.year
	),
	
actor_films_avg_rating AS (
	SELECT *,
			ROUND(AVG(rating) OVER (PARTITION BY actorid, year ORDER BY year)::NUMERIC, 1) AS avg_rating
	
	FROM actor_films
	),
	
windowed AS (
    SELECT aas.actor,
			aas.actor_id,
	        aas.year,
			af.avg_rating,
			SUM(CASE WHEN af.avg_rating IS NULL THEN 0 ELSE 1 END) 
				OVER (PARTITION BY aas.actor_id ORDER BY aas.year) avg_rating_identifier,
	        ARRAY_REMOVE(
	            ARRAY_AGG(
	                CASE
	                    WHEN af.year IS NOT NULL
	                        THEN ROW(
								af.year,
								af.film,
								af.votes,
								af.rating,
								af.filmid
	                        )::films
	                END)
	            OVER (PARTITION BY aas.actor_id ORDER BY aas.year), NULL
	        ) AS films_details
	
    FROM actors_and_years aas
    LEFT JOIN actor_films_avg_rating af
        ON aas.actor_id = af.actorid
        AND aas.year = af.year
    ORDER BY aas.actor_id, aas.year
	),
	
avg_rating_for_null_years AS (
	SELECT *,
			CASE 
				WHEN avg_rating IS NOT NULL THEN avg_rating
				ELSE FIRST_VALUE(avg_rating) OVER (PARTITION BY actor_id, avg_rating_identifier ORDER BY year) 
			END AS latest_avg_rating
	
	FROM windowed
	),

final AS (
	SELECT actor,
		    actor_id,
		    films_details AS films,
			 CASE
		        WHEN latest_avg_rating > 8 THEN 'Star'
		        WHEN latest_avg_rating > 7 AND latest_avg_rating <= 8 THEN 'Good'
		        WHEN latest_avg_rating > 6 AND latest_avg_rating <= 7 THEN 'Average'
		        ELSE 'Bad'
		    END::quality_class AS quality_class,
			year,
		    year - (films_details[CARDINALITY(films_details)]::films).year as years_since_last_active,
		    (films_details[CARDINALITY(films_details)]::films).year = year AS is_active
	
	FROM avg_rating_for_null_years
	)
	
SELECT distinct *
FROM final
order by year