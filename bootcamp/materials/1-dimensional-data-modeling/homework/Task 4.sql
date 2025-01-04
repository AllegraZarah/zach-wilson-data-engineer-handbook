INSERT INTO actors_history_scd

WITH with_previous AS(
	SELECT 
		actor_id,
		actor,
		year,
		quality_class, 
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY year) prev_quality_class,
	    LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY year) prev_is_active
	
	FROM actors
	WHERE year <= 2020 -- So anything after 2020 would be incremental
	),

with_indicator AS (
	SELECT *,
		CASE WHEN quality_class <> prev_quality_class THEN 1
			WHEN is_active <> prev_is_active THEN 1
			ELSE 0
		END change_indicator --we want this indicator to change whether there is a change in quality_class or is_active
	FROM with_previous
	),

with_streaks AS (
	SELECT *, 
		SUM(change_indicator) OVER (PARTITION BY actor_id ORDER BY year) streak_identifier -- running total indicator of no change
	FROM with_indicator
	)
	

SELECT actor_id,
		actor,
		quality_class,
		is_active,
		streak_identifier,
		MIN(year) start_date,
		MAX(year) end_date,
		2020 current_year
	
FROM with_streaks
GROUP BY actor_id, actor, streak_identifier, quality_class, is_active -- Note how expensive group by is, but it is cool for building the historic
ORDER BY actor_id, streak_identifier