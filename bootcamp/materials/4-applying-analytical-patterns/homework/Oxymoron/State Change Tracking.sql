-- A query that does state change tracking for players

WITH start_year_identifier AS(
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY player_name ORDER BY current_season) season_hierarchy
	
	FROM players
)

SELECT *,
		CASE WHEN season_hierarchy = 1 THEN 'New'
	
			WHEN season_hierarchy != 1 AND is_active IS false 
				AND LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS true THEN 'Retired'
			
			WHEN season_hierarchy != 1 AND is_active IS false 
				AND LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS false THEN 'Stayed Retired'
			
			WHEN season_hierarchy != 1 AND is_active IS true
				AND LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS false THEN 'Returned from Retirement'
			
			WHEN season_hierarchy != 1 AND is_active IS true
				AND LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS true THEN 'Continued Playing'
	
		ELSE null
		END change_state_tracking
	
FROM start_year_identifier