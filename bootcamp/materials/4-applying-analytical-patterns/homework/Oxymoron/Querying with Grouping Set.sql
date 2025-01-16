-- A query that uses GROUPING SETS to do efficient aggregations of game_details data

-- Query
WITH 
game_details AS(
	SELECT g_d.game_id, g_d.player_id, g_d.player_name, 
			g_d.team_id, g_.home_team_id, g_.visitor_team_id, 
			g_d.team_abbreviation, g_.season, g_.home_team_wins,
	
			CASE WHEN g_d.team_id = g_.home_team_id THEN g_.home_team_wins::BOOLEAN
				WHEN g_d.team_id = g_.visitor_team_id THEN NOT g_.home_team_wins::BOOLEAN
			ELSE null
			END game_wins,
	
			g_d.pts
				
	
	FROM game_details g_d
	JOIN games g_
	ON g_d.game_id = g_.game_id
),

team_wins AS (
    SELECT DISTINCT
        game_id,
        team_id,
        team_abbreviation,
        season,
        game_wins
    FROM game_details
),
	
groupings AS(
	SELECT 
		CASE WHEN GROUPING(player_id, team_id) = 0 THEN 'player_id__team_id'
			WHEN GROUPING(player_id, season) = 0 THEN 'player_id__season'
			WHEN GROUPING(team_id) = 0 THEN 'team_id'
		END AS aggregation_level,
		
		COALESCE(season::TEXT, '(Overall)') season,
		COALESCE(team_id::TEXT, '(Overall)') team_id,
		COALESCE(team_abbreviation, '(Overall)') team_abbreviation,
		COALESCE(player_id::TEXT, '(Overall)') player_id,
		COALESCE(player_name, '(Overall)') player_name,
	
		SUM(pts) game_points,
	
		CASE WHEN GROUPING(player_id) = 1 
					AND GROUPING(season) = 1			
					AND GROUPING(team_id) = 0 
				THEN (SELECT SUM(game_wins::INTEGER) FROM team_wins tw WHERE tw.team_id = gd.team_id)
		ELSE NULL
	    END as games_won
	
	FROM game_details gd
	
	GROUP BY GROUPING SETS ((player_id, player_name, team_id, team_abbreviation),
							(player_id, player_name, season),
							(team_id, team_abbreviation))
)

SELECT *
FROM groupings


-- Answers to Questions:
-- (1) Who scored the most points playing for one team?
-- ANSWER: Giannis Antetokounmpo (Player ID: 203507)
-- FINAL SELECT STATEMENT: SELECT * FROM groupings WHERE aggregation_level = 'player_id__team_id' AND game_points IS NOT NULL ORDER BY game_points DESC


-- (2) Who scored the most points in one season?
-- ANSWER: James Harden (Player ID: 201935)
-- FINAL SELECT STATEMENT: SELECT * FROM groupings WHERE aggregation_level = 'player_id__season' AND game_points IS NOT NULL ORDER BY game_points DESC


-- (3) Which team has won the most games?
-- ANSWER: Team GSW (Team ID: 1610612744), Won 445 Games
-- FINAL SELECT STATEMENT: SELECT * FROM groupings WHERE aggregation_level = 'team_id' ORDER BY games_won DESC