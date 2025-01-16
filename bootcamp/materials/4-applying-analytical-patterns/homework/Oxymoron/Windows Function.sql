-- A query that uses window functions on game_details to find out the following things:

-- What is the most games a team has won in a 90 game stretch?
WITH 
game_details AS(
	SELECT g_d.game_id, g_d.player_id, g_d.player_name, 
			g_d.team_id, g_.home_team_id, g_.visitor_team_id, 
			g_d.team_abbreviation, g_.season, g_.home_team_wins,
	
			CASE WHEN g_d.team_id = g_.home_team_id THEN g_.home_team_wins::BOOLEAN
				WHEN g_d.team_id = g_.visitor_team_id THEN NOT g_.home_team_wins::BOOLEAN
			ELSE null
			END is_game_won,
	
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
        is_game_won
    FROM game_details
),

games_won_per_plays AS(
	SELECT team_id, team_abbreviation, game_id, is_game_won,
		ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY game_id) game_stretch,	
		SUM(CASE WHEN is_game_won IS TRUE THEN 1 ELSE 0 END) OVER(PARTITION BY team_id ORDER BY game_id) games_won
	
	FROM team_wins
	)

SELECT *
FROM games_won_per_plays
WHERE game_stretch = 90
ORDER BY games_won DESC;

--  ANSWER: 68 games won by GSW with Team ID 1610612744



-- How many games in a row did LeBron James score over 10 points a game?
WITH 
game_details AS(
	SELECT g_d.game_id, g_d.player_id, g_d.player_name, 
			g_d.team_id, g_.home_team_id, g_.visitor_team_id, 
			g_d.team_abbreviation, g_.season, g_.home_team_wins,	
			g_d.pts,

			CASE WHEN pts > 10 AND (LAG(pts) OVER(PARTITION BY player_id ORDER BY g_d.game_id) IS NULL 
									OR
									LAG(pts) OVER(PARTITION BY player_id ORDER BY g_d.game_id) <= 10) THEN 1
				WHEN pts > 10 AND LAG(pts) OVER(PARTITION BY player_id ORDER BY g_d.game_id) IS NOT NULL THEN 0
				ELSE 0
			END AS new_streak_start
	
	FROM game_details g_d
	JOIN games g_
	ON g_d.game_id = g_.game_id

	WHERE player_name = 'LeBron James'
),

streak_group AS (
    SELECT *,
        	SUM(new_streak_start) OVER(PARTITION BY player_id ORDER BY game_id) as streak_group
    
	FROM game_details
)

SELECT streak_group,
		COUNT(1) streak_length
	
FROM streak_group
WHERE pts > 10
GROUP BY streak_group

ORDER BY streak_length DESC

--  ANSWER: 163 games in a row with over 10 points per game was the longest streak by LeBron James