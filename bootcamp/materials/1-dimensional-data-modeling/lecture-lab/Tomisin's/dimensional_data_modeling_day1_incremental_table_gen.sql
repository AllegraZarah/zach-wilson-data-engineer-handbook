-- Dimensional Modeling
			
-- select *
-- from public.player_seasons
-- limit 5

-- Create a table that contains one row per player and an array of their seasons
-- On querying, we they break that array into fields

-- CREATE A STRUCT ()
-- CREATE TYPE season_stats AS ( -- I am thinking TYPE is what a struct is, because the season stat struct is creted under TYPES in the schema 
-- 			season INTEGER,
-- 			gp INTEGER,
-- 			pts REAL,
-- 			reb REAL,
-- 			ast REAL
-- 		)

-- CREATE A CATEGORICAL COLUMN
-- CREATE TYPE scoring_class AS ENUM(
-- 	'Star', 'Good', 'Average', 'Bad' 
-- )

-- Create a table whose column are basically the table object (players name in this case). Notice how these are fixed dimension and not changing like age for example

-- DROP TABLE players;

CREATE TABLE players (
			player_name TEXT,
			height TEXT,
			college TEXT,
			country TEXT,
			draft_year TEXT,
			draft_round TEXT,
			draft_number TEXT,
			season_stats season_stats[], --This is the season_stats array
			scoring_class scoring_class,
			years_since_last_season INTEGER,
			current_season INTEGER, --Now this is not a fixed dimension, but it is useful to tell us what period we are currently in
			PRIMARY KEY(player_name, current_season)
		);

-- Note the first year of this player_seasons is 1996
-- We need to create a previous period cte to join with the curent period
-- This is essentially the seed query

INSERT INTO players
	
WITH 
yesterday AS (
	SELECT * FROM players
	WHERE current_season = 2000
	),

today AS (
	SELECT * FROM player_seasons
	WHERE season = 2001
	)

SELECT 
	-- coalesce the fixed dim values to ensure previous period values are not nulls
	COALESCE (t.player_name, y.player_name) AS player_name,
	COALESCE (t.height, y.height) AS height,
	COALESCE (t.college, y.college) AS college,
	COALESCE (t.country, y.country) AS country,
	COALESCE (t.draft_year, y.draft_year) AS draft_year,
	COALESCE (t.draft_round, y.draft_round) AS draft_round,
	COALESCE (t.draft_number, y.draft_number) AS draft_number,
	-- Add in the seasons array
	CASE WHEN y.season_stats IS NULL THEN ARRAY[ROW(t.season,
													t.gp,
													t.pts,
													t.reb,
													t.ast
													)::season_stats]
		WHEN t.season IS NOT NULL THEN y.season_stats || ARRAY[ROW(t.season,
																	t.gp,
																	t.pts,
																	t.reb,
																	t.ast
																	)::season_stats]
	-- we do not want to be adding to a player's record when the player is retired for example. Hence:
	ELSE y.season_stats --carry history forward
	END AS season_stats,

	CASE WHEN t.season IS NOT NULL THEN
		CASE WHEN t.pts > 20 THEN 'Star'
			WHEN t.pts > 15 THEN 'Good'
			WHEN t.pts > 10 THEN 'Average'
		ELSE 'Bad'
		END :: scoring_class
	ELSE y.scoring_class
	END AS scoring_class,
	
	CASE WHEN t.season IS NOT NULL THEN 0
	ELSE y.years_since_last_season+ 1
	END as years_since_last_season,

	COALESCE(t.season, y.current_season + 1) as curent_season
	
FROM today t 
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name

	--------------------------------------------


-- What if I want to get the player seasons from the player table
-- A great thing about this is that when the unnesting is done, it is in an ordered manner, well sorted
	
-- WITH 
-- unnested AS (
-- 	SELECT player_name,
-- 			UNNEST(season_stats) season_stats
		
-- 	FROM public.players
-- 	WHERE current_Season = 2001 
-- 		-- AND player_name = 'Michael Jordan'
-- 	ORDER BY player_name ASC, current_season ASC 
-- 	)

-- SELECT player_name,
-- 		(season_stats::season_stats).*
-- 		-- (season_stats::season_stats).pts --remember that array that we created at some point is the beginnig, each field name can be called here. Or after doing the .*, focus on only the ones that matter to you
-- FROM unnested

-- We want to kinda see the points of players from their first season to like the latest
SELECT player_name,
	season_stats,
	season_stats[1] AS first_season, --this will return an array, to get the points only,
	(season_stats[1]:: season_stats).pts AS first_season_value_only,
	season_stats[CARDINALITY(season_stats)] AS latest_season, -- I believe CARDINALITY is like latest. This is great, because the number of elements in the arrays in the field are not fixed. Hence it would be different for all, cardinality could be game changer for me
	(season_stats[CARDINALITY(season_stats)]:: season_stats).pts AS latest_season_value_only
	
FROM players
WHERE current_season = 2001
LIMIT 7

-- Assuming we want to see how much the player has improved
SELECT 
	player_name,
	(season_stats[CARDINALITY(season_stats)]:: season_stats).pts /
	CASE WHEN (season_stats[1]::season_stats).pts = 0 
		THEN 1 
	ELSE (season_stats[1]::season_stats).pts 
	END
FROM players
	WHERE current_season = 2001
	ORDER BY 2
	LIMIT 50

-- SELECT *
-- FROM players
-- WHERE current_season = 2001
-- LIMIT 5

-- iNCREMENTALLY build up history
-- Access to historicals
-- Queries are incredibly fast, as no group by is required