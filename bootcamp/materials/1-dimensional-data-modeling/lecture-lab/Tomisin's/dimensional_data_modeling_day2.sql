-- Make adjustments to players table (add is_active column) to move on with today's labs
-- DROP TABLE players;

-- CREATE TABLE players (
-- 			player_name TEXT,
-- 			height TEXT,
-- 			college TEXT,
-- 			country TEXT,
-- 			draft_year TEXT,
-- 			draft_round TEXT,
-- 			draft_number TEXT,
-- 			season_stats season_stats[], --This is the season_stats array
-- 			scoring_class scoring_class,
-- 			years_since_last_season INTEGER,
-- 			current_season INTEGER, --Now this is not a fixed dimension, but it is useful to tell us what period we are currently in
-- 			is_active BOOLEAN,
-- 			PRIMARY KEY(player_name, current_season)
-- 		);

INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'Star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'Good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'Average'
        ELSE 'Bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    w.season,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;

------***********************************-----------------------------------------------
---- Actually Starts from here
CREATE TABLE players_scd (
	player_name TEXT,
	scoring_class scoring_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_date INTEGER,	
	current_season INTEGER,
	PRIMARY KEY (player_name, current_season)
);

-- Start by Looking at all of history and create on scd record for all of history, then we build on top of it incrementally

-- All of history scd





-- SELECT player_name, scoring_class, is_active
-- FROM players
-- 	WHERE current_season = 2022
-- LIMIT 5
