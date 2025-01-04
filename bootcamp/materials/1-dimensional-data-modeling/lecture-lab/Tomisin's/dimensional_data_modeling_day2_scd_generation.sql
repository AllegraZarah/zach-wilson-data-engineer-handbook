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

-- INSERT INTO players
-- WITH years AS (
--     SELECT *
--     FROM GENERATE_SERIES(1996, 2022) AS season
-- ), p AS (
--     SELECT
--         player_name,
--         MIN(season) AS first_season
--     FROM player_seasons
--     GROUP BY player_name
-- ), players_and_seasons AS (
--     SELECT *
--     FROM p
--     JOIN years y
--         ON p.first_season <= y.season
-- ), windowed AS (
--     SELECT
--         pas.player_name,
--         pas.season,
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE
--                     WHEN ps.season IS NOT NULL
--                         THEN ROW(
--                             ps.season,
--                             ps.gp,
--                             ps.pts,
--                             ps.reb,
--                             ps.ast
--                         )::season_stats
--                 END)
--             OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--             NULL
--         ) AS seasons
--     FROM players_and_seasons pas
--     LEFT JOIN player_seasons ps
--         ON pas.player_name = ps.player_name
--         AND pas.season = ps.season
--     ORDER BY pas.player_name, pas.season
-- ), static AS (
--     SELECT
--         player_name,
--         MAX(height) AS height,
--         MAX(college) AS college,
--         MAX(country) AS country,
--         MAX(draft_year) AS draft_year,
--         MAX(draft_round) AS draft_round,
--         MAX(draft_number) AS draft_number
--     FROM player_seasons
--     GROUP BY player_name
-- )
-- SELECT
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons AS season_stats,
--     CASE
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'Star'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'Good'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'Average'
--         ELSE 'Bad'
--     END::scoring_class AS scoring_class,
--     w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
--     w.season,
--     (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
-- FROM windowed w
-- JOIN static s
--     ON w.player_name = s.player_name;

------***********************************-----------------------------------------------
---- Actually Starts from here

-- SLOWLY CHANGING DIMESIONS (SCD)
-- TYPE SCD
-- DROP TABLE players_scd;

-- CREATE TABLE players_scd (
-- 	player_name TEXT,
-- 	scoring_class scoring_class, --Column being tracked
-- 	is_active BOOLEAN, --Second Column being tracked
-- 	start_season INTEGER, --SCD tracker
-- 	end_date INTEGER,	--SCD tracker
-- 	current_season INTEGER,
-- 	PRIMARY KEY (player_name, start_season)
-- );

-- Start by Looking at all of history and create on scd record for all of history, then we build on top of it incrementally

-- All of history scd record
INSERT INTO players_scd
WITH with_previous AS(
SELECT 
	player_name,
	current_season,
	scoring_class, 
	is_active,
	LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) prev_scoring_class,
    LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) prev_is_active
FROM players
WHERE current_season <= 2021 -- So anything after 2021 would be incremental
	),

with_indicator AS ( --Indicator to identify when active and not
	SELECT *,
		CASE WHEN scoring_class <> prev_scoring_class THEN 1
			WHEN is_active <> prev_is_active THEN 1
			ELSE 0
		END change_indicator
	FROM with_previous
	),

with_streaks AS (
	SELECT *, SUM(change_indicator) --running total indicator
				OVER (PARTITION BY player_name ORDER BY current_season) streak_identifier
	FROM with_indicator
	)

SELECT  player_name,
            -- streak_identifier,
			scoring_class,
			is_active,			
            MIN(current_season) AS start_season,
            MAX(current_season) AS end_season,
			2021 AS current_season
FROM with_streaks   
     
GROUP BY player_name, streak_identifier, is_active, scoring_class -- Note how expensive group by is cool for building the historic
ORDER BY player_name, streak_identifier



-- Take an existing scd and build on top of it incrementally
CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        );

WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_season = 2021
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_season
        FROM players_scd
        WHERE current_season = 2021
        AND end_season < 2021
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_season

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_season
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) 