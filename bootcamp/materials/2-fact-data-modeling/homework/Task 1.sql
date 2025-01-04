-- Deduplicate Game Details

WITH deduped AS (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS row_number

	FROM game_details
)

SELECT *
FROM deduped
WHERE row_number = 1