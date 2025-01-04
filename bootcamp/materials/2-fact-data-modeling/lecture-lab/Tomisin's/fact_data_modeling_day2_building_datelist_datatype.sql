CREATE TABLE users_cumulated (
     user_id TEXT,
     dates_active DATE[],
     date DATE,
     PRIMARY KEY (user_id, date)
 );

INSERT INTO users_cumulated
WITH yesterday AS (
    SELECT * 
	FROM users_cumulated
    WHERE date = DATE('2022-01-09')
	),

today AS (
	SELECT user_id::TEXT,
		DATE(event_time) AS today_date
	
	FROM events
	WHERE DATE(event_time) = DATE('2023-01-10')
			AND user_id IS NOT NULL 
		
	GROUP BY user_id, DATE(event_time)
	)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
-- y.dates_active,
-- t.today_date,
-- ARRAY[t.today_date]::DATE[],
-- CASE WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
--                 	ELSE ARRAY[]::DATE[]
--                 END,
	
-- 	COALESCE(y.dates_active, ARRAY[]::DATE[])
--             || CASE WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
--                 	ELSE ARRAY[]::DATE[]
--                 END AS date_list,

	CASE WHEN y.dates_active IS NULL THEN ARRAY[t.today_date]
		WHEN t.today_date IS NULL THEN y.dates_active
		ELSE y.dates_active || ARRAY[t.today_date]
	END AS dates_active,
	
	COALESCE(t.today_date, y.date + Interval '1 day') as date

FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id

-- SELECT *
-- FROM users_cumulated
-- ORDER BY DATE
-- LIMIT 30

-- -- --------------------------------------------


CREATE TABLE user_datelist_int (
    user_id BIGINT,
    datelist_int BIT(32),
    date DATE,
    PRIMARY KEY (user_id, date)
);

INSERT INTO user_datelist_int
WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-03-31') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-02-28', '2023-03-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-03-31')
	),
bits AS (
         SELECT user_id,
                SUM(CASE
                        WHEN is_active THEN POW(2, 32 - days_since)
                        ELSE 0 END)::bigint::bit(32) AS datelist_int,
                DATE('2023-03-31') as date
         FROM starter
         GROUP BY user_id
     )

SELECT * FROM bits

-- -- --------------------------------------------

-- CREATE TABLE monthly_user_site_hits
-- (
--     user_id          BIGINT,
--     hit_array        BIGINT[],
--     month_start      DATE,
--     first_found_date DATE,
--     date_partition DATE,
--     PRIMARY KEY (user_id, date_partition, month_start)
-- );


-- WITH yesterday AS (
--     SELECT *
--     FROM monthly_user_site_hits
--     WHERE date_partition = '2023-03-02'
-- ),
--      today AS (
--          SELECT user_id,
--                 DATE_TRUNC('day', event_time) AS today_date,
--                 COUNT(1) as num_hits
--          FROM events
--          WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-03')
--          AND user_id IS NOT NULL
--          GROUP BY user_id, DATE_TRUNC('day', event_time)
--      )
-- INSERT INTO monthly_user_site_hits
-- SELECT
--     COALESCE(y.user_id, t.user_id) AS user_id,
--        COALESCE(y.hit_array,
--            array_fill(NULL::BIGINT, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')]))
--         || ARRAY[t.num_hits] AS hits_array,
--     DATE('2023-03-01') as month_start,
--     CASE WHEN y.first_found_date < t.today_date
--         THEN y.first_found_date
--         ELSE t.today_date
--             END as first_found_date,
--     DATE('2023-03-03') AS date_partition
--     FROM yesterday y
--     FULL OUTER JOIN today t
--         ON y.user_id = t.user_id

-- -- --------------------------------------------