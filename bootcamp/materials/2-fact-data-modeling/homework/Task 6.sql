-- Cumulative Query To Generate host_activity_datelist

INSERT INTO host_cumulated
	
WITH dates AS(
	SELECT *
	
	FROM GENERATE_SERIES ('2023-01-01'::date, '2023-01-31'::date, '1 day'::interval ) AS date
	),

row_numbered_events AS(
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY host, date(event_time) ORDER BY event_time) AS row_number
	FROM events
	),

deduped_events AS(
	SELECT *
	FROM row_numbered_events
	WHERE row_number = 1
	),

host_start_date AS(
	SELECT host, 
		MIN(event_time) AS first_date

	FROM deduped_events
	GROUP BY host
	),

hosts_and_dates AS(
	SELECT *

	FROM host_start_date
	JOIN dates
	ON date(host_start_date.first_date) <= date(dates.date)
	),

windowed AS(
	SELECT hd.host,
		ARRAY_REMOVE(ARRAY_AGG(
			CASE WHEN e.host IS NOT NULL
				THEN date(e.event_time)
			END)
		OVER (PARTITION BY hd.host ORDER BY hd.date), NULL ) AS host_activity_datelist,
		date(hd.date)

	FROM hosts_and_dates hd
	LEFT JOIN deduped_events e
	ON hd.host = e.host
		AND date(hd.date) = date(e.event_time)
	)

SELECT *
FROM windowed