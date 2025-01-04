-- Monthly Reduced fact table

INSERT INTO host_activity_reduced
	
WITH dates AS(
	SELECT *
	
	FROM GENERATE_SERIES ('2023-01-01'::date, '2023-01-31'::date, '1 day'::interval ) AS date
	),

row_numbered_events AS(
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY host, user_id, event_time ORDER BY event_time) AS row_number
	FROM events
	WHERE user_id IS NOT NULL
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

daily_aggregates AS(
	SELECT hd.host,
		DATE(hd.date), TO_CHAR(date, 'Mon') "month",
		COUNT(hd.host) number_of_hits,
		COUNT(DISTINCT e.user_id) number_of_unique_visitors
	
	FROM hosts_and_dates hd
	LEFT JOIN deduped_events e
	ON hd.host = e.host
		AND date(hd.date) = date(e.event_time)

	GROUP BY hd.host, hd.date
)

SELECT "month",
	host,
	ARRAY_AGG(COALESCE(number_of_hits, 0)) hit_array,
	ARRAY_AGG(COALESCE(number_of_unique_visitors, 0)) unique_visitors_array
	
FROM daily_aggregates
GROUP BY "month", host
	
ON CONFLICT (month, host)
DO 
    UPDATE SET hit_array = EXCLUDED.hit_array,
				unique_visitors_array = EXCLUDED.unique_visitors_array;