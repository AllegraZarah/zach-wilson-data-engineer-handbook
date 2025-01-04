-- Cumulative Query To Generate device_activity_datelist

INSERT INTO user_devices_cumulated
	
WITH dates AS(
	SELECT *
	
	FROM GENERATE_SERIES ('2023-01-01'::date, '2023-01-31'::date, '1 day'::interval ) AS date
	),

row_numbered_events AS(
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY user_id, device_id, date(event_time) ORDER BY event_time) AS row_number
	FROM events
	WHERE user_id IS NOT NULL
	),

deduped_events AS(
	SELECT *
	FROM row_numbered_events
	WHERE row_number = 1
	),

row_numbered_devices AS(
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY device_id, browser_type) AS row_number
	FROM devices
	),

deduped_devices AS(
	SELECT *
	FROM row_numbered_devices
	WHERE row_number = 1
	),

events_start_date AS(
	SELECT user_id, 
		device_id, 
		MIN(event_time) AS first_date

	FROM deduped_events
	GROUP BY user_id, device_id
	),

events_and_dates AS(
	SELECT *

	FROM events_start_date
	JOIN dates
	ON date(events_start_date.first_date) <= date(dates.date)
	),
	
windowed AS(
	SELECT ed.user_id, 
		ed.device_id,
		d.browser_type,
		ARRAY_REMOVE(ARRAY_AGG(
			CASE WHEN e.user_id IS NOT NULL AND e.device_id IS NOT NULL
				THEN date(e.event_time)
			END)
		OVER (PARTITION BY ed.user_id, ed.device_id, browser_type ORDER BY ed.date), NULL ) AS device_activity_datelist,
		date(ed.date)

	FROM events_and_dates ed
	LEFT JOIN deduped_events e
	ON ed.user_id = e.user_id
		AND ed.device_id = e.device_id
		AND date(ed.date) = date(e.event_time)
	
	JOIN deduped_devices d  
	ON ed.device_id = d.device_id
	)

SELECT *
FROM windowed
ORDER BY date desc, user_id, device_id, browser_type
