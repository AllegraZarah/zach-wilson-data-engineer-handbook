-- -- Cumulative Query To Generate device_activity_datelist

-- CREATE TABLE pseudo_user_devices_cumulated (
-- 	user_id TEXT,
-- 	device_id TEXT,
-- 	browser_type TEXT,
-- 	device_activity_datelist DATE[],
-- 	date DATE,
-- 	PRIMARY KEY (user_id, device_id, browser_type, date)
-- 	);

	
-- INSERT INTO pseudo_user_devices_cumulated

WITH row_numbered_events AS(
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY user_id, device_id, date(event_time) ORDER BY event_time) AS row_number
	FROM events
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
	
yesterday AS (
	SELECT *
	FROM pseudo_user_devices_cumulated
	WHERE date = DATE('2023-01-09')
	),

today AS (
	SELECT e.user_id::TEXT, 
		e.device_id::TEXT, 
		d.browser_type,
		DATE(e.event_time) today_date
		
	FROM deduped_events e 
	JOIN deduped_devices d  
		ON e.device_id = d.device_id
	
	WHERE DATE(event_time) = DATE('2023-01-10') 
		AND e.user_id IS NOT NULL
	
	GROUP BY e.user_id, e.device_id, browser_type, DATE(e.event_time)
	)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.device_id, y.device_id) AS device_id,
	COALESCE(t.browser_type, y.browser_type) AS device_id,

	CASE WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.today_date]
		WHEN t.today_date IS NULL THEN y.device_activity_datelist
		ELSE y.device_activity_datelist || ARRAY[t.today_date]
	END AS device_activity_datelist,
	
	COALESCE(t.today_date, y.date + Interval '1 day') as date

FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
	AND t.device_id = y.device_id
	AND t.browser_type = y.browser_type

-----
-- SELECT *
-- FROM pseudo_user_devices_cumulated
-- 	where date = '2023-01-10'
-- ORDER BY date desc, user_id, device_id, browser_type

