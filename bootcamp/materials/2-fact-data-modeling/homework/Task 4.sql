-- datelist_int Generation Query

WITH user_devices AS(
	SELECT *
	FROM user_devices_cumulated
	WHERE date = DATE('2023-01-31')
	),
	
series AS(
	SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
	),

starter AS (
	SELECT udc.device_activity_datelist @> ARRAY [DATE(d.series_date)] AS device_is_active,
		date - DATE(series_date) AS days_since,
		udc.user_id,
		udc.device_id,
		udc.browser_type,
		d.series_date

	FROM user_devices udc
	CROSS JOIN series as d
	),
	
bits AS (
	SELECT user_id,
		device_id,
		browser_type,
		SUM(CASE WHEN device_is_active THEN POW(2, 32 - days_since)
			ELSE 0 
			END)::bigint::bit(32) AS datelist_int,
		DATE(series_date) as date
	
	FROM starter
	GROUP BY user_id, device_id, browser_type, DATE(series_date)
     )

SELECT *
FROM bits