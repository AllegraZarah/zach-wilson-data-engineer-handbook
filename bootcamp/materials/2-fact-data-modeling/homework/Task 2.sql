-- DDL For user_devices_cumulated

CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	device_id TEXT,
	browser_type TEXT,
	device_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (user_id, device_id, browser_type, date)
	);