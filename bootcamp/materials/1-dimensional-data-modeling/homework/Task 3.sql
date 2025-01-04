CREATE TABLE actors_history_scd (
	actor_id TEXT,
	actor TEXT,
	quality_class quality_class, --Column being tracked
	is_active BOOLEAN, --Second Column being tracked
	streak_identifier INTEGER,
	start_date INTEGER, --SCD tracker
	end_date INTEGER,	--SCD tracker
	current_year INTEGER,
	PRIMARY KEY (actor_id, start_date)
	);