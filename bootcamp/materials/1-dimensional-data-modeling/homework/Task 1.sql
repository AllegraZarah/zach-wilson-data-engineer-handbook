CREATE TYPE films AS (
	year INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
	);


CREATE TYPE quality_class AS ENUM(
	'Star', 'Good', 'Average', 'Bad'
	);


CREATE TABLE actors (
	actor TEXT,
	actor_id TEXT,
	films films[],
	quality_class quality_class,
	year INTEGER,
	years_since_last_active INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY (actor_id, year)
	)