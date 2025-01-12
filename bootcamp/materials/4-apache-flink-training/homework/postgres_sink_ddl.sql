-- -- Create processed_events table
CREATE TABLE IF NOT EXISTS sessionized_events (
    host VARCHAR,
    ip VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    event_count BIGINT
    )