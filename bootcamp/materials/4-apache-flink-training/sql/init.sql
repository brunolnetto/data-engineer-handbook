-- Create processed_events table
CREATE TABLE IF NOT EXISTS processed_events (
    ip VARCHAR,
    event_timestamp TIMESTAMP(3),
    referrer VARCHAR,
    host VARCHAR,
    url VARCHAR,
    geodata VARCHAR
);

CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    event_hour TIMESTAMP(3),
    host VARCHAR,
    num_hits BIGINT
);

CREATE TABLE IF NOT EXISTS sessionized_events (
    session_id VARCHAR,
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    session_duration BIGINT,  -- duration in seconds
    event_count BIGINT,
    urls TEXT[]               -- or VARCHAR[] if your DB supports it, or TEXT if you want to store as a JSON string
);

CREATE TABLE IF NOT EXISTS session_statistics (
    host VARCHAR,
    avg_events_per_session DOUBLE PRECISION,
    total_sessions BIGINT,
    total_events BIGINT,
    avg_session_duration DOUBLE PRECISION
);