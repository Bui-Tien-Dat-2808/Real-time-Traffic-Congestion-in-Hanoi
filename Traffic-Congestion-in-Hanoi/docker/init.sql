CREATE TABLE IF NOT EXISTS traffic_incidents (
    incident_id VARCHAR(255) PRIMARY KEY,
    incident_type INTEGER,
    magnitude INTEGER,
    delay_seconds INTEGER,
    length_km DOUBLE PRECISION,
    road_from VARCHAR(255),
    road_to VARCHAR(255),
    description TEXT,
    start_coordinates VARCHAR(255),
    event_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS incident_summary_1min (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    total_incidents BIGINT,
    avg_delay_seconds DOUBLE PRECISION,
    max_delay_seconds INTEGER
);

CREATE INDEX IF NOT EXISTS idx_traffic_incidents_event_time
ON traffic_incidents (event_time);

CREATE INDEX IF NOT EXISTS idx_incident_summary_window_end
ON incident_summary_1min (window_end);
