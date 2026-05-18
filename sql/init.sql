CREATE DATABASE airflow;

\c traffic_congestion;

CREATE TABLE IF NOT EXISTS traffic_incidents (
    incident_id VARCHAR(255), 
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

CREATE INDEX IF NOT EXISTS idx_traffic_event_time ON traffic_incidents(event_time DESC);

CREATE INDEX IF NOT EXISTS idx_traffic_type ON traffic_incidents(incident_type);