CREATE DATABASE football_db;

CREATE SCHEMA football;

-- Bronze
CREATE TABLE bronze_football_events (
    event_id SERIAL PRIMARY KEY,
    match_id INT,
    event_type VARCHAR,
    player_id INT,
    timestamp TIMESTAMP,
    raw_json JSONB
);

CREATE TABLE silver_football_events (
    event_id INT PRIMARY KEY,
    match_id INT,
    event_type VARCHAR,
    player_id INT,
    timestamp TIMESTAMP

-- Gold
CREATE TABLE gold_football_stats (
    match_id INT PRIMARY KEY,
    total_goals INT,
    total_fouls INT
);
