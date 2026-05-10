CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.etl_log (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100),
    task_id VARCHAR(100),
    entity_name VARCHAR(100),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_affected INTEGER DEFAULT 0,
    status VARCHAR(20),
    error_message TEXT
);