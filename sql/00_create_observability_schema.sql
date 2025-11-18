CREATE SCHEMA IF NOT EXISTS observability;

-- Track each complete pipeline run
CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    status VARCHAR,  -- 'running', 'success', 'failed'
    mode VARCHAR,    
    log_file VARCHAR
);

-- Track metrics for each stage within a run
CREATE TABLE IF NOT EXISTS observability.stage_metrics (
    run_id VARCHAR NOT NULL,
    stage_name VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    duration_seconds INTEGER,
    row_count INTEGER DEFAULT 0,
    status VARCHAR,  -- 'success' or 'failed'
    error_message VARCHAR
);

-- Recent activity view
CREATE OR REPLACE VIEW observability.recent_activity AS
SELECT 
    r.run_id,
    r.started_at as run_started,
    r.ended_at as run_ended,
    ROUND(EXTRACT(EPOCH FROM (r.ended_at - r.started_at)) / 60, 1) as total_duration_min,
    r.status as run_status,
    s.stage_name,
    s.duration_seconds as stage_duration_sec,
    s.row_count,
    s.status as stage_status
FROM observability.pipeline_runs r
LEFT JOIN observability.stage_metrics s ON r.run_id = s.run_id
WHERE r.started_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY r.started_at DESC, s.started_at;