-- PostgreSQL Database Schema for Streaming Analytics
-- This schema is designed to efficiently store and retrieve streaming analytics data

-- Main jobs table
CREATE TABLE IF NOT EXISTS streaming_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL UNIQUE,
    job_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_name VARCHAR(255),
    update_time BIGINT NOT NULL,
    create_time BIGINT NOT NULL,
    enable_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Job definitions table
CREATE TABLE IF NOT EXISTS job_definitions (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES streaming_jobs(id) ON DELETE CASCADE UNIQUE,
    focal_entity VARCHAR(100) NOT NULL,
    focal_type VARCHAR(100) NOT NULL,
    resource_filter TEXT,
    stream_name VARCHAR(255) NOT NULL,
    window_gran VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Metrics table
CREATE TABLE IF NOT EXISTS job_metrics (
    id SERIAL PRIMARY KEY,
    job_definition_id INTEGER REFERENCES job_definitions(id) ON DELETE CASCADE,
    entity VARCHAR(100) NOT NULL,
    metric_name VARCHAR(255) NOT NULL,
    aggregation_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_jobs_name ON streaming_jobs(job_name);
CREATE INDEX IF NOT EXISTS idx_jobs_type ON streaming_jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_jobs_enabled ON streaming_jobs(enable_flag);
CREATE INDEX IF NOT EXISTS idx_jobs_create_time ON streaming_jobs(create_time);
CREATE INDEX IF NOT EXISTS idx_jobs_update_time ON streaming_jobs(update_time);

CREATE INDEX IF NOT EXISTS idx_definitions_job_id ON job_definitions(job_id);
CREATE INDEX IF NOT EXISTS idx_definitions_focal_entity ON job_definitions(focal_entity);
CREATE INDEX IF NOT EXISTS idx_definitions_stream_name ON job_definitions(stream_name);
CREATE INDEX IF NOT EXISTS idx_definitions_window_gran ON job_definitions(window_gran);

CREATE INDEX IF NOT EXISTS idx_metrics_definition_id ON job_metrics(job_definition_id);
CREATE INDEX IF NOT EXISTS idx_metrics_entity ON job_metrics(entity);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON job_metrics(metric_name);
CREATE INDEX IF NOT EXISTS idx_metrics_aggregation ON job_metrics(aggregation_type);

-- View for easy querying of complete job information
CREATE OR REPLACE VIEW job_complete_info AS
SELECT 
    sj.id as job_id,
    sj.job_name,
    sj.job_type,
    sj.event_type,
    sj.event_name,
    sj.update_time,
    sj.create_time,
    sj.enable_flag,
    jd.focal_entity,
    jd.focal_type,
    jd.resource_filter,
    jd.stream_name,
    jd.window_gran,
    jd.id as definition_id
FROM streaming_jobs sj
LEFT JOIN job_definitions jd ON sj.id = jd.job_id;

-- View for metrics with job information
CREATE OR REPLACE VIEW job_metrics_complete AS
SELECT 
    jci.job_id,
    jci.job_name,
    jci.job_type,
    jci.focal_entity,
    jci.stream_name,
    jci.window_gran,
    jm.entity,
    jm.metric_name,
    jm.aggregation_type
FROM job_complete_info jci
LEFT JOIN job_metrics jm ON jci.definition_id = jm.job_definition_id;
