-- PostgreSQL Database Schema for Batch Analytics
-- This schema is designed to efficiently store and query batch analytics job definitions

-- Main jobs table
CREATE TABLE IF NOT EXISTS batch_jobs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL UNIQUE,
    job_type VARCHAR(50) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    update_time BIGINT NOT NULL,
    create_time BIGINT NOT NULL,
    enable_flag BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Job definitions table
CREATE TABLE IF NOT EXISTS batchjob_definitions (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES batch_jobs(id) ON DELETE CASCADE,
    end_time VARCHAR(50),
    focal_entity VARCHAR(100) NOT NULL,
    focal_type VARCHAR(50) NOT NULL,
    granularity VARCHAR(20) NOT NULL,
    job_delay INTEGER NOT NULL DEFAULT 0,
    job_type VARCHAR(50) NOT NULL DEFAULT 'default',
    percentile DECIMAL(5,2) DEFAULT 0,
    resource_filter TEXT,
    start_time VARCHAR(50),
    time_period VARCHAR(50) NOT NULL,
    timezone VARCHAR(10) NOT NULL DEFAULT 'UTC',
    peak_aggr VARCHAR(50),
    peak_points INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Metrics table for storing individual metrics
CREATE TABLE IF NOT EXISTS batchjob_metrics (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES batch_jobs(id) ON DELETE CASCADE,
    metric_name VARCHAR(255) NOT NULL,
    entity VARCHAR(100) NOT NULL,
    aggregation_types TEXT[] NOT NULL, -- Array of aggregation types
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_batch_jobs_name ON batch_jobs(job_name);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_type ON batch_jobs(job_type);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_type ON batch_jobs(event_type);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enable_flag ON batch_jobs(enable_flag);
CREATE INDEX IF NOT EXISTS idx_batch_jobs_create_time ON batch_jobs(create_time);

CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_id ON batchjob_definitions(job_id);
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_entity ON batchjob_definitions(focal_entity);
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_granularity ON batchjob_definitions(granularity);
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_time_period ON batchjob_definitions(time_period);

CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_id ON batchjob_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_entity ON batchjob_metrics(entity);
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_name ON batchjob_metrics(metric_name);

-- Function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to automatically update updated_at
CREATE TRIGGER update_batch_jobs_updated_at 
    BEFORE UPDATE ON batch_jobs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_batchjob_definitions_updated_at 
    BEFORE UPDATE ON batchjob_definitions 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_batchjob_metrics_updated_at 
    BEFORE UPDATE ON batchjob_metrics 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- View for easy querying of complete job information
CREATE OR REPLACE VIEW job_complete_view AS
SELECT 
    bj.id,
    bj.job_name,
    bj.job_type,
    bj.event_type,
    bj.event_name,
    bj.update_time,
    bj.create_time,
    bj.enable_flag,
    bj.created_at,
    bj.updated_at,
    jd.focal_entity,
    jd.focal_type,
    jd.granularity,
    jd.job_delay,
    jd.job_type as definition_job_type,
    jd.percentile,
    jd.resource_filter,
    jd.start_time,
    jd.end_time,
    jd.time_period,
    jd.timezone,
    jd.peak_aggr,
    jd.peak_points,
    jd.created_at as definition_created_at,
    jd.updated_at as definition_updated_at,
    COALESCE(
        json_agg(
            json_build_object(
                'metric_name', jm.metric_name,
                'entity', jm.entity,
                'aggregation_types', jm.aggregation_types,
                'created_at', jm.created_at,
                'updated_at', jm.updated_at
            )
        ) FILTER (WHERE jm.id IS NOT NULL), 
        '[]'::json
    ) as metrics
FROM batch_jobs bj
LEFT JOIN batchjob_definitions jd ON bj.id = jd.job_id
LEFT JOIN batchjob_metrics jm ON bj.id = jm.job_id
GROUP BY bj.id, bj.job_name, bj.job_type, bj.event_type, bj.event_name, 
         bj.update_time, bj.create_time, bj.enable_flag, bj.created_at, bj.updated_at,
         jd.focal_entity, jd.focal_type, jd.granularity, jd.job_delay, 
         jd.job_type, jd.percentile, jd.resource_filter, jd.start_time, 
         jd.end_time, jd.time_period, jd.timezone, jd.peak_aggr, jd.peak_points,
         jd.created_at, jd.updated_at;
