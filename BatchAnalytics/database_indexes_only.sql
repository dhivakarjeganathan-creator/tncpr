-- Database Indexes for Batch Processing Algorithm
-- Only indexes - no table modifications
-- Based on existing schema structure

-- ==============================================
-- BATCH_JOBS TABLE INDEXES
-- ==============================================

-- Enable flag index (most critical for main query)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enable_flag 
ON batch_jobs(enable_flag) 
WHERE enable_flag = true;

-- Event name index (for scheduling logic)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_name 
ON batch_jobs(event_name);

-- Job type index (for job type filtering)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_job_type 
ON batch_jobs(job_type);

-- Event type index (for event type filtering)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_type 
ON batch_jobs(event_type);

-- Time-based indexes for job management
CREATE INDEX IF NOT EXISTS idx_batch_jobs_create_time 
ON batch_jobs(create_time);

CREATE INDEX IF NOT EXISTS idx_batch_jobs_update_time 
ON batch_jobs(update_time);

-- Composite index for main query pattern
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enabled_type_event 
ON batch_jobs(enable_flag, job_type, event_name) 
WHERE enable_flag = true;

-- ==============================================
-- BATCHJOB_DEFINITIONS TABLE INDEXES
-- ==============================================

-- Foreign key index (for joins with batch_jobs)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_id 
ON batchjob_definitions(job_id);

-- Granularity index (for granularity-based filtering)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_granularity 
ON batchjob_definitions(granularity);

-- Job delay index (for time-based data selection)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_delay 
ON batchjob_definitions(job_delay);

-- Focal entity index (for entity-specific processing)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_entity 
ON batchjob_definitions(focal_entity);

-- Focal type index (for entity type filtering)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_type 
ON batchjob_definitions(focal_type);

-- Job type index (for job type filtering within definitions)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_type 
ON batchjob_definitions(job_type);

-- Composite index for job processing
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_processing 
ON batchjob_definitions(job_id, granularity, job_delay);

-- ==============================================
-- BATCHJOB_METRICS TABLE INDEXES
-- ==============================================

-- Foreign key index (for joins with batch_jobs)
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_id 
ON batchjob_metrics(job_id);

-- Metric name index (for metric-specific queries)
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_metric_name 
ON batchjob_metrics(metric_name);

-- Entity index (for table-specific queries)
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_entity 
ON batchjob_metrics(entity);

-- Composite index for metric processing
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_processing 
ON batchjob_metrics(job_id, entity, metric_name);

-- ==============================================
-- RULEEXECUTIONRESULTS TABLE INDEXES
-- ==============================================

-- Timestamp index (critical for time-series queries)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp 
ON RuleExecutionResults(timestamp);

-- Config name index (for metric filtering)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_config_name 
ON RuleExecutionResults(udc_config_name);

-- Composite index for common query pattern
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp_config 
ON RuleExecutionResults(timestamp, udc_config_name);

-- Created at index (for audit queries)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_created_at 
ON RuleExecutionResults(created_at);

-- Updated at index (for change tracking)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_updated_at 
ON RuleExecutionResults(updated_at);

-- ==============================================
-- PERFORMANCE OPTIMIZATION INDEXES
-- ==============================================

-- Partial index for active jobs only
CREATE INDEX IF NOT EXISTS idx_batch_jobs_active 
ON batch_jobs(id, event_name, job_type) 
WHERE enable_flag = true;

-- Partial index for recent results (last 30 days)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_recent 
ON RuleExecutionResults(timestamp, udc_config_name) 
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days';

-- ==============================================
-- UPDATE STATISTICS
-- ==============================================

-- Update table statistics for better query planning
ANALYZE batch_jobs;
ANALYZE batchjob_definitions;
ANALYZE batchjob_metrics;
ANALYZE RuleExecutionResults;
