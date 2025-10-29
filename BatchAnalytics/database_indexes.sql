-- Database Indexes for Batch Processing Algorithm
-- Optimized for performance based on query patterns

-- ==============================================
-- BATCH_JOBS TABLE INDEXES
-- ==============================================

-- Primary query filter: enabled jobs
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enable_flag 
ON batch_jobs(enable_flag) 
WHERE enable_flag = true;

-- Event-based scheduling queries
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_name 
ON batch_jobs(event_name);

-- Job type filtering for different job categories
CREATE INDEX IF NOT EXISTS idx_batch_jobs_job_type 
ON batch_jobs(job_type);

-- Event type filtering for scheduled vs triggered jobs
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_type 
ON batch_jobs(event_type);

-- Time-based queries for job management
CREATE INDEX IF NOT EXISTS idx_batch_jobs_create_time 
ON batch_jobs(create_time);

CREATE INDEX IF NOT EXISTS idx_batch_jobs_update_time 
ON batch_jobs(update_time);

-- Composite index for common query pattern: enabled jobs by type and event
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enabled_type_event 
ON batch_jobs(enable_flag, job_type, event_name) 
WHERE enable_flag = true;

-- ==============================================
-- BATCHJOB_DEFINITIONS TABLE INDEXES
-- ==============================================

-- Foreign key relationship (most common join)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_id 
ON batchjob_definitions(job_id);

-- Granularity-based filtering for job processing
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_granularity 
ON batchjob_definitions(granularity);

-- Job delay filtering for time-based data selection
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_delay 
ON batchjob_definitions(job_delay);

-- Focal entity filtering for entity-specific processing
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_entity 
ON batchjob_definitions(focal_entity);

-- Focal type filtering for different entity types
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_type 
ON batchjob_definitions(focal_type);

-- Job type filtering within definitions
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_type 
ON batchjob_definitions(job_type);

-- Composite index for common query: job_id + granularity + job_delay
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_granularity_delay 
ON batchjob_definitions(job_id, granularity, job_delay);

-- ==============================================
-- BATCHJOB_METRICS TABLE INDEXES
-- ==============================================

-- Foreign key relationship (most common join)
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_id 
ON batchjob_metrics(job_id);

-- Metric name filtering for specific metrics
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_metric_name 
ON batchjob_metrics(metric_name);

-- Entity filtering for table-specific queries
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_entity 
ON batchjob_metrics(entity);

-- Composite index for common query: job_id + metric_name
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_metric 
ON batchjob_metrics(job_id, metric_name);

-- Composite index for entity-based queries
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_entity_metric 
ON batchjob_metrics(entity, metric_name);

-- ==============================================
-- RULEEXECUTIONRESULTS TABLE INDEXES
-- ==============================================

-- Timestamp-based queries (most common for time-series data)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp 
ON RuleExecutionResults(timestamp);

-- Config name filtering for specific metrics
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_config_name 
ON RuleExecutionResults(udc_config_name);

-- Composite index for common query: timestamp + config_name
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp_config 
ON RuleExecutionResults(timestamp, udc_config_name);

-- Time range queries with config filtering
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_config_timestamp 
ON RuleExecutionResults(udc_config_name, timestamp);

-- Created at timestamp for audit queries
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_created_at 
ON RuleExecutionResults(created_at);

-- Updated at timestamp for change tracking
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_updated_at 
ON RuleExecutionResults(updated_at);

-- ==============================================
-- PERFORMANCE OPTIMIZATION INDEXES
-- ==============================================

-- Partial indexes for active jobs only (saves space and improves performance)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_active_jobs 
ON batch_jobs(id, event_name, job_type) 
WHERE enable_flag = true;

-- Partial index for recent results (last 30 days)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_recent 
ON RuleExecutionResults(timestamp, udc_config_name, udc_config_value) 
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days';

-- ==============================================
-- QUERY-SPECIFIC COMPOSITE INDEXES
-- ==============================================

-- Main batch processing query optimization
-- Covers: enabled jobs -> definitions -> metrics join
CREATE INDEX IF NOT EXISTS idx_batch_processing_main_query 
ON batch_jobs(id, enable_flag, event_name) 
WHERE enable_flag = true;

-- Job delay filtering optimization
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_delay_granularity 
ON batchjob_definitions(job_delay, granularity, job_id);

-- Metric processing optimization
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_processing 
ON batchjob_metrics(job_id, entity, metric_name, aggregation_types);

-- Results storage optimization
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_storage 
ON RuleExecutionResults(udc_config_name, timestamp, udc_config_value);

-- ==============================================
-- MAINTENANCE INDEXES
-- ==============================================

-- For cleanup operations on old data
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_cleanup 
ON RuleExecutionResults(created_at) 
WHERE created_at < CURRENT_DATE - INTERVAL '90 days';

-- For job history tracking
CREATE INDEX IF NOT EXISTS idx_batch_jobs_history 
ON batch_jobs(created_at, updated_at, job_type);

-- ==============================================
-- STATISTICS UPDATE
-- ==============================================

-- Update table statistics for better query planning
ANALYZE batch_jobs;
ANALYZE batchjob_definitions;
ANALYZE batchjob_metrics;
ANALYZE RuleExecutionResults;
