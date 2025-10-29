-- Minimal Essential Indexes for Batch Processing Algorithm
-- Only the indexes absolutely required for core functionality

-- ==============================================
-- ABSOLUTELY REQUIRED INDEXES
-- ==============================================

-- 1. Enable flag index (critical for main query filter)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enable_flag 
ON batch_jobs(enable_flag) 
WHERE enable_flag = true;

-- 2. Event name index (required for scheduling)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_name 
ON batch_jobs(event_name);

-- 3. Foreign key indexes (required for joins)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_id 
ON batchjob_definitions(job_id);

CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_id 
ON batchjob_metrics(job_id);

-- 4. Timestamp index (required for time-series data)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp 
ON RuleExecutionResults(timestamp);

-- 5. Config name index (required for metric queries)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_config_name 
ON RuleExecutionResults(udc_config_name);

-- ==============================================
-- PERFORMANCE CRITICAL INDEXES
-- ==============================================

-- 6. Composite index for main processing query
CREATE INDEX IF NOT EXISTS idx_batch_processing_query 
ON batch_jobs(id, enable_flag, event_name) 
WHERE enable_flag = true;

-- 7. Job processing optimization
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_processing 
ON batchjob_definitions(job_id, granularity, job_delay);

-- 8. Results storage optimization
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_storage 
ON RuleExecutionResults(udc_config_name, timestamp);

-- ==============================================
-- UPDATE STATISTICS
-- ==============================================

ANALYZE batch_jobs;
ANALYZE batchjob_definitions;
ANALYZE batchjob_metrics;
ANALYZE RuleExecutionResults;
