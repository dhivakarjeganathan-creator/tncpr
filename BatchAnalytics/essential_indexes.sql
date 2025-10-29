-- Essential Database Indexes for Batch Processing Algorithm
-- Minimal set of indexes required for optimal performance

-- ==============================================
-- CRITICAL INDEXES (Required for Core Functionality)
-- ==============================================

-- 1. Enable flag index (most critical - used in main query)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_enable_flag 
ON batch_jobs(enable_flag) 
WHERE enable_flag = true;

-- 2. Event name index (for scheduling logic)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_event_name 
ON batch_jobs(event_name);

-- 3. Foreign key indexes (for joins)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_job_id 
ON batchjob_definitions(job_id);

CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_job_id 
ON batchjob_metrics(job_id);

-- 4. Timestamp index (for time-series queries)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_timestamp 
ON RuleExecutionResults(timestamp);

-- 5. Config name index (for metric filtering)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_config_name 
ON RuleExecutionResults(udc_config_name);

-- ==============================================
-- PERFORMANCE INDEXES (Recommended for Production)
-- ==============================================

-- 6. Composite index for main batch processing query
CREATE INDEX IF NOT EXISTS idx_batch_processing_main 
ON batch_jobs(id, enable_flag, event_name) 
WHERE enable_flag = true;

-- 7. Job delay and granularity filtering
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_processing 
ON batchjob_definitions(job_id, granularity, job_delay);

-- 8. Metric processing optimization
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_processing 
ON batchjob_metrics(job_id, entity, metric_name);

-- 9. Results storage optimization
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_storage 
ON RuleExecutionResults(udc_config_name, timestamp);

-- ==============================================
-- OPTIONAL INDEXES (For Specific Use Cases)
-- ==============================================

-- 10. Job type filtering (if you filter by job types)
CREATE INDEX IF NOT EXISTS idx_batch_jobs_job_type 
ON batch_jobs(job_type);

-- 11. Entity filtering (if you process by entity type)
CREATE INDEX IF NOT EXISTS idx_batchjob_metrics_entity 
ON batchjob_metrics(entity);

-- 12. Focal entity filtering (if you filter by focal entity)
CREATE INDEX IF NOT EXISTS idx_batchjob_definitions_focal_entity 
ON batchjob_definitions(focal_entity);

-- 13. Time-based job management
CREATE INDEX IF NOT EXISTS idx_batch_jobs_create_time 
ON batch_jobs(create_time);

-- 14. Recent results optimization (last 30 days)
CREATE INDEX IF NOT EXISTS idx_rule_execution_results_recent 
ON RuleExecutionResults(timestamp, udc_config_name) 
WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days';

-- ==============================================
-- STATISTICS UPDATE
-- ==============================================

-- Update statistics for better query planning
ANALYZE batch_jobs;
ANALYZE batchjob_definitions;
ANALYZE batchjob_metrics;
ANALYZE RuleExecutionResults;
