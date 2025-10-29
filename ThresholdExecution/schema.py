"""
Database schema definitions for threshold execution system.
"""

def get_create_threshold_queries_table_ddl() -> str:
    """Get DDL for creating the threshold_generated_queries table."""
    return """
    CREATE TABLE IF NOT EXISTS threshold_generated_queries (
        id SERIAL PRIMARY KEY,
        threshold_id VARCHAR(255) NOT NULL,
        tablename VARCHAR(255) NOT NULL,
        metricname VARCHAR(255) NOT NULL,
        mode VARCHAR(50),
        category VARCHAR(100),
        lowerlimit NUMERIC,
        upperlimit NUMERIC,
        occurrence INTEGER,
        clearoccurrence INTEGER,
        cleartime INTEGER,
        time INTEGER,
        periodgranularity INTEGER,
        schedule VARCHAR(255),
        resource TEXT,
        threshold_group VARCHAR(255),
        generated_sql_query TEXT NOT NULL,
        record_count INTEGER DEFAULT 0,
        execution_datetime TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

def get_add_record_count_column_ddl() -> str:
    """Get DDL for adding record_count column to existing table."""
    return """
    ALTER TABLE threshold_generated_queries 
    ADD COLUMN IF NOT EXISTS record_count INTEGER DEFAULT 0;
    """

def get_add_execution_datetime_column_ddl() -> str:
    """Get DDL for adding execution_datetime column to existing table."""
    return """
    ALTER TABLE threshold_generated_queries 
    ADD COLUMN IF NOT EXISTS execution_datetime TIMESTAMP;
    """

def get_create_alarm_table_ddl() -> str:
    """Get DDL for creating the threshold_alarms table."""
    return """
    CREATE TABLE IF NOT EXISTS threshold_alarms (
        id SERIAL PRIMARY KEY,
        alarm_id VARCHAR(255) UNIQUE NOT NULL,
        threshold_id VARCHAR(255) NOT NULL,
        tablename VARCHAR(255) NOT NULL,
        metricname VARCHAR(255) NOT NULL,
        record_id VARCHAR(255) NOT NULL,
        record_timestamp TIMESTAMP NOT NULL,
        metric_value NUMERIC,
        lowerlimit NUMERIC,
        upperlimit NUMERIC,
        occurrence_count INTEGER,
        alarm_severity VARCHAR(50),
        alarm_status VARCHAR(50) DEFAULT 'ACTIVE',
        alarm_message TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

def get_create_alarm_indexes_ddl() -> str:
    """Get DDL for creating indexes on the threshold_alarms table."""
    return """
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_threshold_id 
    ON threshold_alarms(threshold_id);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_tablename 
    ON threshold_alarms(tablename);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_record_id 
    ON threshold_alarms(record_id);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_record_timestamp 
    ON threshold_alarms(record_timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_alarm_status 
    ON threshold_alarms(alarm_status);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_alarms_created_at 
    ON threshold_alarms(created_at);
    """

def get_create_indexes_ddl() -> str:
    """Get DDL for creating indexes on the threshold_generated_queries table."""
    return """
    CREATE INDEX IF NOT EXISTS idx_threshold_generated_queries_threshold_id 
    ON threshold_generated_queries(threshold_id);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_generated_queries_tablename 
    ON threshold_generated_queries(tablename);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_generated_queries_metricname 
    ON threshold_generated_queries(metricname);
    
    CREATE INDEX IF NOT EXISTS idx_threshold_generated_queries_created_at 
    ON threshold_generated_queries(created_at);
    """
