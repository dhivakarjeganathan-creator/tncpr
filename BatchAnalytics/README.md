# Batch Processing Algorithm

A scalable batch processing system for analytics data aggregation based on configurable schedules and granularities.

## Overview

This system implements the requirements from `batchanalreq.txt` to process batch jobs with the following key features:

- **Configurable Scheduling**: Jobs run based on event names that map to cron expressions
- **Flexible Granularity**: Support for hour, day, week, and custom time-based aggregations
- **Multiple Aggregation Types**: SUM, AVG, COUNT, MIN, MAX, and more
- **Job Delay Filtering**: Process data based on time delays (e.g., process data from 2 hours ago)
- **Scalable Architecture**: Threaded processing with configurable concurrency limits

## Architecture

### Core Components

1. **BatchProcessor**: Main orchestrator that manages job execution
2. **DatabaseManager**: Handles all database operations
3. **GranularityProcessor**: Manages time-based data aggregation
4. **CronScheduler**: Converts event names to cron schedules
5. **BatchProcessingService**: High-level service wrapper

### Database Schema

The system uses the following tables:
- `batch_jobs`: Job definitions and scheduling
- `batchjob_definitions`: Job configuration details
- `batchjob_metrics`: Metrics to process and aggregation types
- `metricsandtables`: Mapping between metrics and source tables
- `RuleExecutionResults`: Storage for aggregated results

## Usage

### Basic Setup

```python
from batch_processor import BatchProcessingService

# Initialize the service (reads from .env file)
service = BatchProcessingService()

# Start the service (runs in background)
service.start()

# Or run jobs immediately (for testing)
service.run_immediate()
```

### Environment Configuration

Create a `.env` file in your project root:

```bash
# Database Configuration
DB_HOST=localhost
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=test@1234
DB_PORT=5432
DB_MIN_CONNECTIONS=1
DB_MAX_CONNECTIONS=10

# Logging Configuration
LOG_LEVEL=INFO

# Batch Processing Configuration
MAX_CONCURRENT_JOBS=10
BATCH_SIZE=1000
JOB_TIMEOUT=3600
```

### Alternative: Direct Database Parameters

```python
# You can pass database parameters directly
service = BatchProcessingService(
    host='localhost',
    database='hierarchy_db',
    user='postgres',
    password='test@1234',
    port=5432,
    min_connections=1,
    max_connections=10
)
```

### Configuration

The system supports various configuration options through environment variables:

```bash
# Database
export DB_HOST="localhost"
export DB_NAME="hierarchy_db"
export DB_USER="postgres"
export DB_PASSWORD="test@1234"
export DB_PORT="5432"
export DB_MIN_CONNECTIONS=1
export DB_MAX_CONNECTIONS=10

# Performance
export MAX_CONCURRENT_JOBS=10
export BATCH_SIZE=1000
export JOB_TIMEOUT=3600

# Logging
export LOG_LEVEL=INFO
```

### Event Name Mapping

The system maps event names to cron expressions:

| Event Name | Cron Expression | Description |
|------------|----------------|-------------|
| `EVERYHOUR` | `0 * * * *` | Every hour |
| `EVERYDAY8AMET` | `0 8 * * *` | Every day at 8 AM ET |
| `EVERYWEEK` | `0 0 * * 0` | Every week (Sunday) |
| `EVERYHOURBYMIN10` | `10 * * * *` | Every 10th minute |

### Granularity Support

Supported granularities:
- `1-hour`, `2-hour`, `4-hour`, `6-hour`, `12-hour`
- `1-day`, `2-day`, `3-day`
- `1-week`, `2-week`

### Aggregation Types

Supported aggregation types:
- `sum`: Sum of values
- `avg`: Average of values
- `count`: Count of records
- `min`: Minimum value
- `max`: Maximum value
- `median`: Median value
- `std`: Standard deviation
- `var`: Variance

## Key Features

### 1. Flexible Data Retrieval

The system handles two types of data storage:
- **Row-based**: For `RuleExecutionResults` table (metrics stored as rows)
- **Column-based**: For other tables (metrics stored as columns)

### 2. Job Delay Filtering

Jobs can be configured with delays:
- `0`: Process all data
- `1`: Process data from 1 hour ago
- `2`: Process data from 2 hours ago
- `4`: Process data from 4 hours ago

### 3. Scalable Processing

- Threaded execution for concurrent job processing
- Configurable batch sizes and timeouts
- Error handling and retry mechanisms
- Performance monitoring and logging

### 4. Real-time Scheduling

- Jobs run based on their configured schedules
- Automatic detection of when jobs should execute
- No delays in job execution
- Efficient resource utilization

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up the database schema:
```bash
psql -d your_database -f database_schema.sql
```

3. Configure environment variables:
```bash
export DATABASE_URL="your_database_connection_string"
```

4. Run the service:
```bash
python batch_processor.py
```

## Monitoring

The system provides comprehensive logging:
- Job execution status
- Performance metrics
- Error tracking
- Aggregation results

Logs are written to both console and file (`batch_processor.log`).

## Example Job Configuration

```sql
-- Create a job that runs every hour
INSERT INTO batch_jobs (event_name, enable_flag) VALUES ('EVERYHOUR', TRUE);

-- Define the job configuration
INSERT INTO batchjob_definitions (job_id, focal_entity, entity, granularity, job_delay, resource_filter) 
VALUES (1, 'user', 'user_activity', '1-hour', 0, 'status = "active"');

-- Add metrics to process
INSERT INTO batchjob_metrics (job_id, metric_name, aggregation_types) 
VALUES (1, 'page_views', 'sum,avg,count');

-- Map the metric to a table
INSERT INTO metricsandtables (metricname, tablename) 
VALUES ('page_views', 'user_activity');
```

## Performance Considerations

- **Batch Size**: Configure appropriate batch sizes for your data volume
- **Concurrency**: Set `MAX_CONCURRENT_JOBS` based on your system capacity
- **Timeouts**: Configure `JOB_TIMEOUT` to prevent long-running jobs
- **Indexing**: Ensure proper database indexes for performance
- **Monitoring**: Use logging to monitor job performance and identify bottlenecks

## Error Handling

The system includes comprehensive error handling:
- Database connection failures
- Job execution errors
- Data processing errors
- Scheduling conflicts
- Resource exhaustion

All errors are logged with appropriate context for debugging.