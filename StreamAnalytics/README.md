# Streaming Analytics PostgreSQL Loader

This project provides a comprehensive solution for storing and retrieving streaming analytics data in a PostgreSQL database. It includes a well-designed database schema, Python loader script, and query functions for efficient data retrieval.

## Features

- **Normalized Database Schema**: Efficiently stores streaming analytics data with proper relationships
- **Data Loading**: Loads JSON data into PostgreSQL with conflict resolution
- **Query Functions**: Built-in functions for common data retrieval patterns
- **Performance Optimized**: Includes indexes for fast querying
- **Flexible Filtering**: Query by entity, window granularity, aggregation type, and more
- **Environment Configuration**: Uses .env files for secure configuration management
- **Setup Automation**: Automated setup script for easy deployment

## Database Schema

The database schema consists of three main tables:

### 1. `streaming_jobs`
Stores the main job information:
- `id`: Primary key
- `job_name`: Unique job identifier
- `job_type`: Type of job (e.g., "STREAM")
- `event_type`: Event type (e.g., "SUBSCRIBE")
- `update_time`: Last update timestamp
- `create_time`: Creation timestamp
- `enable_flag`: Whether the job is enabled

### 2. `job_definitions`
Stores job definition details:
- `id`: Primary key
- `job_id`: Foreign key to streaming_jobs
- `focal_entity`: The main entity being monitored
- `focal_type`: Type of focal entity
- `resource_filter`: Resource filtering criteria
- `stream_name`: Name of the stream
- `window_gran`: Window granularity (e.g., "1-minute", "5-minute")

### 3. `job_metrics`
Stores individual metrics for each job:
- `id`: Primary key
- `job_definition_id`: Foreign key to job_definitions
- `entity`: Entity associated with the metric
- `metric_name`: Name of the metric
- `aggregation_type`: Type of aggregation (sum, Average, Max, Min)

## Configuration Files

The project uses several configuration files:

### `.env` file
Contains sensitive database configuration:
```env
DB_HOST=localhost
DB_NAME=streaming_analytics
DB_USER=postgres
DB_PASSWORD=your_password
DB_PORT=5432
LOG_LEVEL=INFO
```

### `config.py`
Python configuration module that:
- Loads environment variables from .env file
- Provides configuration validation
- Offers default values for all settings
- Includes utility functions for configuration management

### `setup.py`
Automated setup script that:
- Creates .env file with default values
- Validates configuration
- Checks dependencies
- Provides setup guidance

## Installation

### Quick Installation (Recommended)

1. **Run the automated installer**:
   ```bash
   python install.py
   ```

2. **Set up PostgreSQL database**:
   ```bash
   # Create database
   createdb streaming_analytics
   
   # Run schema creation
   psql -d streaming_analytics -f database_schema.sql
   ```

3. **Update .env file** with your database credentials:
   ```env
   DB_HOST=localhost
   DB_NAME=streaming_analytics
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_PORT=5432
   LOG_LEVEL=INFO
   ```

### Manual Installation

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up configuration**:
   ```bash
   # Run setup script to create .env file
   python setup.py
   
   # Or manually create .env file with your database settings
   ```

3. **Set up PostgreSQL database**:
   ```bash
   # Create database
   createdb streaming_analytics
   
   # Run schema creation
   psql -d streaming_analytics -f database_schema.sql
   ```

### Python Version Compatibility

- **Python 3.8-3.12+**: Fully supported
- **Python 3.12+**: Now fully compatible with automatic compatibility fixes

**For Python 3.12+ users**:
```bash
# The code automatically handles Python 3.12+ compatibility
# No additional setup required beyond normal installation
```

**If you encounter any issues**:
```bash
# Run compatibility check
python python312_compat.py

# Or install compatibility packages manually
pip install setuptools wheel
```

For more troubleshooting help, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## Usage

### Basic Usage

```python
from streaming_analytics_loader import StreamingAnalyticsLoader
from config import Config

# Validate configuration
if not Config.validate_config():
    print("Please check your .env file configuration")
    exit(1)

# Initialize loader (uses config from .env file)
loader = StreamingAnalyticsLoader()

# Connect and load data
loader.connect()
loader.create_tables()
loader.load_all_data('Streaming_analytics.json')
```

### Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run setup
python setup.py

# 3. Update .env file with your database credentials

# 4. Run the loader
python streaming_analytics_loader.py

# 5. Try the example
python example_usage.py
```

### Query Examples

#### Get all jobs
```python
all_jobs = loader.get_all_jobs()
print(f"Total jobs: {len(all_jobs)}")
```

#### Get jobs by entity
```python
gnb_jobs = loader.get_jobs_by_entity('GNB')
print(f"GNB jobs: {len(gnb_jobs)}")
```

#### Get jobs by window granularity
```python
minute_jobs = loader.get_jobs_by_window_granularity('1-minute')
print(f"1-minute window jobs: {len(minute_jobs)}")
```

#### Get metrics for a specific job
```python
metrics = loader.get_metrics_for_job('ACPF.Endc.Drop')
for metric in metrics:
    print(f"Metric: {metric['metric_name']}, Aggregation: {metric['aggregation_type']}")
```

#### Get jobs by aggregation type
```python
sum_jobs = loader.get_jobs_by_aggregation_type('sum')
print(f"Jobs using 'sum' aggregation: {len(sum_jobs)}")
```

#### Get specific job details
```python
job = loader.get_job_by_name('ACPF.Endc.Drop')
if job:
    print(f"Job: {job['job_name']}")
    print(f"Focal Entity: {job['focal_entity']}")
    print(f"Window Granularity: {job['window_gran']}")
```

### Advanced Queries

You can also run custom SQL queries using the database connection:

```python
# Custom query example
with loader.connection.cursor() as cursor:
    cursor.execute("""
        SELECT job_name, focal_entity, window_gran, 
               COUNT(*) as metric_count
        FROM job_metrics_complete 
        WHERE focal_entity = 'GNB'
        GROUP BY job_name, focal_entity, window_gran
        ORDER BY metric_count DESC
    """)
    
    results = cursor.fetchall()
    for row in results:
        print(f"Job: {row[0]}, Metrics: {row[3]}")
```

## Database Views

The schema includes two helpful views for easy querying:

### `job_complete_info`
Combines job and definition information:
```sql
SELECT * FROM job_complete_info WHERE focal_entity = 'GNB';
```

### `job_metrics_complete`
Combines job, definition, and metrics information:
```sql
SELECT * FROM job_metrics_complete WHERE job_name = 'ACPF.Endc.Drop';
```

## Performance Considerations

- **Indexes**: The schema includes indexes on commonly queried fields
- **Normalization**: Data is properly normalized to avoid redundancy
- **Views**: Pre-built views for common query patterns
- **Upsert Logic**: Handles data updates efficiently with ON CONFLICT clauses

## Error Handling

The loader includes comprehensive error handling:
- Database connection errors
- JSON parsing errors
- Data insertion errors
- Query execution errors

All errors are logged with appropriate detail levels.

## Data Types

The schema handles various data types from the JSON:
- **Timestamps**: Stored as BIGINT for millisecond precision
- **Boolean**: ENABLE_FLAG stored as BOOLEAN
- **Text**: Job names, entities, and metrics stored as VARCHAR/TEXT
- **Arrays**: Aggregation types are normalized into separate records

## Maintenance

### Updating Data
The loader handles updates by:
- Using ON CONFLICT for job updates
- Clearing and re-inserting metrics
- Maintaining referential integrity

### Backup and Recovery
```bash
# Backup
pg_dump streaming_analytics > backup.sql

# Restore
psql streaming_analytics < backup.sql
```

## Example Output

After loading the data, you can expect to see:
- 20+ streaming jobs
- Multiple entities (GNB, AUPF, DU, ENB, etc.)
- Various window granularities (1-minute, 5-minute, 15-minute, 1-hour)
- Different aggregation types (sum, Average, Max, Min)

## Troubleshooting

### Common Issues

1. **Connection Errors**: Verify database credentials and network connectivity
2. **Permission Errors**: Ensure the database user has CREATE and INSERT permissions
3. **JSON Parsing Errors**: Verify the JSON file format and encoding
4. **Constraint Violations**: Check for duplicate job names or invalid data

### Debug Mode
Enable debug logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source and available under the MIT License.
