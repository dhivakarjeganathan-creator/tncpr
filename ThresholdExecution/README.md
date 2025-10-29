# Threshold Execution System

This system processes threshold rules and generates SQL queries for monitoring and alerting based on the requirements specified in `Executionrequirements.txt`.

## Features

- Processes threshold rules from database tables
- Generates SQL queries for each threshold job
- Handles special cases for `RuleExecutionResults` table
- Generates simple threshold queries with basic conditions
- Stores generated queries in a dedicated table
- Configurable through environment variables

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database

Edit `config.env` file with your database configuration:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Threshold Configuration
THRESHOLD_QUERY_TABLE=threshold_rules
METRICS_TABLE=metricsandtables
GENERATED_QUERIES_TABLE=threshold_generated_queries

# Application Configuration
LOG_LEVEL=INFO
BATCH_SIZE=100
```

### 3. Database Schema

The system expects the following tables to exist:

- `threshold_rules` - Contains threshold rule definitions
- `metricsandtables` - Maps metrics to table names
- `RuleExecutionResults` - Special table for rule execution results

The system will automatically create the `threshold_generated_queries` table to store generated SQL queries.

## Usage

### Run the System

```bash
python main.py
```

### What the System Does

1. **Retrieves Threshold Jobs**: Executes the predefined threshold query to get all active threshold rules
2. **Generates SQL Queries**: For each threshold job, creates a SQL query that:
   - Retrieves metric data from the specified table
   - Applies lower and upper limit conditions
   - Special handling for `RuleExecutionResults` table
3. **Stores Results**: Saves all generated queries to the `threshold_generated_queries` table
4. **Displays Output**: Shows all generated queries in the console

### Generated Query Structure

For each threshold job, the system generates a query that:

```sql
SELECT id, timestamp, metric_value
FROM table_name
WHERE 1=1
AND (metric_value >= lower_limit AND metric_value <= upper_limit)
ORDER BY id, timestamp ASC
```

### Special Cases

- **RuleExecutionResults Table**: Uses `udc_config_name` as metric name and `udc_config_value` as metric value

## File Structure

```
├── main.py                    # Main execution script
├── database.py               # Database connection and utilities
├── threshold_processor.py    # Threshold processing logic
├── schema.py                 # Database schema definitions
├── config.env               # Configuration file
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Configuration Options

- `DB_HOST`: Database host
- `DB_PORT`: Database port
- `DB_NAME`: Database name
- `DB_USER`: Database username
- `DB_PASSWORD`: Database password
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `BATCH_SIZE`: Batch size for processing

## Error Handling

The system includes comprehensive error handling:
- Database connection errors
- Query execution errors
- Individual threshold processing errors
- Schema setup errors

All errors are logged with appropriate detail levels.

## Output

The system provides:
1. Console output showing all generated queries
2. Database storage of generated queries in `threshold_generated_queries` table
3. Detailed logging of the entire process

## Requirements

- Python 3.7+
- PostgreSQL database
- Required Python packages (see requirements.txt)

## Notes

- The system uses the exact threshold query provided in the requirements
- Generated queries are not executed automatically - they are only generated and stored
- The system handles NULL values appropriately in all conditions
- Consecutive occurrence logic uses window functions for accurate detection
