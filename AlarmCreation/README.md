# Alarm Processing System

This system processes alarm data from a PostgreSQL database and generates INSERT statements for each alarm record.

## Features

- Connects to PostgreSQL database using environment variables
- Executes a query to retrieve alarm data based on last execution time
- Generates INSERT statements for each row returned
- Tracks last execution time to avoid duplicate processing
- Saves all SQL statements to a timestamped file
- Handles SQL injection protection with proper escaping

## Files

- `alarm_processor.py` - Main Python script
- `database.env` - Database configuration file
- `requirements.txt` - Python dependencies
- `alarmreq.txt` - Original requirements document

## Setup

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Update database configuration in `database.env`:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=hierarchy_db
   DB_USER=postgres
   DB_PASSWORD=test@1234
   ```

3. Ensure your PostgreSQL database is running and accessible with the provided credentials.

## Usage

Run the alarm processor:
```bash
python alarm_processor.py
```

The script will:
1. Connect to the database
2. Create/verify the `alarm_last_execution` table
3. Execute the alarm query using the last execution time
4. Generate INSERT statements for each result
5. Save all statements to a timestamped SQL file (e.g., `alarm_inserts_20231201_143022.sql`)
6. Update the last execution time

## Database Schema

The script expects the following tables to exist:
- `threshold_alarms` - Contains alarm data
- `nfname_results` - Contains node information
- `alerts.status` - Target table for INSERT statements

The script will automatically create:
- `alarm_last_execution` - Tracks last execution time

## Query Logic

The system uses the following query to retrieve alarm data:
```sql
SELECT distinct
    b.nfname_expr_value AS node,
    CASE 
        WHEN lower(a.alarm_severity) = 'critical' THEN 5
        WHEN lower(a.alarm_severity) = 'major' THEN 4
        WHEN lower(a.alarm_severity) = 'minor' THEN 3
        ELSE 0
    END AS severity,
    a.alarm_message AS summary,
    a.metricname AS alertgroup,
    a.record_id AS alertkey,
    a.record_id AS identifier,
    '2' AS type,
    b.region AS Region
FROM threshold_alarms a 
LEFT JOIN nfname_results b ON a.record_id = b.id_expr
WHERE a.created_at <= [last_execution_time]
```

## Output

The script generates INSERT statements in the following format:
```sql
INSERT INTO alerts.status(Node,Severity,Summary,AlertGroup,AlertKey,Identifier,Type,Region) VALUES 
('node_value', 5, 'summary_text', 'alertgroup_value', 'alertkey_value', 'identifier_value', 2, 'region_value');
```

All statements are saved to a single SQL file with a timestamp in the filename.
