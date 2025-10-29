# Metrics and Tables SQL Execution

This project implements the requirements to execute the `metricsandtables.sql` file in a PostgreSQL database.

## Files Created

- `.env` - Database configuration file
- `execute_sql.py` - Python script to execute the SQL file
- `requirements.txt` - Python dependencies
- `README.md` - This documentation file

## Setup Instructions

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure database connection:**
   - The `.env` file contains the database configuration
   - Update the values in `.env` if your database settings are different:
     ```
     DB_HOST=localhost
     DB_PORT=5432
     DB_NAME=hierarchy_db
     DB_USER=postgres
     DB_PASSWORD=test@1234
     ```

3. **Ensure PostgreSQL is running:**
   - Make sure PostgreSQL is installed and running
   - Ensure the database `hierarchy_db` exists
   - Verify the user `postgres` has access to the database

## Usage

Run the Python script to execute the SQL file:

```bash
python execute_sql.py
```

## What the Script Does

The `execute_sql.py` script will:

1. Load database configuration from `.env` file
2. Connect to the PostgreSQL database
3. Execute the `metricsandtables.sql` file
4. Display the results including:
   - Number of records inserted
   - Sample records from the `metricsandtables` table

## SQL File Contents

The `metricsandtables.sql` file:
- Creates a `metricsandtables` table
- Populates it with table and column information from the database schema
- Inserts specific data from the `ruleexecutionresults` table

## Error Handling

The script includes error handling for:
- Database connection issues
- SQL file not found
- SQL execution errors
- Proper connection cleanup
