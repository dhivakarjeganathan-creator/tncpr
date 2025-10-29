# Troubleshooting Guide

This guide helps resolve common issues when setting up and running the Streaming Analytics Loader.

## Python Version Issues

### Issue: `ModuleNotFoundError: No module named 'distutils'`

**Problem**: This error occurs with Python 3.12+ where `distutils` has been removed.

**Solutions**:

1. **Use the automated compatibility fix** (Recommended):
   ```bash
   # Run the compatibility checker
   python python312_compat.py
   
   # Or use the automated installer
   python install.py
   ```

2. **Install setuptools explicitly**:
   ```bash
   pip install setuptools wheel
   ```

3. **Use the updated requirements.txt**:
   ```bash
   # The updated requirements.txt includes Python 3.12+ compatibility
   pip install -r requirements.txt
   ```

### Issue: Python 3.12+ compatibility warnings

**Problem**: Warnings about deprecated features in Python 3.12+.

**Solutions**:

1. **The code automatically handles these warnings**:
   - Compatibility checks are built-in
   - Automatic setuptools installation
   - Graceful handling of deprecated features

2. **Manual compatibility setup**:
   ```bash
   # Install compatibility packages
   pip install setuptools wheel
   
   # Run compatibility check
   python python312_compat.py
   ```

### Issue: `psycopg2` installation fails

**Problem**: psycopg2 compilation issues on some systems.

**Solutions**:

1. **Use psycopg2-binary** (already in requirements.txt):
   ```bash
   pip install psycopg2-binary
   ```

2. **Install system dependencies** (Linux/macOS):
   ```bash
   # Ubuntu/Debian
   sudo apt-get install libpq-dev python3-dev
   
   # macOS
   brew install postgresql
   ```

3. **Use conda instead of pip**:
   ```bash
   conda install psycopg2
   ```

## Database Connection Issues

### Issue: `psycopg2.OperationalError: connection to server failed`

**Solutions**:

1. **Check PostgreSQL is running**:
   ```bash
   # Check if PostgreSQL is running
   pg_ctl status
   
   # Start PostgreSQL if not running
   pg_ctl start
   ```

2. **Verify database exists**:
   ```bash
   # List databases
   psql -l
   
   # Create database if it doesn't exist
   createdb streaming_analytics
   ```

3. **Check connection parameters in .env file**:
   ```env
   DB_HOST=localhost
   DB_NAME=streaming_analytics
   DB_USER=postgres
   DB_PASSWORD=your_actual_password
   DB_PORT=5432
   ```

### Issue: `psycopg2.OperationalError: FATAL: password authentication failed`

**Solutions**:

1. **Reset PostgreSQL password**:
   ```bash
   # Connect to PostgreSQL as superuser
   psql -U postgres
   
   # Change password
   ALTER USER postgres PASSWORD 'new_password';
   ```

2. **Update .env file with correct password**:
   ```env
   DB_PASSWORD=new_password
   ```

## File Permission Issues

### Issue: `PermissionError: [Errno 13] Permission denied`

**Solutions**:

1. **Check file permissions**:
   ```bash
   # Make sure you have read access to JSON file
   ls -la Streaming_analytics.json
   ```

2. **Run with appropriate permissions**:
   ```bash
   # On Windows, run as administrator if needed
   # On Linux/macOS, check file ownership
   ```

## JSON Parsing Issues

### Issue: `json.JSONDecodeError: Expecting value`

**Solutions**:

1. **Validate JSON file**:
   ```bash
   # Check if JSON is valid
   python -m json.tool Streaming_analytics.json > /dev/null
   ```

2. **Check file encoding**:
   ```bash
   # Ensure file is UTF-8 encoded
   file Streaming_analytics.json
   ```

## Environment Setup Issues

### Issue: `.env` file not found

**Solutions**:

1. **Run setup script**:
   ```bash
   python setup.py
   ```

2. **Create .env file manually**:
   ```env
   DB_HOST=localhost
   DB_NAME=streaming_analytics
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_PORT=5432
   LOG_LEVEL=INFO
   ```

### Issue: `ModuleNotFoundError: No module named 'config'`

**Solutions**:

1. **Ensure you're in the correct directory**:
   ```bash
   # Make sure you're in the project directory
   ls -la config.py
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Performance Issues

### Issue: Slow database operations

**Solutions**:

1. **Check database indexes**:
   ```sql
   -- Connect to database and check indexes
   \d streaming_jobs
   \d job_definitions
   \d job_metrics
   ```

2. **Optimize PostgreSQL settings**:
   ```sql
   -- Increase shared_buffers if needed
   SHOW shared_buffers;
   ```

## Getting Help

If you encounter issues not covered here:

1. **Check the logs**: The application provides detailed logging
2. **Verify your Python version**: `python --version`
3. **Check PostgreSQL version**: `psql --version`
4. **Ensure all dependencies are installed**: `pip list`

## Quick Fixes

### Reset Everything
```bash
# Remove virtual environment
rm -rf streaming_analytics_env

# Create new virtual environment
python3.11 -m venv streaming_analytics_env
source streaming_analytics_env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run setup
python setup.py
```

### Test Database Connection
```python
import psycopg2
from config import Config

try:
    conn = psycopg2.connect(**Config.get_db_config())
    print("Database connection successful!")
    conn.close()
except Exception as e:
    print(f"Database connection failed: {e}")
```
