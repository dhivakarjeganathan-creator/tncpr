# Logging Configuration for Threshold Execution System

This document explains how to configure and use logging in the Threshold Execution System.

## Overview

The system now includes comprehensive logging functionality that saves logs to files while optionally displaying them in the console. This is essential for production environments where you need to track system behavior, debug issues, and monitor performance.

## Files Added

1. **`logging_config.py`** - Centralized logging configuration module
2. **`example_with_logging.py`** - Example script showing how to use logging
3. **`run_with_logging.py`** - Production script with logging enabled
4. **`test_system.py`** - Updated to use file logging

## Quick Start

### Basic Usage

```python
from logging_config import setup_logging, get_logger

# Set up logging
log_file = setup_logging(
    log_level=logging.INFO,
    include_console=True
)

# Get a logger
logger = get_logger(__name__)
logger.info("Your log message here")
```

### Advanced Configuration

```python
from logging_config import setup_logging, get_logger
import logging

# Custom configuration
log_file = setup_logging(
    log_level=logging.DEBUG,  # More verbose logging
    log_file='custom_log_file.log',  # Custom log file name
    include_console=False  # Disable console output for production
)

logger = get_logger(__name__)
```

## Log File Structure

### File Location
- **Default**: `logs/threshold_execution_YYYYMMDD_HHMMSS.log`
- **Custom**: Specify your own path in `setup_logging()`

### Log Format
```
2025-10-05 21:22:21,889 - module_name - INFO - function_name:line_number - Log message
```

### Log Levels
- **DEBUG**: Detailed information for debugging
- **INFO**: General information about program execution
- **WARNING**: Something unexpected happened but the program continues
- **ERROR**: A serious problem occurred
- **CRITICAL**: A very serious error occurred

## Production Usage

### 1. Using the Production Script

```bash
python run_with_logging.py
```

This will:
- Create timestamped log files in the `logs/` directory
- Display logs in console (can be disabled)
- Log all threshold processing activities
- Provide detailed execution summaries

### 2. Custom Integration

```python
from logging_config import setup_logging, get_logger
from threshold_processor import ThresholdProcessor
from database import DatabaseConnection

# Set up logging
log_file = setup_logging(log_level=logging.INFO, include_console=True)
logger = get_logger(__name__)

# Your application code
db_connection = DatabaseConnection()
processor = ThresholdProcessor(db_connection)

logger.info("Starting threshold processing...")
results = processor.process_all_thresholds()
logger.info(f"Processed {len(results)} thresholds")
```

## Log File Management

### Automatic Timestamping
Log files are automatically timestamped to prevent overwrites:
- `threshold_execution_20251005_212221.log`
- `threshold_execution_20251005_214530.log`

### Log Rotation
For production environments, consider implementing log rotation:

```python
from logging.handlers import RotatingFileHandler

# Add to logging_config.py for log rotation
file_handler = RotatingFileHandler(
    log_file, 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=5
)
```

## Monitoring and Alerts

### Key Log Messages to Monitor

1. **System Startup/Shutdown**:
   ```
   INFO - Starting Threshold Execution System
   INFO - Threshold execution completed successfully
   ```

2. **Database Operations**:
   ```
   INFO - Database connection established successfully
   INFO - Retrieved X threshold jobs
   ```

3. **Threshold Processing**:
   ```
   INFO - Processed X threshold jobs
   WARNING - ALERT: X threshold violations detected
   ```

4. **Errors**:
   ```
   ERROR - Error generating threshold query
   CRITICAL - Database connection failed
   ```

### Log Analysis Examples

**Count threshold violations**:
```bash
grep "ALERT:" threshold_execution_*.log | wc -l
```

**Find errors**:
```bash
grep "ERROR" threshold_execution_*.log
```

**Monitor specific threshold**:
```bash
grep "THR001" threshold_execution_*.log
```

## Configuration Options

### Log Levels
- `logging.DEBUG` - Most verbose, includes all messages
- `logging.INFO` - General information (recommended for production)
- `logging.WARNING` - Warnings and above
- `logging.ERROR` - Errors and above
- `logging.CRITICAL` - Only critical messages

### Console Output
- `include_console=True` - Show logs in console (development)
- `include_console=False` - File-only logging (production)

### Log File Location
- Default: `logs/` directory (auto-created)
- Custom: Specify full path in `log_file` parameter

## Troubleshooting

### Common Issues

1. **No log file created**:
   - Check write permissions in the directory
   - Ensure `logs/` directory exists or can be created

2. **Empty log file**:
   - Verify logging configuration is called before other modules
   - Check log level settings

3. **Console output not showing**:
   - Set `include_console=True` in `setup_logging()`
   - Check if logging is configured multiple times

### Debug Mode

Enable debug logging for troubleshooting:

```python
log_file = setup_logging(
    log_level=logging.DEBUG,
    include_console=True
)
```

## Best Practices

1. **Use appropriate log levels**:
   - INFO for normal operations
   - WARNING for recoverable issues
   - ERROR for serious problems

2. **Include context in log messages**:
   ```python
   logger.info(f"Processing threshold {threshold_id} for metric {metric_name}")
   ```

3. **Log important state changes**:
   ```python
   logger.info(f"Threshold processing completed. {len(results)} jobs processed")
   ```

4. **Use structured logging for complex data**:
   ```python
   logger.info(f"Threshold result: {result}")
   ```

5. **Monitor log file sizes** in production environments

## Integration with Monitoring Systems

The structured log format makes it easy to integrate with monitoring systems like:
- ELK Stack (Elasticsearch, Logstash, Kibana)
- Splunk
- CloudWatch Logs
- Datadog

Example log parsing for monitoring:
```python
# Extract threshold violations for alerting
import re
with open('threshold_execution.log', 'r') as f:
    content = f.read()
    violations = re.findall(r'ALERT: (\d+) threshold violations', content)
    total_violations = sum(int(v) for v in violations)
```

This logging system provides comprehensive visibility into the Threshold Execution System's operation, making it easier to monitor, debug, and maintain in production environments.
