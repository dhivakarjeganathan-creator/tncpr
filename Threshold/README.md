# Threshold Rules Processor

A Python application that processes threshold definition JSON files and stores the extracted rules in a PostgreSQL database.

## Overview

This application reads threshold definitions from a JSON file, extracts threshold rules based on categories (critical, major, minor, warning) and modes (burst, period), and stores them in a structured database table.

## Features

- **JSON Parsing**: Reads threshold definition JSON files
- **Rule Extraction**: Automatically detects categories and modes from evaluation data
- **Database Storage**: Stores extracted rules in PostgreSQL database
- **Flexible Processing**: Handles multiple evaluations and rule combinations
- **Error Handling**: Robust error handling and validation

## Files

- `main.py` - Main application entry point
- `database_schema.py` - PostgreSQL database schema and operations
- `threshold_parser.py` - JSON parsing and rule extraction logic
- `config.py` - Configuration management
- `requirements.txt` - Python dependencies
- `README.md` - This documentation

## Database Schema

The application creates a `threshold_rules` table with the following columns:

- `threshold_id` (SERIAL PRIMARY KEY) - Auto-incrementing ID
- `name` (VARCHAR(255)) - Threshold rule name
- `metric` (VARCHAR(255)) - Metric name
- `mode` (VARCHAR(50)) - Mode: 'burst' or 'period'
- `category` (VARCHAR(50)) - Category: 'critical', 'major', 'minor', or 'warning'
- `lowerlimit` (DECIMAL(10,2)) - Lower limit value
- `upperlimit` (DECIMAL(10,2)) - Upper limit value
- `occurrence` (INTEGER) - Occurrence count
- `clearoccurrence` (INTEGER) - Clear occurrence count
- `cleartime` (INTEGER) - Clear time value
- `time` (INTEGER) - Time value
- `activeuntil` (TEXT) - Active until date
- `periodgranularity` (INTEGER) - Period granularity
- `schedule` (TEXT) - Schedule information
- `created_at` (TIMESTAMP) - Record creation timestamp
- `updated_at` (TIMESTAMP) - Record update timestamp

## Prerequisites

- Python 3.6 or higher
- PostgreSQL database server
- Required Python packages (install with `pip install -r requirements.txt`)

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Database Configuration

Create a `.env` file with your PostgreSQL credentials:

```bash
python main.py --create-env
```

This creates a `.env.template` file. Copy it to `.env` and update with your values:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=threshold_rules
DB_USER=postgres
DB_PASSWORD=your_password_here

# Application Configuration
JSON_FILE=Threshold_definitions.json
```

### 3. Create PostgreSQL Database

Create the database in PostgreSQL:

```sql
CREATE DATABASE threshold_rules;
```

## Usage

### 1. Create Sample Data (Optional)

To create a sample JSON file for testing:

```bash
python main.py --create-sample
```

This creates `Threshold_definitions.json` with sample data.

### 2. Run the Application

```bash
python main.py
```

The application will:
1. Load the JSON file (`Threshold_definitions.json`)
2. Connect to the PostgreSQL database
3. Create the `threshold_rules` table (if it doesn't exist)
4. Extract threshold rules from the JSON
5. Store the rules in the database
6. Display a summary of processed rules

### 3. Using Your Own JSON File

Place your `Threshold_definitions.json` file in the same directory as the Python scripts and run:

```bash
python main.py
```

## JSON File Format

The JSON file should contain threshold definitions with the following structure:

```json
{
    "name": "Threshold Rule Name",
    "metric": "Metric Name",
    "evaluations": [
        {
            "burst_critical_enabled": true,
            "period_critical_enabled": true,
            "burst_critical_lower_limit": "0.0",
            "burst_critical_upper_limit": "100.0",
            "period_critical_lower_limit": "80.0",
            "period_critical_upper_limit": "100.0",
            "active_until": "No end date",
            "period_granularity": 1,
            "schedule": "",
            // ... other evaluation parameters
        }
    ]
}
```

## Rule Extraction Logic

The application automatically detects:

1. **Mode Detection**: 
   - If any `burst_*_enabled` is true → 'burst' mode
   - If any `period_*_enabled` is true → 'period' mode

2. **Category Detection**:
   - `*_critical_*` enabled → 'critical' category
   - `*_major_*` enabled → 'major' category
   - `*_minor_*` enabled → 'minor' category
   - `*_warning_*` enabled → 'warning' category

3. **Rule Creation**: One rule is created for each enabled category-mode combination.

## Example Output

```
=== Threshold Rules Processor ===

1. Initializing database: threshold_rules.db
Table 'threshold_rules' created successfully.

2. Loading JSON file: Threshold_definitions.json
Successfully loaded JSON from Threshold_definitions.json

Threshold Information:
  Name: ACPF.Daily.CpuUsageMax.percent
  Metric: ACPF.Daily.CpuUsageMax.percent
  Owner: icpadmin
  Evaluations: 1

3. Processing evaluations...
Extracted rule: critical period - ACPF.Daily.CpuUsageMax.percent
Extracted 1 threshold rules.

4. Inserting rules into database...
  Inserted rule ID 1: critical period

Successfully inserted 1 rules into the database.

5. Database Summary:
  Total rules in database: 1

Sample data:
  ID: 1, Category: critical, Mode: period, Lower: 80.0, Upper: 0.0

Processing completed successfully!
```

## Error Handling

The application includes comprehensive error handling for:
- Missing JSON files
- Invalid JSON format
- Database connection issues
- Rule extraction errors
- Data validation errors

## Requirements

- Python 3.6 or higher
- PostgreSQL database server
- psycopg2-binary (PostgreSQL adapter)
- python-dotenv (environment variable management)
