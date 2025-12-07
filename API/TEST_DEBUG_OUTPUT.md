# Test Debug Output Feature

## Overview
Added comprehensive debug output to all tests that prints:
1. **API URL** - The full API endpoint with all parameters
2. **Generated SQL Query** - The actual SQL query executed against the database
3. **Query Results** - Summary and sample data from the query

## Changes Made

### 1. API Route Enhancement (`api/routes.py`)
- Added `debug` parameter (optional, defaults to `False`)
- When `debug=true`, the response includes a `debug` object containing:
  - `query`: The generated SQL query
  - `query_params`: Query parameters (for PostgreSQL)
  - `table_name`: The actual database table name used
  - `entity_filters`: Parsed entity filters
  - `properties`: Selected properties

**Example API call with debug:**
```
GET /api/v1/timeseries?table=mkt_corning&metrics=ranmarket_endc_sessions_rn&debug=true
```

### 2. Pytest Test Suite (`tests/test_api_combinations.py`)
- Updated `make_request()` method to automatically add `debug=true` parameter
- Added `_print_test_info()` method that prints:
  - API URL
  - Generated SQL query
  - Query parameters
  - Results summary (table, metrics, count, timestamps, granularity)
  - Sample data (first 3 records)

**Output Format:**
```
================================================================================
API REQUEST
================================================================================
URL: http://localhost:8000/api/v1/timeseries?table=mkt_corning&metrics=...
Status Code: 200

--------------------------------------------------------------------------------
GENERATED SQL QUERY
--------------------------------------------------------------------------------
SELECT "timestamp" AS "timestamp", "market", "region", ...
FROM "mkt_corning"
WHERE ...
ORDER BY "timestamp" ASC

Query Parameters: ['143']

--------------------------------------------------------------------------------
QUERY RESULTS
--------------------------------------------------------------------------------
Table: mkt_corning
Metrics: ranmarket_endc_sessions_rn, ranmarket_intra_cu_ho_attempts_number
Count: 10
Start: 1749992400000
End: 1750057199000

Sample Data (showing first 3 records):

  Record 1:
    timestamp: 1749992400000
    market: US
    region: East
    ranmarket_endc_sessions_rn: 123.45
    ...

  Record 2:
    ...

================================================================================
```

### 3. Manual Validation Script (`tests/test_manual_validation.py`)
- Updated `print_response()` function to show:
  - API URL
  - Generated SQL query (if debug mode)
  - Query parameters
  - Results summary
  - Sample data
- All test functions automatically include `debug=true` parameter

## Usage

### Running Pytest Tests
```bash
pytest tests/test_api_combinations.py -v -s
```
The `-s` flag shows print statements. Each test will print:
- API URL
- Generated SQL query
- Query results

### Running Manual Tests
```bash
python tests/test_manual_validation.py
```
All test cases will automatically print debug information.

### Using Debug in API Calls
Add `debug=true` to any API call:
```
GET /api/v1/timeseries?table=mkt_corning&metrics=ranmarket_endc_sessions_rn&debug=true
```

## Benefits

1. **Transparency**: See exactly what SQL is being generated
2. **Debugging**: Easily identify query issues
3. **Validation**: Verify queries match expectations
4. **Learning**: Understand how API parameters translate to SQL
5. **Troubleshooting**: Quickly identify problems with filters, aggregation, etc.

## Notes

- Debug information is only included when `debug=true` is specified
- Query parameters are shown for PostgreSQL (empty list for Presto)
- Sample data shows first 3 records to keep output manageable
- All tests automatically enable debug mode for full visibility

