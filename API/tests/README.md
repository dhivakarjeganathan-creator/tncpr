# Test Suite for KPI Timeseries API

This directory contains comprehensive test scripts for validating the KPI Timeseries API with various parameter combinations based on the examples provided in the requirements.

## Test Files

### 1. `test_api_combinations.py`
Pytest-based automated test suite with comprehensive test cases covering:
- Basic queries with metrics only
- Time range queries
- Entity filtering (single and multiple values)
- Aggregation with different granularities
- Properties selection
- Ordering (ascending/descending)
- Complex queries combining all parameters
- Error handling

### 2. `test_manual_validation.py`
Manual test script that can be run directly to validate API combinations. This script:
- Tests all combinations from the requirements examples
- Provides detailed output for each test
- Shows response structure and sample data
- Validates error cases

## Running the Tests

### Prerequisites

1. Install test dependencies:
```bash
pip install pytest requests
```

2. Make sure the API server is running:
```bash
python main.py
```

### Running Pytest Tests

```bash
# Run all tests
pytest tests/test_api_combinations.py -v

# Run specific test
pytest tests/test_api_combinations.py::TestAPICombinations::test_basic_query_metrics_only -v

# Run with output
pytest tests/test_api_combinations.py -v -s
```

### Running Manual Validation Script

```bash
python tests/test_manual_validation.py
```

This will run all test cases interactively and show detailed results.

## Test Cases Overview

### Test Case 1: Basic Query
- **Purpose**: Validate minimal required parameters
- **Parameters**: `metrics` only
- **Expected**: Returns data with specified metrics

### Test Case 2: Time Range Query
- **Purpose**: Validate timestamp filtering (Example 2 style)
- **Parameters**: `metrics`, `start`, `end`
- **Expected**: Returns data within time range

### Test Case 3: Single Entity Filter
- **Purpose**: Validate single entity filtering
- **Parameters**: `metrics`, `market`
- **Expected**: Returns filtered data

### Test Case 4: Multiple Entity Values
- **Purpose**: Validate multiple entity filter values (Example 2 style)
- **Parameters**: `metrics`, `id` (comma-separated values)
- **Expected**: Returns data matching any of the filter values

### Test Case 5: Aggregation
- **Purpose**: Validate time aggregation (Example 1 style)
- **Parameters**: `metrics`, `start`, `end`, `requestgranularity`
- **Expected**: Returns aggregated data by specified granularity

### Test Case 6: Properties Selection
- **Purpose**: Validate entity column selection (Example 1 style)
- **Parameters**: `metrics`, `properties`
- **Expected**: Returns only specified entity columns

### Test Case 7: Ordering
- **Purpose**: Validate timestamp ordering (Example 2 style)
- **Parameters**: `metrics`, `start`, `end`, `orderBy`
- **Expected**: Returns data ordered by timestamp

### Test Case 8: Complex Query (Example 1)
- **Purpose**: Validate complex query matching Example 1 from requirements
- **Parameters**: Multiple metrics, time range, entity filters, properties, granularity
- **Expected**: Returns aggregated data with all filters applied

### Test Case 9: Complex Query (Example 2)
- **Purpose**: Validate complex query matching Example 2 from requirements
- **Parameters**: Multiple metrics, multiple entity filters, time range, ordering
- **Expected**: Returns ordered data with all filters applied

### Test Case 10: Different Granularities
- **Purpose**: Validate different time granularity options
- **Parameters**: `requestgranularity` with "30-minute", "1-hour", "1-day"
- **Expected**: Returns data aggregated by specified granularity

### Test Case 11: All Entity Filters
- **Purpose**: Validate all entity filter types together
- **Parameters**: All entity columns with multiple values
- **Expected**: Returns data matching all filter conditions

### Test Case 12: Error Handling
- **Purpose**: Validate error responses
- **Tests**: Missing parameters, invalid formats, invalid properties
- **Expected**: Appropriate error status codes and messages

## Example API Calls Tested

### Example 1 Style (from requirements):
```
GET /api/v1/timeseries/mkt_corning?
  metrics=SystemUsage.AvgCPU0Freq.number,SystemUsage.CPU0Power.number,...
  &start=1749992400000
  &end=1750057199000
  &market=US,EU
  &properties=market,region,technology
  &requestgranularity=1-hour
```

### Example 2 Style (from requirements):
```
GET /api/v1/timeseries/mkt_corning?
  metrics=DU.S5NC_DRBDrop_pct_SA,DU.S5NC_NRDCAdd_MN_Succ_pct_SA,...
  &id=00000000001,03100010001,03100010002,...
  &start=1749992400000
  &end=1750057199000
  &orderBy=time+
```

## Notes

- Tests assume the API server is running on `http://localhost:8000`
- Tests may fail if the database doesn't have sample data
- Some tests validate response structure rather than actual data values
- Error tests verify proper error handling and status codes

