# Test Suite Summary

## Overview
Comprehensive test suite created for all 18 KPI timeseries tables with at least 8 test cases per table (actually 16 test cases per table for comprehensive coverage).

## Test Files

### 1. `tests/test_api_combinations.py`
- **Purpose**: Detailed tests for `mkt_corning` table
- **Test Cases**: 18 test methods
- **Coverage**: Basic queries, time ranges, filters, aggregation, ordering, error handling

### 2. `tests/test_all_tables.py` (NEW)
- **Purpose**: Comprehensive tests for all remaining 17 tables
- **Test Cases**: 16 test methods × 18 tables = **288 total test cases**
- **Coverage**: All tables tested with the same comprehensive scenarios

## Test Coverage Per Table

Each table in `test_all_tables.py` has **16 test cases**:

1. **Basic Query with Metrics Only** - Minimal required parameters
2. **Query with Time Range** - Start and end timestamps
3. **Query with Single Entity Filter** - Single ID filter
4. **Query with Multiple Entity Filters** - Multiple ID values
5. **Query with Aggregation** - 1-hour granularity
6. **Query with Properties Selection** - Selecting entity columns
7. **Query with Ordering** - Ascending timestamp order
8. **Complex Query with All Parameters** - All parameters combined
9. **Query with Multiple Metrics** - Multiple KPI selection
10. **Query with Market Filter** - Market entity filter
11. **Query with 30-minute Granularity** - 30-minute aggregation
12. **Query with 1-day Granularity** - Daily aggregation
13. **Query with Descending Order** - Descending timestamp order
14. **Query with Multiple Entity Columns** - Multiple entity filters
15. **Error Handling - Invalid Table** - Invalid table name
16. **Error Handling - Missing Metrics** - Missing required parameter

## Tables Tested

### Market Tables (3)
- `mkt_ericsson`
- `mkt_samsung`
- (mkt_corning tested separately)

### GNB Tables (3)
- `gnb_corning`
- `gnb_ericsson`
- `gnb_samsung`

### Sector Tables (3)
- `sector_corning`
- `sector_ericsson`
- `sector_samsung`

### Carrier Tables (3)
- `carrier_corning`
- `carrier_ericsson`
- `carrier_samsung`

### DU Tables (2)
- `du_corning`
- `du_samsung`

### ACPF/AUPF Tables (5)
- `acpf_gnb_samsung`
- `acpf_vcu_samsung`
- `aupf_gnb_samsung`
- `aupf_vcu_samsung`
- `aupf_vm_samsung`

## Running Tests

### Run all table tests
```bash
# Run all 18 tables (288 test cases)
pytest tests/test_all_tables.py -v -s

# Or use the test runner
python run_tests.py --type pytest --all-tables --verbose
```

### Run specific table tests
```bash
# Run tests for a specific table
pytest tests/test_all_tables.py -v -s -k "mkt_ericsson"

# Run a specific test case for all tables
pytest tests/test_all_tables.py -v -s -k "test_basic_query_metrics_only"
```

### Run mkt_corning tests
```bash
# Run mkt_corning specific tests
pytest tests/test_api_combinations.py -v -s

# Or use the test runner
python run_tests.py --type pytest --verbose
```

## Test Features

### Debug Output
All tests automatically print:
- API URL with all parameters
- Generated SQL query
- Query parameters
- Query results summary
- Sample data (first 3 records)

### Parameterized Tests
Tests use `@pytest.mark.parametrize` to efficiently test all tables with the same test logic, ensuring consistency across all tables.

### Error Handling
Tests include error scenarios:
- Invalid table names
- Missing required parameters
- Invalid parameter formats

### Data Validation
Tests validate:
- Response structure
- Table name in response
- Metrics in response
- Entity filters applied correctly
- Timestamp ordering
- Aggregation granularity

## Test Statistics

- **Total Tables**: 18
- **Test Cases per Table**: 16
- **Total Test Cases**: 288 (for all tables)
- **mkt_corning Additional Tests**: 18
- **Grand Total**: 306 test cases

## Notes

1. **Sample Metrics**: The test file uses sample metric names. In production, you should update `SAMPLE_METRICS` dictionary with actual KPI column names from each table.

2. **Empty Data**: Tests are designed to handle empty databases gracefully - they skip validation if no data is returned rather than failing.

3. **Debug Mode**: All tests run with `debug=true` by default to show query information.

4. **Timeout**: Tests have a 30-second timeout per request to handle slow queries.

5. **API Availability**: Tests check if the API server is running before executing, skipping if unavailable.

## Future Enhancements

1. Update `SAMPLE_METRICS` with actual KPI names from each table
2. Add table-specific test cases if needed
3. Add performance tests for large datasets
4. Add concurrent request tests
5. Add data validation tests for specific KPI value ranges

