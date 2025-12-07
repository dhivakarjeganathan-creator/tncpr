# How to See Test Output (API URL, Query, Results)

## The Issue
Pytest by default captures stdout/stderr, so `print()` statements don't show unless you use the `-s` flag.

## Solutions

### Option 1: Run pytest with `-s` flag (Recommended)
```bash
pytest tests/test_api_combinations.py -v -s
```

The `-s` flag tells pytest to show print statements.

### Option 2: Use the test runner script
```bash
python run_tests.py --type pytest --verbose
```

The script now automatically includes the `-s` flag.

### Option 3: Use pytest.ini configuration
I've created a `pytest.ini` file that includes `-s` by default. Just run:
```bash
pytest tests/test_api_combinations.py -v
```

### Option 4: Test a single API call (Easiest to see output)
```bash
python test_single_api.py
```

This script makes one API call and shows all the output clearly.

### Option 5: Run manual validation tests
```bash
python tests/test_manual_validation.py
```

These tests always show output (they're not pytest).

## What You'll See

When you run tests with output enabled, you'll see:

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
WHERE "id" = %s
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
    ranmarket_endc_sessions_rn: 123.45
    ...
================================================================================
```

## Quick Test

To quickly verify it works, run:
```bash
python test_single_api.py
```

This will make one API call and show all the debug information.

## Troubleshooting

If you still don't see output:
1. Make sure the API server is running: `python main.py`
2. Check that you're using the `-s` flag with pytest
3. Try the single API test script first: `python test_single_api.py`
4. Check that `debug=true` is in the API request parameters

