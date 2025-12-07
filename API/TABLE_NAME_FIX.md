# Table Name Case Sensitivity Fix

## Issue
The database table name is `mkt_corning` (lowercase), but the code was using `MKT_Corning` (camel case), causing the error:
```
relation "MKT_Corning" does not exist
```

## Root Cause
In PostgreSQL, when table names are quoted with double quotes, they are case-sensitive. The query builder uses `_escape_identifier()` which wraps table names in double quotes, so the case must match exactly.

## Fix Applied

### Changed in `api/routes.py`:

**Before:**
```python
TABLE_CONFIG = {
    "mkt_corning": {
        "table_name": "MKT_Corning",  # Wrong case
        ...
    }
}
```

**After:**
```python
TABLE_CONFIG = {
    "mkt_corning": {
        "table_name": "mkt_corning",  # Correct lowercase
        ...
    }
}
```

### Also Updated Response
Changed the response to return the table name as provided (lowercase) instead of the internal table_name variable.

## Verification

The query will now generate:
```sql
FROM "mkt_corning"
```

Instead of:
```sql
FROM "MKT_Corning"
```

This matches the actual database table name and should resolve the error.

## Note
When adding other tables, make sure to use the exact case as it appears in the database:
- If the table is created as `mkt_corning` (lowercase), use `"mkt_corning"`
- If the table is created as `MKT_CORNING` (uppercase), use `"MKT_CORNING"`
- The case must match exactly when using quoted identifiers in PostgreSQL

