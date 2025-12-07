# Fixes Summary - Requirements and Error Fixes

## Issues Fixed

### 1. Error: `function pg_catalog.btrim(bigint) does not exist`

**Problem:**
When using aggregation (`requestgranularity=1-hour`), the query builder was trying to use `TRIM()` on columns that are of type `bigint`. PostgreSQL's `TRIM()` function only works on text types, causing errors like:
```
function pg_catalog.btrim(bigint) does not exist
```

**Root Cause:**
Some KPI columns in the database are `bigint` type (e.g., `gnb_endcaddatt`, `gnb_endcaddsucc`, `aupf_cpuusageavg_percent`, `aupfvminterface_inoctets_vm_aupf`), while others are `character varying` or `text`. The query builder was assuming all columns were text and applying `TRIM()` directly.

**Fix Applied:**
Updated `database/query_builder.py` to cast columns to `TEXT` first before applying `TRIM()`. This allows the query to work with both `bigint` and `character varying` columns.

**Before:**
```python
f"AVG(CASE WHEN TRIM({metric_col}) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM({metric_col}) AS NUMERIC) ELSE NULL END) AS {metric_col}"
```

**After:**
```python
f"AVG(CASE WHEN TRIM(CAST({metric_col} AS TEXT)) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(CAST({metric_col} AS TEXT)) AS NUMERIC) ELSE NULL END) AS {metric_col}"
```

**Affected Tables:**
- `acpf_gnb_samsung` - columns like `gnb_endcaddatt` (bigint)
- `acpf_vcu_samsung` - columns like `acpf_cpuusageavg_percent` (character varying, but some are bigint)
- `aupf_vcu_samsung` - columns like `aupf_cpuusageavg_percent` (bigint)
- `aupf_vm_samsung` - columns like `aupfvminterface_inoctets_vm_aupf` (bigint)
- Any other tables with `bigint` KPI columns

## Files Modified

1. **database/query_builder.py**
   - Updated aggregation logic to cast columns to TEXT before applying TRIM()
   - Works for both PostgreSQL and Presto (with appropriate syntax)

## Testing

The fix ensures that:
1. Columns of type `bigint` can be aggregated without errors
2. Columns of type `character varying` continue to work as before
3. Both types are properly validated and converted to NUMERIC for aggregation
4. Invalid values (non-numeric strings) are safely handled by returning NULL

## Verification

After this fix, queries with `requestgranularity=1-hour` should work correctly for all tables, regardless of whether the KPI columns are `bigint` or `character varying`.

