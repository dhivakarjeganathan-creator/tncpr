# API Changes Summary

## Changes Based on Updated Requirements

### 1. API Endpoint Format Changed

**Before:**
```
GET /api/v1/timeseries/mkt_corning?metrics=...
```

**After:**
```
GET /api/v1/timeseries?table=mkt_corning&metrics=...
```

### 2. Entity Filter Format Changed

**Before:**
```
id=143
market=US,EU
```

**After:**
```
searchByProperties=resource.id==143
searchByProperties=resource.market==US,EU
```

### 3. Timestamp Column Handling

- Timestamp column is stored as `character varying(50)` containing Unix timestamps in milliseconds as strings
- Comparisons are done as string comparisons
- Aggregation converts to timestamp, truncates, then converts back to Unix milliseconds string

### 4. New Parameters

- `table` (required): Table name (e.g., "mkt_corning")
- `searchByProperties`: Entity filters in format `resource.column==value`
- `flatten`: Ignored parameter (kept for compatibility)

### 5. Example API Call

```
GET /api/v1/timeseries?table=mkt_corning&metrics=ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number&start=1749992400000&end=1750057199000&searchByProperties=resource.id==143&properties=type,id&requestgranularity=1-hour
```

## Code Changes Made

### 1. `api/routes.py`
- Changed route from `/timeseries/mkt_corning` to `/timeseries`
- Added `table` parameter
- Added `searchByProperties` parameter parsing
- Added `parse_search_by_properties()` function to parse `resource.column==value` format
- Added `TABLE_CONFIG` dictionary for table configuration
- Removed individual entity filter parameters (market, region, etc.)

### 2. `database/query_builder.py`
- Updated `_format_timestamp()` to handle timestamp as string (character varying)
- Updated `_get_aggregation_function()` to properly handle string timestamp aggregation
- Timestamp comparisons now use string comparison
- Aggregation converts string to timestamp, truncates, then converts back to Unix milliseconds string

## Next Steps

1. Update tests to use new API format
2. Add support for other tables (GNB_Corning, SECTOR_Corning, etc.)
3. Test with actual database data

