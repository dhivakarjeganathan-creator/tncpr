# All 18 Tables Implemented

## Summary
All 18 KPI timeseries tables have been added to the API configuration. The API now supports querying any of these tables using the same `/timeseries` endpoint with the `table` parameter.

## Tables Configured

### Market Tables (3)
1. **mkt_corning** - Corning Market KPI timeseries data
2. **mkt_ericsson** - Ericsson Market KPI timeseries data
3. **mkt_samsung** - Samsung Market KPI timeseries data

### GNB Tables (3)
4. **gnb_corning** - Corning GNB KPI timeseries data
5. **gnb_ericsson** - Ericsson GNB KPI timeseries data
6. **gnb_samsung** - Samsung GNB KPI timeseries data

### Sector Tables (3)
7. **sector_corning** - Corning Sector KPI timeseries data
8. **sector_ericsson** - Ericsson Sector KPI timeseries data
9. **sector_samsung** - Samsung Sector KPI timeseries data

### Carrier Tables (3)
10. **carrier_corning** - Corning Carrier KPI timeseries data
11. **carrier_ericsson** - Ericsson Carrier KPI timeseries data
12. **carrier_samsung** - Samsung Carrier KPI timeseries data

### DU Tables (2)
13. **du_corning** - Corning DU KPI timeseries data
14. **du_samsung** - Samsung DU KPI timeseries data

### ACPF/AUPF Tables (5)
15. **acpf_gnb_samsung** - ACPF GNB KPI timeseries data
16. **acpf_vcu_samsung** - ACPF VCU KPI timeseries data
17. **aupf_gnb_samsung** - AUPF GNB KPI timeseries data
18. **aupf_vcu_samsung** - AUPF VCU KPI timeseries data
19. **aupf_vm_samsung** - AUPF VM KPI timeseries data

## Entity Columns
All tables use the same entity columns:
- `market`
- `region`
- `vcptype`
- `technology`
- `datacenter`
- `site`
- `id`

## Timestamp Column
All tables use `timestamp` as the timestamp column (stored as `character varying(50)` containing Unix timestamps in milliseconds as strings).

## API Usage

### Example API Call
```
GET /api/v1/timeseries?table=mkt_ericsson&metrics=ranmarket_5gnr_endc_setup_failure_pct,ranmarket_5gnr_dl_mac_volume_mb&start=1749992400000&end=1750057199000&searchByProperties=resource.id==143&properties=market,id&requestgranularity=1-hour
```

### Supported Tables
You can use any of the 18 table names in the `table` parameter:
- `mkt_corning`, `mkt_ericsson`, `mkt_samsung`
- `gnb_corning`, `gnb_ericsson`, `gnb_samsung`
- `sector_corning`, `sector_ericsson`, `sector_samsung`
- `carrier_corning`, `carrier_ericsson`, `carrier_samsung`
- `du_corning`, `du_samsung`
- `acpf_gnb_samsung`, `acpf_vcu_samsung`
- `aupf_gnb_samsung`, `aupf_vcu_samsung`, `aupf_vm_samsung`

## Implementation Details

### Changes Made
1. Updated `api/routes.py` - Added all 18 tables to `TABLE_CONFIG`
2. All tables share the same entity column structure
3. All tables use the same timestamp column format
4. The existing API endpoint automatically supports all tables

### No Code Changes Required
The existing API implementation already supports multiple tables through the `TABLE_CONFIG` dictionary. By adding all tables to this configuration, they are automatically available through the same endpoint.

## Testing
To test any table, use the same API format but change the `table` parameter:

```bash
# Test mkt_ericsson
curl "http://localhost:8000/api/v1/timeseries?table=mkt_ericsson&metrics=ranmarket_5gnr_endc_setup_failure_pct&debug=true"

# Test gnb_corning
curl "http://localhost:8000/api/v1/timeseries?table=gnb_corning&metrics=<kpi_name>&debug=true"
```

## Notes
- All table names are lowercase to match the database schema
- Entity columns are consistent across all tables
- Timestamp handling is the same for all tables (character varying with Unix milliseconds)
- Aggregation and filtering work the same way for all tables

