# Error Fixes Summary

## Issues Fixed

### Problem: Column Does Not Exist Errors
The test file was using placeholder metric names that don't exist in the actual database tables, causing errors like:
```
column "gnb_s5nc_drbdrop_pct_sa" does not exist
column "acpf_gnb_metric1" does not exist
```

### Root Cause
The `SAMPLE_METRICS` dictionary in `tests/test_all_tables.py` contained placeholder or incorrect column names that don't match the actual database schema.

### Fix Applied
Updated `SAMPLE_METRICS` dictionary with actual column names extracted from CREATE TABLE statements:

**Before (Incorrect):**
```python
"gnb_samsung": "gnb_s5nc_drbdrop_pct_sa,gnb_s5nc_drbsetupfail_pct_sa",
"sector_samsung": "sector_s5nc_drbdrop_pct_sa,sector_s5nc_drbsetupfail_pct_sa",
"du_samsung": "du_s5nc_drbdrop_pct_sa,du_s5nc_drbsetupfail_pct_sa",
"acpf_gnb_samsung": "acpf_gnb_metric1,acpf_gnb_metric2",
"acpf_vcu_samsung": "acpf_vcu_metric1,acpf_vcu_metric2",
"aupf_gnb_samsung": "aupf_gnb_metric1,aupf_gnb_metric2",
"aupf_vcu_samsung": "aupf_vcu_metric1,aupf_vcu_metric2",
"aupf_vm_samsung": "aupf_vm_metric1,aupf_vm_metric2"
```

**After (Correct):**
```python
"gnb_samsung": "gnb_s5nr_dlmaclayerdatavolume_mb,gnb_s5nr_totalerabsetupfailurerate_percent",
"sector_samsung": "sector_s5nr_dlmaclayerdatavolume_mb,sector_s5nr_totalerabsetupfailurerate_percent",
"du_samsung": "du_s5nr_dlmaclayerdatavolume_mb,du_s5nr_totalerabsetupfailurerate_percent",
"acpf_gnb_samsung": "gnb_endcaddatt,gnb_endcaddsucc",
"acpf_vcu_samsung": "acpf_cpuusageavg_percent,acpf_memusageavg_percent",
"aupf_gnb_samsung": "gnb_s5nr_totalerabsetupfailurerate_pct,gnb_s5nr_dlmaclayerdatavolume_mb",
"aupf_vcu_samsung": "aupf_cpuusageavg_percent,aupf_memusageavg_percent",
"aupf_vm_samsung": "aupfvminterface_inoctets_vm_aupf,aupfvminterface_outoctets_vm_aupf"
```

## Column Name Details

### Key Differences Found:
1. **Samsung tables (gnb, sector, du)**: Use `_s5nr_` prefix, not `_s5nc_`
2. **Suffix variations**: Some use `_percent`, others use `_pct`
   - `gnb_samsung`: `_percent`
   - `sector_samsung`: `_percent`
   - `du_samsung`: `_percent`
   - `aupf_gnb_samsung`: `_pct` (different table structure)
3. **ACPF/AUPF tables**: Have different column naming patterns
   - `acpf_gnb_samsung`: Uses `gnb_` prefix (not `acpf_gnb_`)
   - `acpf_vcu_samsung`: Uses `acpf_` prefix
   - `aupf_gnb_samsung`: Uses `gnb_` prefix (not `aupf_gnb_`)
   - `aupf_vcu_samsung`: Uses `aupf_` prefix
   - `aupf_vm_samsung`: Uses `aupfvminterface_` prefix

## Files Modified

1. **tests/test_all_tables.py**
   - Updated `SAMPLE_METRICS` dictionary with actual column names from CREATE TABLE statements

## Verification

All column names now match the actual database schema as defined in the CREATE TABLE statements in `apiinitialreq.txt`.

## Testing

After these fixes, the tests should no longer produce "column does not exist" errors. The tests will now use valid column names that exist in the database tables.
