## UI Timeseries API

FastAPI service that returns KPI timeseries data in a flattened JSON format, with optional
enrichment fields sourced from `enrichmentlookup`.

### Endpoint

`GET /service/dataset/metric/uitimeseries`

### Query Parameters

- `entityNames` (optional): comma-separated list of entity IDs to filter on `id`.
- `metrics` (required): comma-separated list of metrics (column names).
- `start` (optional): epoch milliseconds start time.
- `end` (optional): epoch milliseconds end time.
- `metricDoubleValue` (optional): if `true`, values are cast to float.
- `fillUpNull` (optional): ignored.
- `sort` (optional): `+timestamp`, `-timestamp`, or `timestamp`.
- `properties` (optional): comma-separated enrichment fields from `enrichmentlookup`
  (ex: `NFName,NFType,Market`).

### Response Shape

Each metric becomes its own row in the response list:

```json
[
  {
    "parent": 0,
    "timestamp": 1768420800000,
    "scope": "ibm-itnm",
    "entityName": "07191022164",
    "parentName": "",
    "date": "2026-01-14",
    "NFName": "abcd",
    "NFType": "adpf",
    "Market": "143",
    "metric": "savg_DU.Samsung.Performance.Sub6.UPNY",
    "entity": 0,
    "time": "08:00 PM",
    "parentId": "",
    "value": 0.0
  }
]
```

### Metrics Lookup

`metricsandtables` is used to resolve which KPI table to query. If multiple tables are returned,
the API runs separate queries per table and merges the results.

### Enrichment Lookup

If `properties` is provided, the API queries `enrichmentlookup` by `entityid` and adds the
requested columns to each response item. See `enrichmentlookup.sql` for the table definition.

### Configuration

Environment variables are loaded from `.env`:

- `DB_TYPE`: `postgresql`, `presto`, or `watsonx.data`
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `DB_SCHEMA` (PostgreSQL): schema for metadata lookups (default `public`)
- `PRESTO_CATALOG`, `PRESTO_SCHEMA` (Presto/Watsonx Data)
- `METRICS_TABLE_NAME`, `METRICS_TABLE_METRIC_COLUMN`, `METRICS_TABLE_TABLE_COLUMN`
- `RULE_EXECUTION_TABLE_NAME`
- `ENRICHMENT_TABLE_NAME`, `ENRICHMENT_TABLE_ENTITY_COLUMN`

### Running

```bash
pip install -r UIAPI/requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8001
```

### Test Script

`UIAPI/test_uitimeseries.py` calls the endpoint and prints the JSON result.
