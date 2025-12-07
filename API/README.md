# KPI Timeseries API

REST API for querying KPI timeseries data from PostgreSQL or Watsonx Data (Presto). This API provides flexible querying capabilities with support for entity filtering, time range selection, KPI/metric selection, and time-based aggregation.

## Features

- **Multi-database support**: Works with both PostgreSQL and Watsonx Data (Presto)
- **Flexible filtering**: Filter by entity columns (market, region, vcptype, technology, datacenter, site, id)
- **Time range queries**: Filter data by timestamp range
- **KPI selection**: Select specific KPIs/metrics to retrieve
- **Time aggregation**: Aggregate data by time granularity (hour, minute, day)
- **Modular architecture**: Clean, maintainable code structure with proper error handling

## Project Structure

```
.
├── main.py                 # FastAPI application entry point
├── config.py              # Configuration management
├── requirements.txt       # Python dependencies
├── api/
│   ├── __init__.py
│   └── routes.py         # API route definitions
├── database/
│   ├── __init__.py
│   ├── connection.py     # Database connection management
│   └── query_builder.py  # SQL query builder
└── utils/
    ├── __init__.py
    ├── error_handler.py  # Error handling utilities
    └── validators.py     # Input validation utilities
```

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables (or use defaults):
```bash
# Database Configuration
export DB_TYPE=postgresql  # or "presto" for Watsonx Data
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=hierarchy_db
export DB_USER=postgres
export DB_PASSWORD=test@1234

# Presto/Watsonx specific (if using Presto)
export PRESTO_CATALOG=hive
export PRESTO_SCHEMA=your_schema

# API Configuration
export API_HOST=0.0.0.0
export API_PORT=8000
export DEBUG=False
```

## Running the API

```bash
python main.py
```

Or using uvicorn directly:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

## API Documentation

Once the server is running, you can access:
- **Interactive API docs**: http://localhost:8000/docs
- **Alternative docs**: http://localhost:8000/redoc

## API Endpoints

### GET /api/v1/timeseries/mkt_corning

Get MKT_Corning timeseries data with flexible filtering and aggregation.

#### Query Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `metrics` | string | Yes | Comma-separated list of KPI/metric names to retrieve |
| `start` | string | No | Start timestamp (Unix timestamp in milliseconds or ISO format) |
| `end` | string | No | End timestamp (Unix timestamp in milliseconds or ISO format) |
| `market` | string | No | Filter by market (comma-separated for multiple values) |
| `region` | string | No | Filter by region (comma-separated for multiple values) |
| `vcptype` | string | No | Filter by vcptype (comma-separated for multiple values) |
| `technology` | string | No | Filter by technology (comma-separated for multiple values) |
| `datacenter` | string | No | Filter by datacenter (comma-separated for multiple values) |
| `site` | string | No | Filter by site (comma-separated for multiple values) |
| `id` | string | No | Filter by id (comma-separated for multiple values) |
| `properties` | string | No | Comma-separated list of entity columns to return (default: all) |
| `requestgranularity` | string | No | Time granularity for aggregation (e.g., "1-hour", "30-minute", "1-day") |
| `orderBy` | string | No | Order by timestamp: "time" or "time+" for ascending, "time-" for descending (default: ascending) |

#### Example Requests

**Basic query with metrics:**
```
GET /api/v1/timeseries/mkt_corning?metrics=cpu_usage,memory_usage,network_throughput
```

**Query with time range:**
```
GET /api/v1/timeseries/mkt_corning?metrics=cpu_usage&start=1749992400000&end=1750057199000
```

**Query with entity filters:**
```
GET /api/v1/timeseries/mkt_corning?metrics=cpu_usage&market=US&region=East
```

**Query with aggregation:**
```
GET /api/v1/timeseries/mkt_corning?metrics=cpu_usage,memory_usage&start=1749992400000&end=1750057199000&requestgranularity=1-hour
```

**Complex query with multiple filters:**
```
GET /api/v1/timeseries/mkt_corning?metrics=cpu_usage,memory_usage&market=US,EU&region=East&start=1749992400000&end=1750057199000&requestgranularity=1-hour&orderBy=time+
```

#### Response Format

```json
{
  "table": "MKT_Corning",
  "metrics": ["cpu_usage", "memory_usage"],
  "count": 100,
  "start": 1749992400000,
  "end": 1750057199000,
  "granularity": "1-hour",
  "data": [
    {
      "timestamp": "2024-01-01T00:00:00",
      "market": "US",
      "region": "East",
      "vcptype": "Type1",
      "technology": "5G",
      "datacenter": "DC1",
      "site": "Site1",
      "id": "12345",
      "cpu_usage": 45.5,
      "memory_usage": 60.2
    },
    ...
  ]
}
```

### GET /api/v1/health

Health check endpoint to verify API and database connectivity.

**Response:**
```json
{
  "status": "healthy",
  "database": "postgresql",
  "database_connected": true
}
```

## Database Compatibility

The API is designed to work with both PostgreSQL and Presto (Watsonx Data). The query builder automatically generates SQL compatible with the selected database type.

### PostgreSQL
- Uses `psycopg2` for connections
- Supports parameterized queries
- Uses `DATE_TRUNC` for time aggregation
- Uses `TO_TIMESTAMP` for timestamp conversion

### Presto (Watsonx Data)
- Uses `pyhive` for connections
- Uses string interpolation with proper escaping
- Uses `DATE_TRUNC` for time aggregation
- Uses `FROM_UNIXTIME` for timestamp conversion

## Error Handling

The API includes comprehensive error handling:
- **Validation errors** (400): Invalid input parameters
- **Database errors** (500): Database connection or query execution failures
- **Not found errors** (404): Resource not found
- **Generic errors** (500): Unexpected errors with safe error messages

All errors return JSON responses with descriptive error messages.

## Security Considerations

- Input validation to prevent SQL injection
- Column and table name sanitization
- Parameterized queries for PostgreSQL
- Proper string escaping for Presto

## Future Enhancements

- Support for remaining 17 tables (GNB_Corning, SECTOR_Corning, etc.)
- Schema introspection to automatically detect KPI columns
- Caching layer for frequently accessed data
- Rate limiting
- Authentication and authorization
- Batch query support

## License

[Add your license here]

