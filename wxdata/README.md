# Watsonx.data CSV to Iceberg Pipeline

Automated pipeline to ingest CSV files into Watsonx.data using Apache Iceberg tables. This solution automatically detects new CSV files, applies schema management, and makes all data queryable via SQL.

## Architecture

```
CSV Files → Object Storage (MinIO/S3) → watsonx.data → Iceberg Tables → SQL Queries
```

For detailed sequence diagrams showing component interactions, see [SEQUENCE_DIAGRAM.md](SEQUENCE_DIAGRAM.md).

## Features

- **Automatic File Detection**: Monitors landing zone for new CSV files
- **Schema Management**: Supports multiple data formats (carrier and du) with configurable schemas
- **Iceberg Integration**: Creates and manages Iceberg tables for efficient querying
- **Automated Processing**: Processes files in batches and moves them to processed zone
- **Metadata Compaction**: Optional compaction of Iceberg table metadata
- **Modular Design**: Clean, maintainable code structure

## Project Structure

```
wxdata/
├── config.yaml              # Configuration file
├── main.py                  # Entry point
├── requirements.txt         # Python dependencies
├── README.md               # This file
├── src/
│   ├── __init__.py
│   ├── minio_client.py     # MinIO/S3 operations
│   ├── schema_manager.py   # Schema definitions and management
│   ├── iceberg_manager.py  # Iceberg table operations
│   ├── file_processor.py   # File detection and processing
│   └── orchestrator.py     # Main orchestration logic
└── schemas/
    ├── carrier_schema.json # Carrier data format schema
    └── du_schema.json      # DU data format schema
```

## Prerequisites

1. **Python 3.8+**
2. **MinIO** (or S3-compatible storage)
3. **Watsonx.data** with Iceberg support
4. **Apache Spark** (3.5+)
5. **Hive Metastore** (for catalog management)

## Installation

1. Clone or navigate to the project directory:
```bash
cd wxdata
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Install PySpark with Iceberg support:
```bash
# Download Iceberg Spark runtime JAR
# Place it in your Spark jars directory or configure Spark to use it
```

4. Configure `config.yaml` with your environment settings:
   - MinIO endpoint and credentials
   - Watsonx.data catalog and database settings
   - Spark configuration
   - Table definitions

## Configuration

Edit `config.yaml` to match your environment:

### MinIO Configuration
```yaml
minio:
  endpoint: "localhost:9000"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  secure: false
  bucket_name: "samsung-data"
```

### Watsonx.data Configuration
```yaml
watsonx:
  catalog_name: "hive_metastore"
  database_name: "samsung_db"
  warehouse_path: "s3://samsung-data/warehouse"
```

### Spark Configuration
Update Spark configs to point to your Spark cluster and Iceberg JARs.

## Usage

### Single Run Mode
Process all files in the landing zone once:
```bash
python main.py --mode once
```

### Continuous Mode
Continuously monitor and process new files:
```bash
python main.py --mode continuous
```

### Custom Configuration
```bash
python main.py --config custom_config.yaml --mode once
```

## CSV File Structure

Place CSV files in the following structure in MinIO:

```
s3://bucket-name/
└── landing/
    └── Samsung/
        ├── carrier/
        │   └── *.csv
        └── du/
            └── *.csv
```

The pipeline automatically identifies the format based on the path (carrier or du).

## Schema Management

Schemas are defined in JSON files under `schemas/`:

- `carrier_schema.json`: Schema for carrier data format
- `du_schema.json`: Schema for DU data format

Each schema file contains:
```json
{
  "format": "carrier",
  "fields": [
    {"name": "id", "type": "string", "nullable": false},
    {"name": "timestamp", "type": "timestamp", "nullable": false},
    ...
  ]
}
```

Supported types: `string`, `integer`, `double`, `float`, `date`, `timestamp`

## Processing Flow

1. **Detection**: Scans landing zone for new CSV files
2. **Identification**: Identifies format type (carrier/du) from path
3. **Schema Application**: Loads appropriate schema
4. **Table Creation**: Creates Iceberg table if it doesn't exist
5. **Data Ingestion**: Loads CSV data into Iceberg table
6. **File Movement**: Moves processed file to `/processed/` directory
7. **Compaction**: Optionally compacts table metadata

## Querying Data

Once data is loaded, you can query it using SQL:

```sql
-- Query carrier data
SELECT * FROM hive_metastore.samsung_db.carrier_data
WHERE timestamp >= '2024-01-01';

-- Query DU data
SELECT * FROM hive_metastore.samsung_db.du_data
WHERE status = 'active';
```

## Logging

Logs are written to:
- Console (stdout)
- File: `logs/pipeline.log`

Log level can be configured in `config.yaml`.

## Troubleshooting

### Common Issues

1. **Connection to MinIO fails**
   - Check endpoint and credentials in `config.yaml`
   - Ensure MinIO is running and accessible

2. **Spark session creation fails**
   - Verify Spark installation and configuration
   - Check Iceberg JARs are available
   - Review Spark logs for detailed errors

3. **Schema validation errors**
   - Verify CSV files match schema definitions
   - Check schema JSON files are valid
   - Review error logs for specific field mismatches

4. **Table creation fails**
   - Ensure Hive Metastore is running
   - Check database exists and is accessible
   - Verify warehouse path is correct

## Development

### Adding New Formats

1. Create a new schema file in `schemas/` (e.g., `new_format_schema.json`)
2. Add table configuration in `config.yaml`:
```yaml
tables:
  new_format:
    table_name: "new_format_data"
    schema_path: "schemas/new_format_schema.json"
    partition_columns: []
```
3. Update `schema_manager.py` if needed for format identification

### Extending Functionality

The modular design allows easy extension:
- Add new processors in `src/`
- Extend schema manager for additional validation
- Add custom transformations in `file_processor.py`

## License

[Add your license here]

## Support

For issues or questions, please [add contact information or issue tracker link].

