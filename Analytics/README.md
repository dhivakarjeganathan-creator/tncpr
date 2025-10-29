# Analytics Data ETL System

This system loads CSV files containing analytics data into PostgreSQL database tables. It processes 15 different types of analytics data from Samsung, Corning, and Ericsson equipment.

## Features

- **Automated CSV Processing**: Automatically identifies and processes CSV files based on naming patterns
- **Data Validation**: Validates primary keys and filters out invalid records
- **Database Schema Management**: Creates and manages 15 different table schemas
- **CRUD Operations**: Full Create, Read, Update, Delete operations for all tables
- **Error Handling**: Comprehensive error handling and logging
- **Data Cleaning**: Cleans column names and data values for database compatibility

## Table Types

The system processes 15 different table types:

### Samsung Tables
- `MKT_Samsung` - Market level data (Primary Key: Id, Timestamp)
- `GNB_Samsung` - gNB level data (Primary Key: Id, Timestamp)
- `DU_Samsung` - DU level data (Primary Key: Id, Timestamp)
- `SECTOR_Samsung` - Sector level data (Primary Key: Id, Timestamp)
- `CARRIER_Samsung` - Carrier level data (Primary Key: Id, Timestamp)
- `ACPF_Samsung` - ACPF VCU data (Primary Key: Id, Timestamp)
- `AUPF_Samsung` - AUPF VCU data (Primary Key: Id, Timestamp)

### Corning Tables
- `MKT_Corning` - Market level data (Primary Key: Id, Timestamp)
- `GNB_Corning` - gNB level data (Primary Key: Id, Timestamp)
- `SECTOR_Corning` - Sector level data (Primary Key: Id, Timestamp)
- `CARRIER_Corning` - Carrier level data (Primary Key: Id, Timestamp)

### Ericsson Tables
- `MKT_Ericsson` - Market level data (Primary Key: Id, Timestamp)
- `GNB_Ericsson` - gNB level data (Primary Key: Id, Timestamp)
- `SECTOR_Ericsson` - Sector level data (Primary Key: Id, Timestamp)
- `CARRIER_Ericsson` - Carrier level data (Primary Key: Id, Timestamp)

## Installation

1. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Setup PostgreSQL Database**:
   - Install PostgreSQL
   - Create a database named `analytics_db` (or update `db_config.env`)
   - Update database credentials in `db_config.env`

3. **Configure Database Connection**:
   Edit `db_config.env` with your PostgreSQL credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=analytics_db
   DB_USER=postgres
   DB_PASSWORD=your_password
   ```

## Usage

### Basic ETL Process

Run the complete ETL process:
```bash
python main_etl.py
```

### Setup Database Only

Create tables without loading data:
```bash
python main_etl.py --setup-only
```

### Show Database Summary

Display current database status:
```bash
python main_etl.py --summary
```

### Custom Data Directory

Process CSV files from a specific directory:
```bash
python main_etl.py --data-dir /path/to/csv/files
```

### Custom Configuration

Use a different database configuration file:
```bash
python main_etl.py --config /path/to/config.env
```

## File Naming Convention

The system automatically identifies file types based on naming patterns:

- **Samsung Market**: `*_MKT_SAMSUNG_*`
- **Samsung GNB**: `*_GNB_SAMSUNG_*`
- **Samsung DU**: `*_DU_SAMSUNG_*`
- **Samsung Sector**: `*_SECTOR_SAMSUNG_*`
- **Samsung Carrier**: `*_CARRIER_SAMSUNG_*`
- **Samsung VCU (ACPF)**: `12-reports_*_VCU_SAMSUNG_*`
- **Samsung VCU (AUPF)**: `13-reports_*_VCU_SAMSUNG_*`
- **Corning Market**: `*_MKT_CORNING_*`
- **Corning GNB**: `*_GNB_CORNING_*`
- **Corning Sector**: `*_SECTOR_CORNING_*`
- **Corning Carrier**: `*_CARRIER_CORNING_*`
- **Ericsson Market**: `*_MKT_ERICSSON_*`
- **Ericsson GNB**: `*_GNB_ERICSSON_*`
- **Ericsson Sector**: `*_SECTOR_ERICSSON_*`
- **Ericsson Carrier**: `*_CARRIER_ERICSSON_*`

## Data Validation

The system performs the following validations:

1. **Primary Key Validation**: Ensures primary key fields are not empty
2. **Data Type Conversion**: Converts string numbers to appropriate numeric types
3. **Null Value Handling**: Properly handles empty, null, and NaN values
4. **Column Name Cleaning**: Converts column names to database-compatible format

## Database Schema

Each table includes:
- Common columns: Market, Region, VCP Type, Technology, Datacenter, Site, Frequency
- Specific performance metrics based on table type
- Primary key constraints
- Timestamps for data tracking

## Logging

The system generates detailed logs:
- Console output for real-time monitoring
- `etl.log` file for persistent logging
- Error tracking and debugging information

## Error Handling

- Graceful handling of malformed CSV files
- Database connection error recovery
- Data validation error logging
- Transaction rollback on failures

## Performance

- Batch insert operations for efficient data loading
- Connection pooling for database operations
- Memory-efficient CSV processing
- Duplicate record prevention with ON CONFLICT DO NOTHING

## Troubleshooting

### Common Issues

1. **Database Connection Failed**:
   - Check PostgreSQL service is running
   - Verify credentials in `db_config.env`
   - Ensure database exists

2. **No CSV Files Found**:
   - Check file naming convention
   - Verify data directory path
   - Ensure files have `.csv` extension

3. **Permission Denied**:
   - Check file permissions
   - Ensure database user has appropriate privileges

4. **Memory Issues**:
   - Process files in smaller batches
   - Increase system memory
   - Use chunked processing for large files

### Log Analysis

Check `etl.log` for detailed error information:
```bash
tail -f etl.log
```

## Support

For issues or questions:
1. Check the logs for error details
2. Verify database connectivity
3. Ensure CSV files follow naming conventions
4. Check data validation requirements
