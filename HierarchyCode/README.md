# Hierarchy Management System

A comprehensive system for managing telecommunications hierarchy data with CRUD operations and CSV import functionality.

## Overview

This system manages a hierarchical structure of telecommunications entities:
- **Region** (highest level)
- **Market** (belongs to regions)
- **GNB** (belongs to markets)
- **DU** (belongs to GNBs)
- **Sector** (belongs to DUs or directly to GNBs)
- **Carrier** (belongs to sectors)

## Features

- ✅ Complete CRUD operations for all entities
- ✅ PostgreSQL database integration
- ✅ CSV file processing with flexible column name matching
- ✅ Audit logging to track processed files
- ✅ Duplicate file processing prevention
- ✅ Hierarchical data validation
- ✅ Interactive command-line interface

## Installation

### Prerequisites

- Python 3.7+
- PostgreSQL 12+
- pip (Python package manager)

### Setup

1. **Clone or download the project files**

2. **Run the setup script:**
   ```bash
   python setup.py
   ```

3. **Configure database:**
   - Update the `.env` file with your PostgreSQL credentials
   - Create a PostgreSQL database named `hierarchy_db`

4. **Install dependencies manually (if setup script fails):**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

### Database Configuration

Create a `.env` file with your database settings:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=your_password_here
CSV_FOLDER_PATH=./csv_files
```

### CSV Column Mapping

The system automatically recognizes these column names (case-insensitive):
- **Region**: `region`, `REGION`
- **Market**: `market`, `MARKET`
- **GNB**: `gnb`, `GNB`
- **DU**: `du`, `DU`
- **Sector**: `sector`, `SECTOR`
- **Carrier**: `carrier`, `CARRIER`

## Usage

### Running the Application

```bash
python main.py
```

### Menu Options

1. **Process CSV files** - Import data from CSV files
2. **Display hierarchy** - Show complete hierarchy structure
3. **Show hierarchy summary** - Display entity counts
4. **Show processing summary** - View CSV processing statistics
5. **Exit** - Close the application

### CSV File Processing

1. Place your CSV files in the `csv_files` folder
2. Ensure column names match the supported formats
3. Use the "Process CSV files" option in the main menu
4. The system will:
   - Validate file format
   - Check for duplicates (using file hash)
   - Import new records
   - Create audit logs

### Example CSV Files

The system includes example CSV files in the `csv_files` folder:

- `regions.csv` - Sample regions
- `markets.csv` - Sample markets with regions
- `gnbs.csv` - Sample GNBs with markets
- `dus.csv` - Sample DUs with GNBs
- `sectors.csv` - Sample sectors with DUs
- `carriers.csv` - Sample carriers with sectors

## Database Schema

### Tables

- `regions` - Top-level regions
- `markets` - Markets within regions
- `gnbs` - GNBs within markets
- `dus` - DUs within GNBs
- `sectors` - Sectors within DUs or GNBs
- `carriers` - Carriers within sectors
- `audit_log` - Processing history and statistics

### Relationships

- Region → Market (1:many)
- Market → GNB (1:many)
- GNB → DU (1:many)
- GNB → Sector (1:many, direct)
- DU → Sector (1:many)
- Sector → Carrier (1:many)

## API Reference

### CRUD Operations

The system provides comprehensive CRUD operations for all entities:

```python
from crud_operations import CRUDOperations
from models import SessionLocal

db = SessionLocal()
crud = CRUDOperations(db)

# Create
region = crud.create_region("North America")

# Read
region = crud.get_region(1)
region = crud.get_region_by_name("North America")

# Update
crud.update_region(1, "North America Updated")

# Delete
crud.delete_region(1)
```

### CSV Processing

```python
from csv_processor import CSVProcessor

processor = CSVProcessor()

# Process single file
result = processor.process_csv_file("path/to/file.csv")

# Process all files in folder
results = processor.process_csv_folder("path/to/folder")
```

## Error Handling

- Database connection errors are logged and handled gracefully
- CSV parsing errors are captured in audit logs
- Duplicate file processing is prevented
- Invalid data is skipped with appropriate logging

## Logging

The system creates detailed logs in `hierarchy_system.log` including:
- Database operations
- CSV processing results
- Error messages
- Performance metrics

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check PostgreSQL is running
   - Verify credentials in `.env` file
   - Ensure database `hierarchy_db` exists

2. **CSV Processing Errors**
   - Check column names match supported formats
   - Ensure CSV files are properly formatted
   - Check file permissions

3. **Import Errors**
   - Verify all required parent entities exist
   - Check for data consistency
   - Review audit logs for specific errors

### Support

For issues or questions:
1. Check the log file: `hierarchy_system.log`
2. Review audit logs in the database
3. Verify configuration settings

## License

This project is provided as-is for educational and development purposes.
