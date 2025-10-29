# Hierarchy GNB Processor

This Python application processes hierarchy data from an Excel file and stores it in a PostgreSQL database according to specific business rules.

## Features

- **Excel File Processing**: Reads data from `AUPF_ACPF_GNB_Hierarchy.xlsx`
- **Data Transformation**: 
  - Determines type (AUPF/ACPF) based on name field
  - Splits GNB values by comma
  - Adds leading zero to 6-digit GNB values
  - Creates separate rows for each GNB
- **PostgreSQL Integration**: Stores data in a structured table
- **Error Handling**: Comprehensive logging and error management
- **Data Validation**: Ensures data integrity and proper formatting

## Requirements

- Python 3.7+
- PostgreSQL 9.6+
- Required Python packages (see `requirements.txt`)

## Installation

1. **Clone or download the project files**

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PostgreSQL database**:
   - Run the database setup script (optional):
     ```bash
     python setup_database.py
     ```

## Configuration

The application uses environment variables for configuration. You can set them in two ways:

### Option 1: Create a .env file (Recommended)

1. Copy the example environment file:
   ```bash
   cp env.example .env
   ```

2. Edit the `.env` file with your actual database credentials:

```env
# Database Configuration
DB_HOST=localhost
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=your_password_here
DB_PORT=5432

# File Configuration
EXCEL_FILE_PATH=AUPF_ACPF_GNB_Hierarchy.xlsx

# Processing Configuration
CLEAR_EXISTING_DATA=True
```

### Option 2: Set Environment Variables

Set the environment variables directly in your system:

```bash
export DB_HOST=localhost
export DB_NAME=hierarchy_db
export DB_USER=postgres
export DB_PASSWORD=your_password_here
export DB_PORT=5432
export EXCEL_FILE_PATH=AUPF_ACPF_GNB_Hierarchy.xlsx
export CLEAR_EXISTING_DATA=True
```

### Default Values

If no environment variables are set, the application will use these defaults:
- `DB_HOST`: localhost
- `DB_NAME`: hierarchy_db
- `DB_USER`: postgres
- `DB_PASSWORD`: test@1234
- `DB_PORT`: 5432
- `EXCEL_FILE_PATH`: AUPF_ACPF_GNB_Hierarchy.xlsx
- `CLEAR_EXISTING_DATA`: True

## Usage

### Basic Usage

Run the main processor script:

```bash
python hierarchy_processor.py
```

### Custom Usage

Use the processor in your own code:

```python
from hierarchy_processor import HierarchyProcessor
from config import DATABASE_CONFIG

# Create processor instance
processor = HierarchyProcessor(DATABASE_CONFIG)

# Process Excel file
success = processor.process_file('AUPF_ACPF_GNB_Hierarchy.xlsx')
```

### Example Usage

Run the example script:

```bash
python example_usage.py
```

## Database Schema

The application creates a table with the following structure:

```sql
CREATE TABLE hierarchy_gnb (
    id SERIAL PRIMARY KEY,
    type VARCHAR(10) NOT NULL CHECK (type IN ('AUPF', 'ACPF')),
    typeid VARCHAR(255) NOT NULL,
    asm_unique_id VARCHAR(255) NOT NULL,
    gnb TEXT NOT NULL
);
```

## Data Processing Rules

1. **Type Determination**: 
   - If name contains "AUPF" → type = "AUPF"
   - If name contains "ACPF" → type = "ACPF"

2. **GNB Processing**:
   - Split GNB values by comma
   - Add leading zero to 6-digit GNB values
   - Create separate row for each GNB

3. **Data Validation**:
   - Name field cannot be null
   - Type must be determinable from name
   - GNB values are formatted as text

## Input File Format

The Excel file should contain the following columns:
- `name`: Contains AUPF or ACPF identifier
- `asm_unique_id`: Unique identifier for the ASM
- `gnoebId`: Comma-separated list of GNB values

## Output

The processed data is stored in the PostgreSQL `hierarchy_gnb` table with:
- `id`: Auto-incrementing primary key
- `type`: Either "AUPF" or "ACPF"
- `typeid`: Value from the name field
- `asm_unique_id`: ASM unique identifier
- `gnb`: Individual GNB value (formatted)

## Error Handling

The application includes comprehensive error handling:
- Database connection errors
- File reading errors
- Data validation errors
- Transaction rollback on failures

## Logging

All operations are logged with timestamps and appropriate log levels:
- INFO: Normal operations and progress
- ERROR: Errors and failures
- WARNING: Non-critical issues

## Files

- `hierarchy_processor.py`: Main processing class
- `config.py`: Configuration settings
- `setup_database.py`: Database setup script
- `example_usage.py`: Usage example
- `requirements.txt`: Python dependencies
- `README.md`: This documentation

## Troubleshooting

### Common Issues

1. **Database Connection Error**:
   - Verify PostgreSQL is running
   - Check database credentials in `config.py`
   - Ensure database exists

2. **Excel File Not Found**:
   - Verify `AUPF_ACPF_GNB_Hierarchy.xlsx` exists in the project directory
   - Check file permissions

3. **Data Processing Errors**:
   - Check Excel file format and column names
   - Verify data in name field contains "AUPF" or "ACPF"
   - Check for null or invalid values

### Debug Mode

Enable debug logging by modifying the logging level in the script:

```python
logging.basicConfig(level=logging.DEBUG)
```

## License

This project is provided as-is for internal use.
