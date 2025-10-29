# JSON Data Loader

This application loads JSON data from `Group_configuration.json` and `Time_scheduling.json` files into a PostgreSQL database.

## Features

- ✅ Virtual environment setup
- ✅ PostgreSQL database integration
- ✅ SQLAlchemy ORM models
- ✅ JSON data loading
- ✅ Data validation and error handling
- ✅ Query examples and statistics

## Prerequisites

- Python 3.8+
- PostgreSQL database
- pip (Python package installer)

## Installation

1. **Clone or download this project**

2. **Create and activate virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # On Windows
   # or
   source venv/bin/activate  # On Linux/Mac
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure database:**
   - Update the `.env` file with your PostgreSQL database credentials:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=hierarchy_db
   DB_USER=postgres
   DB_PASSWORD=your_password_here
   ```

5. **Ensure PostgreSQL is running and create the database:**
   ```sql
   CREATE DATABASE hierarchy_db;
   ```

## Usage

### Load Data

Run the main application to load JSON data into the database:

```bash
python main.py
```

This will:
- Test database connection
- Create necessary tables
- Load data from both JSON files
- Display loading statistics

### Query Data

To query and explore the loaded data:

```bash
python query_data.py
```

This will show:
- Data statistics
- Sample group configurations
- Sample time schedules
- Static groups with resources
- Enabled schedules

### Database Setup Only

To only create database tables without loading data:

```bash
python database.py
```

## Database Schema

### Group Configurations Table
- `id`: Primary key
- `group_name`: Unique group name
- `group_type`: 'dynamic' or 'static'
- `condition`: Dynamic group condition
- `description`: Group description
- `status`: Group status (ACTIVE, INACTIVE, etc.)
- `resources`: JSON field for static group resources
- `start_time`, `update_time`: Timestamps
- `created_at`, `updated_at`: Audit timestamps

### Time Schedules Table
- `id`: Primary key
- `name`: Unique schedule name
- `time_period`: JSON array of time periods
- `enabled`: Boolean flag
- `tz`: Timezone
- `start`, `end`: Date strings
- `frequency`, `every_day`: Scheduling parameters
- `created_at`, `updated_at`: Audit timestamps

### Resources Table
- `id`: Primary key
- `resource_id`: Resource identifier
- `tenant`: Resource tenant
- `resource_type`: Type of resource
- `group_configuration_id`: Foreign key to group

## File Structure

```
├── main.py                 # Main application entry point
├── database.py             # Database connection and setup
├── models.py               # SQLAlchemy database models
├── data_loader.py          # JSON data loading logic
├── query_data.py           # Data querying examples
├── requirements.txt        # Python dependencies
├── .env                    # Environment configuration
├── Group_configuration.json # Input JSON data
├── Time_scheduling.json    # Input JSON data
└── README.md              # This file
```

## Troubleshooting

### Database Connection Issues
- Ensure PostgreSQL is running
- Verify database credentials in `.env`
- Check if the database exists
- Ensure the user has proper permissions

### JSON File Issues
- Ensure both JSON files are in the project directory
- Verify JSON files are valid and not corrupted
- Check file permissions

### Python Environment Issues
- Ensure virtual environment is activated
- Verify all dependencies are installed
- Check Python version compatibility

## Data Loading Notes

- Duplicate entries are skipped (based on unique constraints)
- Static groups have their resources stored in a separate table
- All timestamps are preserved from the original JSON
- Data validation ensures data integrity

## Query Examples

### Get all active groups:
```python
from database import SessionLocal
from models import GroupConfiguration

db = SessionLocal()
active_groups = db.query(GroupConfiguration).filter(
    GroupConfiguration.status == 'ACTIVE'
).all()
```

### Get enabled schedules:
```python
from models import TimeScheduling

enabled_schedules = db.query(TimeScheduling).filter(
    TimeScheduling.enabled == True
).all()
```

### Get static groups with resources:
```python
from models import GroupConfiguration, Resource

static_groups = db.query(GroupConfiguration).filter(
    GroupConfiguration.group_type == 'static'
).all()

for group in static_groups:
    resources = db.query(Resource).filter(
        Resource.group_configuration_id == group.id
    ).all()
```

## Support

For issues or questions, please check:
1. Database connection and credentials
2. JSON file format and location
3. Python environment and dependencies
4. Error messages in the console output
