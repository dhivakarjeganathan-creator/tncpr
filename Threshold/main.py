"""
Main application to process threshold definitions and populate database.
"""

import os
import sys
from database_schema import ThresholdDatabase
from threshold_parser import ThresholdParser
from config import Config, create_env_template

def main():
    """Main application entry point."""
    print("=== Threshold Rules Processor (PostgreSQL) ===")
    
    # Validate configuration
    if not Config.validate_db_config():
        print("\nPlease set up your database configuration:")
        print("1. Copy .env.template to .env")
        print("2. Update .env with your PostgreSQL credentials")
        print("3. Run the application again")
        return 1
    
    # Configuration
    json_file = Config.JSON_FILE
    db_config = Config.get_db_config()
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"Error: JSON file '{json_file}' not found.")
        print("Please ensure the Threshold_definitions.json file is in the current directory.")
        return 1
    
    try:
        # Initialize database
        print(f"\n1. Initializing PostgreSQL database: {db_config['database']}")
        db = ThresholdDatabase(db_config)
        db.connect()
        db.create_table()
        
        # Initialize parser
        print(f"\n2. Loading JSON file: {json_file}")
        parser = ThresholdParser(json_file)
        parser.load_json()
        
        # Display threshold information
        threshold_info = parser.get_threshold_info()
        print(f"\nThreshold Information:")
        print(f"  Name: {threshold_info['name']}")
        print(f"  Metric: {threshold_info['metric']}")
        print(f"  Owner: {threshold_info['owner']}")
        print(f"  Thresholds: {threshold_info['threshold_count']}")
        print(f"  Evaluations: {threshold_info['evaluation_count']}")
        
        # Process evaluations and extract rules
        print(f"\n3. Processing evaluations...")
        rules = parser.process_evaluations()
        
        if not rules:
            print("No rules extracted from the JSON file.")
            return 1
        
        print(f"Extracted {len(rules)} threshold rules.")
        
        # Insert rules into database
        print(f"\n4. Inserting rules into database...")
        inserted_count = 0
        
        for rule in rules:
            try:
                rule_id = db.insert_threshold_rule(rule)
                inserted_count += 1
                print(f"  Inserted rule ID {rule_id}: {rule['category']} {rule['mode']}")
            except Exception as e:
                print(f"  Error inserting rule: {e}")
        
        print(f"\nSuccessfully inserted {inserted_count} rules into the database.")
        
        # Display summary
        print(f"\n5. Database Summary:")
        all_rules = db.get_all_rules()
        print(f"  Total rules in database: {len(all_rules)}")
        
        # Display sample data
        if all_rules:
            print(f"\nSample data:")
            for rule in all_rules[:3]:  # Show first 3 rules
                print(f"  ID: {rule['threshold_id']}, "
                      f"Category: {rule['category']}, "
                      f"Mode: {rule['mode']}, "
                      f"Lower: {rule['lowerlimit']}, "
                      f"Upper: {rule['upperlimit']}")
        
        # Close database connection
        db.close()
        print(f"\nProcessing completed successfully!")
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

def create_sample_json():
    """Create a sample JSON file for testing."""
    sample_data = {
        "name": "ACPF.Daily.CpuUsageMax.percent",
        "creation_time": 1727173986963,
        "target_rule": "omni_rule",
        "tag": "",
        "user_groups": "",
        "resource": [],
        "evaluations": [
            {
                "burst_major_lower_limit": "0.0",
                "burst_major_clear_time": 0,
                "burst_major_upper_limit": "0.0",
                "active_until": "No end date",
                "period_critical_lower_limit": "80.0",
                "period_critical_time": 10800,
                "burst_critical_lower_limit": "0.0",
                "period_granularity": 1,
                "burst_warning_mode": 0,
                "burst_minor_occurrence": 3,
                "burst_critical_enabled": True,
                "burst_critical_time": 0,
                "period_critical_enabled": True,
                "period_critical_mode": 0,
                "burst_warning_clear_time": 0,
                "period_critical_mode": 0,
                "burst_warning_clear_time": 0,
                "period_warning_enabled": False,
                "period_warning_upper_limit": "0.0",
                "period_major_lower_limit": "0.0",
                "burst_critical_upper_limit": "0.0",
                "period_minor_time": 0,
                "burst_warning_enabled": False,
                "period_major_enabled": False,
                "period_minor_mode": 0,
                "burst_minor_clear_time": 0,
                "burst_minor_lower_limit": "0.0",
                "burst_warning_lower_limit": "0.0",
                "burst_major_clear_occurrence": 0,
                "burst_warning_upper_limit": "0.0",
                "burst_critical_mode": 0,
                "burst_minor_clear_occurrence": 0,
                "burst_minor_upper_limit": "0.0",
                "burst_warning_time": 0,
                "burst_warning_occurrence": 0,
                "burst_minor_mode": 0,
                "period_major_mode": 0,
                "period_generate_event": True,
                "period_major_upper_limit": "0.0",
                "burst_generate_event": True,
                "burst_enabled": True,
                "burst_critical_clear_time": 0,
                "period_minor_lower_limit": "0.0",
                "burst_critical_occurrence": 0,
                "burst_warning_clear_occurrence": 0,
                "schedule": "",
                "burst_major_time": 0,
                "period_critical_upper_limit": "0.0",
                "schedule_desc": "Always",
                "period_major_time": 0,
                "period_warning_time": 0,
                "burst_minor_enabled": False,
                "baseline_enabled": False,
                "burst_major_enabled": False,
                "period_enabled": False,
                "period_minor_enabled": False,
                "burst_major_mode": 0,
                "burst_reset_time": 0,
                "period_warning_lower_limit": "0.0",
                "burst_critical_clear_occurrence": 0,
                "period_minor_upper_limit": "0.0",
                "burst_minor_time": 0,
                "burst_major_occurrence": 0,
                "period_warning_mode": 0
            }
        ],
        "threshold_group": [],
        "metric": "ACPF.Daily.CpuUsageMax.percent",
        "can_edit": True,
        "owner": "icpadmin",
        "update_time": 1727173986963
    }
    
    import json
    with open("Threshold_definitions.json", 'w', encoding='utf-8') as f:
        json.dump(sample_data, f, indent=4)
    print("Sample JSON file 'Threshold_definitions.json' created.")

if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--create-sample":
            create_sample_json()
        elif sys.argv[1] == "--create-env":
            create_env_template()
        else:
            print("Usage: python main.py [--create-sample|--create-env]")
            sys.exit(1)
    else:
        exit_code = main()
        sys.exit(exit_code)
