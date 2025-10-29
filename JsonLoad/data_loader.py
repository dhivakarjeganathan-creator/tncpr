import json
import os
import pandas as pd
from sqlalchemy.orm import sessionmaker
from database import engine, test_connection
from models import GroupConfiguration, TimeScheduling, Resource, Ericsson5GEnrichment, Samsung5GEnrichment
from datetime import datetime
import time

# Create session
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def load_group_configurations():
    """Load group configuration data from JSON file"""
    try:
        with open('Group_configuration.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        db = SessionLocal()
        loaded_count = 0
        
        for item in data:
            # Check if group already exists
            existing_group = db.query(GroupConfiguration).filter(
                GroupConfiguration.group_name == item['groupName']
            ).first()
            
            if existing_group:
                print(f"WARNING: Group '{item['groupName']}' already exists, skipping...")
                continue
            
            # Create group configuration
            group_config = GroupConfiguration(
                condition=item.get('condition', ''),
                description=item.get('description', ''),
                group_by=item.get('groupBy', ''),
                group_name=item['groupName'],
                group_type=item['groupType'],
                relation=item.get('relation', ''),
                resources=item.get('resources', ''),
                start_time=item.get('startTime'),
                status=item.get('status', ''),
                update_time=item.get('updateTime')
            )
            
            db.add(group_config)
            db.flush()  # Get the ID
            
            # Handle resources for static groups
            if item.get('groupType') == 'static' and item.get('resources'):
                for resource_data in item['resources']:
                    resource = Resource(
                        resource_id=resource_data['id'],
                        tenant=resource_data.get('tenant', ''),
                        resource_type=resource_data.get('type', ''),
                        group_configuration_id=group_config.id
                    )
                    db.add(resource)
            
            loaded_count += 1
        
        db.commit()
        print(f"SUCCESS: Successfully loaded {loaded_count} group configurations")
        return True
        
    except Exception as e:
        print(f"ERROR: Error loading group configurations: {e}")
        return False
    finally:
        db.close()

def load_time_schedulings():
    """Load time scheduling data from JSON file"""
    try:
        with open('Time_scheduling.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        db = SessionLocal()
        loaded_count = 0
        
        for item in data:
            # Check if schedule already exists
            existing_schedule = db.query(TimeScheduling).filter(
                TimeScheduling.name == item['name']
            ).first()
            
            if existing_schedule:
                print(f"WARNING: Schedule '{item['name']}' already exists, skipping...")
                continue
            
            # Create time scheduling
            time_schedule = TimeScheduling(
                name=item['name'],
                time_period=item.get('time_period', []),
                rank_of_week=item.get('rank_of_week'),
                enabled=item.get('enabled', True),
                tz=item.get('tz', ''),
                rank_of_week_day=item.get('rank_of_week_day'),
                day_of_month=item.get('day_of_month'),
                end=item.get('end', ''),
                frequency=item.get('frequency'),
                every_day=item.get('every_day'),
                start=item.get('start', ''),
                day=item.get('day'),
                day_of_month_type=item.get('day_of_month_type'),
                month=item.get('month')
            )
            
            db.add(time_schedule)
            loaded_count += 1
        
        db.commit()
        print(f"SUCCESS: Successfully loaded {loaded_count} time schedules")
        return True
        
    except Exception as e:
        print(f"ERROR: Error loading time schedules: {e}")
        return False
    finally:
        db.close()

def load_all_data():
    """Load all JSON and CSV data into the database"""
    print("Starting data loading process...")
    
    # Test database connection first
    if not test_connection():
        print("ERROR: Database connection failed. Please check your configuration.")
        return False
    
    print("\nLoading Group Configurations...")
    group_success = load_group_configurations()
    
    print("\nLoading Time Schedules...")
    time_success = load_time_schedulings()
    
    print("\nLoading Ericsson 5G Enrichment...")
    time.sleep(20)
    #ericsson_success = load_ericsson_5g_enrichment()
    
    print("\nLoading Samsung 5G Enrichment...")
    samsung_success = load_samsung_5g_enrichment()
    
    # if group_success and time_success and ericsson_success and samsung_success:
    #     print("\nSUCCESS: All data loaded successfully!")
    #     return True
    # else:
    #     print("\nERROR: Some data failed to load. Please check the errors above.")
    #     return False
    return True

def load_ericsson_5g_enrichment():
    """Load Ericsson 5G Enrichment data from CSV file"""
    try:
        print("Loading Ericsson 5G Enrichment data...")
        # Read CSV with name as string to preserve leading zeros
        df = pd.read_csv('Ericsson-5G-Enrichment.csv', dtype={'name': str})
        
        db = SessionLocal()
        loaded_count = 0
        batch_size = 1000  # Process in batches for large files
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            for _, row in batch_df.iterrows():
                # Check if record already exists
                existing_record = db.query(Ericsson5GEnrichment).filter(
                    Ericsson5GEnrichment.name == row['name']
                ).first()
                
                if existing_record:
                    continue  # Skip duplicates
                
                # Create new record
                ericsson_record = Ericsson5GEnrichment(
                    name=str(row['name']),  # Convert to string
                    band=str(row.get('Band', '')),
                    trans_cell_type=str(row.get('transCellType', '')),
                    primary_site_name=str(row.get('PrimarySite_Name', '')),
                    administrative_state=str(row.get('administrativeState', '')),
                    operational_state=str(row.get('operationalState', ''))
                )
                
                db.add(ericsson_record)
                loaded_count += 1
            
            # Commit batch
            db.commit()
            print(f"Processed {min(i + batch_size, len(df))} records...")
        
        print(f"SUCCESS: Successfully loaded {loaded_count} Ericsson 5G Enrichment records")
        return True
        
    except Exception as e:
        print(f"ERROR: Error loading Ericsson 5G Enrichment data: {e}")
        return False
    finally:
        db.close()

def load_samsung_5g_enrichment():
    """Load Samsung 5G Enrichment data from CSV file"""
    try:
        print("Loading Samsung 5G Enrichment data...")
        # Read CSV with gNodeBDUID as string to preserve leading zeros
        df = pd.read_csv('Samsung-5G-Enrichment.csv', dtype={'gNodeBDUID': str})
        
        db = SessionLocal()
        loaded_count = 0
        batch_size = 1000  # Process in batches for large files
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            for _, row in batch_df.iterrows():
                # Check if record already exists
                existing_record = db.query(Samsung5GEnrichment).filter(
                    Samsung5GEnrichment.name == row['name']
                ).first()
                
                if existing_record:
                    continue  # Skip duplicates
                
                # Create new record
                samsung_record = Samsung5GEnrichment(
                    name=str(row['name']),  # Convert to string
                    site_name=str(row.get('Site_Name', '')),
                    trans_cell_type=str(row.get('transCellType', '')),
                    gnodeb_duid=str(row.get('gNodeBDUID', '')),  # Convert to string
                    du_name=str(row.get('duName', '')),
                    band=str(row.get('Band', ''))
                )
                
                db.add(samsung_record)
                loaded_count += 1
            
            # Commit batch
            db.commit()
            print(f"Processed {min(i + batch_size, len(df))} records...")
        
        print(f"SUCCESS: Successfully loaded {loaded_count} Samsung 5G Enrichment records")
        return True
        
    except Exception as e:
        print(f"ERROR: Error loading Samsung 5G Enrichment data: {e}")
        return False
    finally:
        db.close()

def get_data_summary():
    """Get summary of loaded data"""
    db = SessionLocal()
    try:
        group_count = db.query(GroupConfiguration).count()
        time_count = db.query(TimeScheduling).count()
        resource_count = db.query(Resource).count()
        ericsson_count = db.query(Ericsson5GEnrichment).count()
        samsung_count = db.query(Samsung5GEnrichment).count()
        
        print(f"\nData Summary:")
        print(f"   Group Configurations: {group_count}")
        print(f"   Time Schedules: {time_count}")
        print(f"   Resources: {resource_count}")
        print(f"   Ericsson 5G Enrichment: {ericsson_count}")
        print(f"   Samsung 5G Enrichment: {samsung_count}")
        
    except Exception as e:
        print(f"ERROR: Error getting data summary: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    load_all_data()
    get_data_summary()
