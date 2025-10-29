#!/usr/bin/env python3
"""
Query Data Script
This script demonstrates how to query the loaded JSON data from the database.
"""

from database import SessionLocal
from models import GroupConfiguration, TimeScheduling, Resource, Ericsson5GEnrichment, Samsung5GEnrichment
from sqlalchemy import func

def get_all_groups():
    """Get all group configurations"""
    db = SessionLocal()
    try:
        groups = db.query(GroupConfiguration).all()
        print(f"\nFound {len(groups)} group configurations:")
        print("-" * 80)
        for group in groups[:10]:  # Show first 10
            print(f"Name: {group.group_name}")
            print(f"Type: {group.group_type}")
            print(f"Status: {group.status}")
            print(f"Description: {group.description[:50]}..." if group.description else "No description")
            print("-" * 40)
        
        if len(groups) > 10:
            print(f"... and {len(groups) - 10} more groups")
            
    except Exception as e:
        print(f"ERROR: Error querying groups: {e}")
    finally:
        db.close()

def get_all_schedules():
    """Get all time schedules"""
    db = SessionLocal()
    try:
        schedules = db.query(TimeScheduling).all()
        print(f"\nFound {len(schedules)} time schedules:")
        print("-" * 80)
        for schedule in schedules[:10]:  # Show first 10
            print(f"Name: {schedule.name}")
            print(f"Enabled: {schedule.enabled}")
            print(f"Timezone: {schedule.tz}")
            print(f"Start: {schedule.start}")
            print(f"End: {schedule.end}")
            print("-" * 40)
        
        if len(schedules) > 10:
            print(f"... and {len(schedules) - 10} more schedules")
            
    except Exception as e:
        print(f"ERROR: Error querying schedules: {e}")
    finally:
        db.close()

def get_static_groups():
    """Get all static groups with their resources"""
    db = SessionLocal()
    try:
        static_groups = db.query(GroupConfiguration).filter(
            GroupConfiguration.group_type == 'static'
        ).all()
        
        print(f"\nFound {len(static_groups)} static groups:")
        print("-" * 80)
        
        for group in static_groups:
            print(f"Group: {group.group_name}")
            print(f"Description: {group.description}")
            
            # Get resources for this group
            resources = db.query(Resource).filter(
                Resource.group_configuration_id == group.id
            ).all()
            
            print(f"Resources ({len(resources)}):")
            for resource in resources[:5]:  # Show first 5 resources
                print(f"  - {resource.resource_id} ({resource.resource_type})")
            
            if len(resources) > 5:
                print(f"  ... and {len(resources) - 5} more resources")
            print("-" * 40)
            
    except Exception as e:
        print(f"ERROR: Error querying static groups: {e}")
    finally:
        db.close()

def get_enabled_schedules():
    """Get all enabled time schedules"""
    db = SessionLocal()
    try:
        enabled_schedules = db.query(TimeScheduling).filter(
            TimeScheduling.enabled == True
        ).all()
        
        print(f"\nFound {len(enabled_schedules)} enabled schedules:")
        print("-" * 80)
        for schedule in enabled_schedules:
            print(f"Name: {schedule.name}")
            print(f"Timezone: {schedule.tz}")
            print(f"Frequency: {schedule.frequency}")
            print("-" * 40)
            
    except Exception as e:
        print(f"ERROR: Error querying enabled schedules: {e}")
    finally:
        db.close()

def get_ericsson_5g_data():
    """Get Ericsson 5G Enrichment data"""
    db = SessionLocal()
    try:
        ericsson_data = db.query(Ericsson5GEnrichment).limit(10).all()
        
        print(f"\nFound {len(ericsson_data)} Ericsson 5G Enrichment records (showing first 10):")
        print("-" * 80)
        for record in ericsson_data:
            print(f"Name: {record.name}")
            print(f"Band: {record.band}")
            print(f"Cell Type: {record.trans_cell_type}")
            print(f"Site: {record.primary_site_name}")
            print(f"Admin State: {record.administrative_state}")
            print(f"Operational State: {record.operational_state}")
            print("-" * 40)
            
    except Exception as e:
        print(f"ERROR: Error querying Ericsson 5G data: {e}")
    finally:
        db.close()

def get_samsung_5g_data():
    """Get Samsung 5G Enrichment data"""
    db = SessionLocal()
    try:
        samsung_data = db.query(Samsung5GEnrichment).limit(10).all()
        
        print(f"\nFound {len(samsung_data)} Samsung 5G Enrichment records (showing first 10):")
        print("-" * 80)
        for record in samsung_data:
            print(f"Name: {record.name}")
            print(f"Site: {record.site_name}")
            print(f"Cell Type: {record.trans_cell_type}")
            print(f"gNodeB DUID: {record.gnodeb_duid}")
            print(f"DU Name: {record.du_name}")
            print(f"Band: {record.band}")
            print("-" * 40)
            
    except Exception as e:
        print(f"ERROR: Error querying Samsung 5G data: {e}")
    finally:
        db.close()

def get_band_statistics():
    """Get band statistics from both 5G enrichment tables"""
    db = SessionLocal()
    try:
        # Ericsson band statistics
        ericsson_bands = db.query(
            Ericsson5GEnrichment.band, 
            func.count(Ericsson5GEnrichment.id).label('count')
        ).group_by(Ericsson5GEnrichment.band).all()
        
        # Samsung band statistics
        samsung_bands = db.query(
            Samsung5GEnrichment.band, 
            func.count(Samsung5GEnrichment.id).label('count')
        ).group_by(Samsung5GEnrichment.band).all()
        
        print(f"\nBand Statistics:")
        print("=" * 50)
        print("Ericsson 5G Enrichment:")
        for band, count in ericsson_bands:
            print(f"  {band}: {count}")
        
        print("\nSamsung 5G Enrichment:")
        for band, count in samsung_bands:
            print(f"  {band}: {count}")
        print("=" * 50)
        
    except Exception as e:
        print(f"ERROR: Error getting band statistics: {e}")
    finally:
        db.close()

def get_data_statistics():
    """Get data statistics"""
    db = SessionLocal()
    try:
        # Group statistics
        total_groups = db.query(GroupConfiguration).count()
        dynamic_groups = db.query(GroupConfiguration).filter(
            GroupConfiguration.group_type == 'dynamic'
        ).count()
        static_groups = db.query(GroupConfiguration).filter(
            GroupConfiguration.group_type == 'static'
        ).count()
        active_groups = db.query(GroupConfiguration).filter(
            GroupConfiguration.status == 'ACTIVE'
        ).count()
        
        # Schedule statistics
        total_schedules = db.query(TimeScheduling).count()
        enabled_schedules = db.query(TimeScheduling).filter(
            TimeScheduling.enabled == True
        ).count()
        
        # Resource statistics
        total_resources = db.query(Resource).count()
        
        # 5G Enrichment statistics
        ericsson_count = db.query(Ericsson5GEnrichment).count()
        samsung_count = db.query(Samsung5GEnrichment).count()
        
        print(f"\nData Statistics:")
        print("=" * 50)
        print(f"Total Groups: {total_groups}")
        print(f"  - Dynamic: {dynamic_groups}")
        print(f"  - Static: {static_groups}")
        print(f"  - Active: {active_groups}")
        print(f"Total Schedules: {total_schedules}")
        print(f"  - Enabled: {enabled_schedules}")
        print(f"Total Resources: {total_resources}")
        print(f"Ericsson 5G Enrichment: {ericsson_count}")
        print(f"Samsung 5G Enrichment: {samsung_count}")
        print("=" * 50)
        
    except Exception as e:
        print(f"ERROR: Error getting statistics: {e}")
    finally:
        db.close()

def main():
    """Main query function"""
    print("Data Query Tool")
    print("=" * 50)
    
    # Get data statistics
    get_data_statistics()
    
    # Show sample data
    get_all_groups()
    get_all_schedules()
    get_static_groups()
    get_enabled_schedules()
    
    # Show 5G enrichment data
    get_ericsson_5g_data()
    get_samsung_5g_data()
    get_band_statistics()
    
    print("\nQuery completed!")

if __name__ == "__main__":
    main()
