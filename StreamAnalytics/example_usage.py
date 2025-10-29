"""
Example usage of the Streaming Analytics Loader
This script demonstrates how to use the loader with various query patterns.
"""

from streaming_analytics_loader import StreamingAnalyticsLoader
from config import Config

def main():
    """Demonstrate various usage patterns of the streaming analytics loader."""
    
    # Validate configuration
    if not Config.validate_config():
        print("Configuration validation failed. Please check your .env file or environment variables.")
        print("Run 'python config.py' to create a sample .env file.")
        return
    
    # Initialize loader (will use config from config.py)
    loader = StreamingAnalyticsLoader()
    
    try:
        print("Connecting to database...")
        loader.connect()
        
        print("Creating tables...")
        loader.create_tables()
        
        print("Loading data from JSON...")
        loader.load_all_data('Streaming_analytics.json')
        
        print("\n" + "="*50)
        print("STREAMING ANALYTICS DATA ANALYSIS")
        print("="*50)
        
        # 1. Get all jobs
        all_jobs = loader.get_all_jobs()
        print(f"\n1. Total jobs loaded: {len(all_jobs)}")
        
        # 2. Get jobs by entity
        print("\n2. Jobs by Entity:")
        entities = ['GNB', 'AUPF', 'DU', 'ENB', 'ACPF']
        for entity in entities:
            jobs = loader.get_jobs_by_entity(entity)
            print(f"   {entity}: {len(jobs)} jobs")
        
        # 3. Get jobs by window granularity
        print("\n3. Jobs by Window Granularity:")
        granularities = ['1-minute', '5-minute', '15-minute', '1-hour']
        for gran in granularities:
            jobs = loader.get_jobs_by_window_granularity(gran)
            print(f"   {gran}: {len(jobs)} jobs")
        
        # 4. Get jobs by aggregation type
        print("\n4. Jobs by Aggregation Type:")
        aggregation_types = ['sum', 'Average', 'Max', 'Min']
        for agg_type in aggregation_types:
            jobs = loader.get_jobs_by_aggregation_type(agg_type)
            print(f"   {agg_type}: {len(jobs)} jobs")
        
        # 5. Show detailed metrics for a specific job
        if all_jobs:
            first_job = all_jobs[0]
            print(f"\n5. Detailed metrics for job: {first_job['job_name']}")
            metrics = loader.get_metrics_for_job(first_job['job_name'])
            for metric in metrics:
                print(f"   - {metric['entity']}.{metric['metric_name']} ({metric['aggregation_type']})")
        
        # 6. Show jobs with most metrics
        print("\n6. Jobs with most metrics:")
        job_metric_counts = {}
        for job in all_jobs:
            metrics = loader.get_metrics_for_job(job['job_name'])
            job_metric_counts[job['job_name']] = len(metrics)
        
        # Sort by metric count
        sorted_jobs = sorted(job_metric_counts.items(), key=lambda x: x[1], reverse=True)
        for job_name, count in sorted_jobs[:5]:  # Top 5
            print(f"   {job_name}: {count} metrics")
        
        # 7. Show enabled vs disabled jobs
        enabled_count = sum(1 for job in all_jobs if job['enable_flag'])
        disabled_count = len(all_jobs) - enabled_count
        print(f"\n7. Job Status:")
        print(f"   Enabled: {enabled_count}")
        print(f"   Disabled: {disabled_count}")
        
        # 8. Show time range of jobs
        if all_jobs:
            create_times = [job['create_time'] for job in all_jobs]
            update_times = [job['update_time'] for job in all_jobs]
            
            from datetime import datetime
            oldest_create = min(create_times)
            newest_update = max(update_times)
            
            print(f"\n8. Time Range:")
            print(f"   Oldest job created: {datetime.fromtimestamp(oldest_create/1000)}")
            print(f"   Newest update: {datetime.fromtimestamp(newest_update/1000)}")
        
        print("\n" + "="*50)
        print("ANALYSIS COMPLETE")
        print("="*50)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        loader.disconnect()
        print("\nDatabase connection closed.")

if __name__ == "__main__":
    main()
