"""
Clear processing module for threshold alarms.
This module handles clearing of active alarms based on clear conditions.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from database import DatabaseConnection

logger = logging.getLogger(__name__)

class ClearProcessor:
    """Processes alarm clearing logic for threshold alarms."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize the clear processor with database connection."""
        self.db = db_connection
    
    def get_active_alarms(self) -> List[Dict[str, Any]]:
        """Retrieve all active alarms from the threshold_alarms table."""
        try:
            query = """
            SELECT alarm_id, tablename, metricname, threshold_id, record_id, 
                   record_timestamp, lowerlimit, upperlimit, alarm_severity, 
                   alarm_message, created_at
            FROM threshold_alarms 
            WHERE alarm_status = 'ACTIVE'
            ORDER BY created_at ASC
            """
            
            results = self.db.execute_query(query)
            logger.info(f"Retrieved {len(results)} active alarms")
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving active alarms: {e}")
            raise
    
    def generate_clear_query(self, alarm: Dict[str, Any]) -> str:
        """
        Generate clear query for a specific alarm.
        
        Args:
            alarm: Dictionary containing alarm details
            
        Returns:
            Generated clear query string
        """
        try:
            tablename = alarm.get('tablename', '')
            metricname = alarm.get('metricname', '')
            record_id = alarm.get('record_id', '')
            record_timestamp = alarm.get('record_timestamp')
            lowerlimit = alarm.get('lowerlimit')
            upperlimit = alarm.get('upperlimit')
            alarm_id = alarm.get('alarm_id', '')
            
            # Format timestamp for SQL query
            if isinstance(record_timestamp, str):
                timestamp_str = record_timestamp
            else:
                timestamp_str = record_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            limitclause = f""" """
            columnname = f""" """
            if tablename.lower() == 'ruleexecutionresults':
                columnname = 'udc_config_value'
            else:
                columnname = metricname

            if lowerlimit is not None and lowerlimit > 0:
                limitclause += f""" AND CAST(t.{columnname} AS NUMERIC) < {lowerlimit} """
            if upperlimit is not None and upperlimit > 0:
                limitclause += f""" AND CAST(t.{columnname} AS NUMERIC) > {upperlimit} """
            
            # Construct the query as specified in requirements
            if tablename.lower() == 'ruleexecutionresults':
                clear_query = f"""
                UPDATE threshold_alarms ta
                SET alarm_status = 'CLEARED'
                WHERE alarm_status = 'ACTIVE' AND EXISTS (
                    SELECT 1
                    FROM {tablename} t
                    WHERE t.id = '{record_id}'
                    AND t.timestamp > '{timestamp_str}'
                    {limitclause}
                    AND t.id = ta.record_id    
                    AND t.udc_config_name = ta.metricname
                    AND t.udc_config_name = '{metricname}'
                )
                AND ta.alarm_id = '{alarm_id}'
                AND ta.metricname = '{metricname}'
                """
            else:
                # Build the clear query based on the requirements with proper type casting
                clear_query = f"""
                UPDATE threshold_alarms ta
                SET alarm_status = 'CLEARED'
                WHERE alarm_status = 'ACTIVE' AND EXISTS (
                    SELECT 1
                    FROM {tablename} t
                    WHERE t.id = '{record_id}'
                    AND t.timestamp > '{timestamp_str}'
                    {limitclause}
                    AND t.id = ta.record_id
                )
                AND ta.alarm_id = '{alarm_id}'
                AND ta.metricname = '{metricname}'
                """
            
            logger.debug(f"Generated clear query for alarm {alarm_id}: {clear_query}")
            return clear_query
            
        except Exception as e:
            logger.error(f"Error generating clear query for alarm {alarm.get('alarm_id')}: {e}")
            return None
    
    def execute_clear_query(self, clear_query: str) -> bool:
        """
        Execute the clear query and return success status.
        
        Args:
            clear_query: SQL query to execute
            
        Returns:
            True if query executed successfully, False otherwise
        """
        try:
            self.db.execute_ddl(clear_query)
            logger.info("Clear query executed successfully")
            return True
        except Exception as e:
            logger.error(f"Error executing clear query: {e}")
            return False
    
    def process_alarm_clearing(self) -> Dict[str, Any]:
        """
        Process clearing of all active alarms.
        
        Returns:
            Dictionary containing processing results
        """
        try:
            logger.info("Starting alarm clearing process...")
            
            # Get all active alarms
            active_alarms = self.get_active_alarms()
            
            if not active_alarms:
                logger.info("No active alarms found to process")
                return {
                    'total_alarms': 0,
                    'processed_alarms': 0,
                    'cleared_alarms': 0,
                    'failed_alarms': 0,
                    'errors': []
                }
            
            processed_count = 0
            cleared_count = 0
            failed_count = 0
            errors = []
            
            # Process each alarm
            for alarm in active_alarms:
                try:
                    alarm_id = alarm.get('alarm_id')
                    logger.info(f"Processing alarm: {alarm_id}")
                    
                    # Generate clear query
                    clear_query = self.generate_clear_query(alarm)
                    
                    if clear_query:
                        # Execute clear query
                        success = self.execute_clear_query(clear_query)
                        
                        if success:
                            cleared_count += 1
                            logger.info(f"Alarm {alarm_id} cleared successfully")
                        else:
                            failed_count += 1
                            error_msg = f"Failed to clear alarm {alarm_id}"
                            errors.append(error_msg)
                            logger.error(error_msg)
                    else:
                        failed_count += 1
                        error_msg = f"Failed to generate clear query for alarm {alarm_id}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                    
                    processed_count += 1
                    
                except Exception as e:
                    failed_count += 1
                    error_msg = f"Error processing alarm {alarm.get('alarm_id')}: {e}"
                    errors.append(error_msg)
                    logger.error(error_msg)
                    continue
            
            # Prepare results
            results = {
                'total_alarms': len(active_alarms),
                'processed_alarms': processed_count,
                'cleared_alarms': cleared_count,
                'failed_alarms': failed_count,
                'errors': errors
            }
            
            logger.info(f"Alarm clearing process completed:")
            logger.info(f"  Total alarms: {results['total_alarms']}")
            logger.info(f"  Processed: {results['processed_alarms']}")
            logger.info(f"  Cleared: {results['cleared_alarms']}")
            logger.info(f"  Failed: {results['failed_alarms']}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in alarm clearing process: {e}")
            raise
    
    def get_cleared_alarms_summary(self) -> List[Dict[str, Any]]:
        """Get summary of recently cleared alarms."""
        try:
            query = """
            SELECT alarm_id, threshold_id, tablename, metricname, 
                   record_id, alarm_severity, created_at, updated_at
            FROM threshold_alarms 
            WHERE alarm_status = 'CLEARED'
            ORDER BY updated_at DESC
            LIMIT 50
            """
            
            results = self.db.execute_query(query)
            logger.info(f"Retrieved {len(results)} recently cleared alarms")
            return results
            
        except Exception as e:
            logger.error(f"Error retrieving cleared alarms summary: {e}")
            return []

def main():
    """Main function to run the clear processing."""
    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        logger.info("Starting Clear Processing System")
        logger.info("=" * 50)
        
        # Initialize database connection
        logger.info("Initializing database connection...")
        db = DatabaseConnection()
        
        # Initialize clear processor
        logger.info("Initializing clear processor...")
        processor = ClearProcessor(db)
        
        # Process alarm clearing
        logger.info("Processing alarm clearing...")
        results = processor.process_alarm_clearing()
        
        # Display results
        logger.info("\n" + "=" * 50)
        logger.info("CLEAR PROCESSING RESULTS")
        logger.info("=" * 50)
        logger.info(f"Total Alarms Found: {results['total_alarms']}")
        logger.info(f"Alarms Processed: {results['processed_alarms']}")
        logger.info(f"Alarms Cleared: {results['cleared_alarms']}")
        logger.info(f"Alarms Failed: {results['failed_alarms']}")
        
        if results['errors']:
            logger.info(f"\nErrors encountered:")
            for error in results['errors']:
                logger.info(f"  - {error}")
        
        # Get cleared alarms summary
        if results['cleared_alarms'] > 0:
            logger.info(f"\nRecently cleared alarms:")
            cleared_summary = processor.get_cleared_alarms_summary()
            for alarm in cleared_summary[:10]:  # Show first 10
                logger.info(f"  - {alarm['alarm_id']}: {alarm['tablename']}.{alarm['metricname']} (Severity: {alarm['alarm_severity']})")
        
        logger.info("=" * 50)
        logger.info("Clear Processing System completed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
