"""
Threshold query processor for generating SQL queries based on threshold rules.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import uuid
from database import DatabaseConnection, get_threshold_query
import json
import pytz

# Set up logging with new file every time
from logging_config import setup_logging
from datetime import datetime

# Create timestamped log file for this execution
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = setup_logging(
    log_level=logging.INFO,
    log_file=f'logs/threshold_execution_{timestamp}.log',
    include_console=True
)

logger = logging.getLogger(__name__)
logger.info(f"Threshold processor initialized - Log file: {log_file}")

class ThresholdProcessor:
    """Processes threshold rules and generates SQL queries for each threshold job."""
    
    def __init__(self, db_connection: DatabaseConnection):
        """Initialize the threshold processor with database connection."""
        self.db = db_connection
    
    def get_threshold_jobs(self) -> List[Dict[str, Any]]:
        """Retrieve all active threshold jobs from the database."""
        try:
            query = get_threshold_query()
            results = self.db.execute_query(query)
            logger.info(f"Retrieved {len(results)} threshold jobs")
            return results
        except Exception as e:
            logger.error(f"Error retrieving threshold jobs: {e}")
            raise
    
    def _generate_threshold_query(self, thresholdsourceid: str) -> tuple[str, str]:
        """
        Generate SQL query for a specific threshold source id.
        
        Args:
            thresholdsourceid: The threshold source ID to query group_configurations
            
        Returns:
            Generated SQL query string for enrichmentdetails view
        """
        try:
            # First, get the condition from group_configurations
            condition_query = f"""
            SELECT condition
            FROM group_configurations
            WHERE group_name = '{thresholdsourceid}'
            """
            
            logger.info(f"Querying group_configurations for thresholdsourceid: {thresholdsourceid}")
            results = self.db.execute_query(condition_query)
            
            if not results or len(results) == 0:
                logger.warning(f"No condition found for thresholdsourceid: {thresholdsourceid}")
                return ""
            
            condition = results[0].get('condition', '')
            logger.info(f"Retrieved condition: {condition}")
            
            # Parse the condition to extract type, ranMarket, and Band information
            parsed_conditions = self._parse_condition(condition)
            
            # Generate the enrichmentdetails query
            enrichment_query, type = self._build_enrichment_query(parsed_conditions)
            
            logger.info(f"Generated enrichment query for source id: {thresholdsourceid}")
            logger.info(f"Query: {enrichment_query}")
            return enrichment_query, type
            
        except Exception as e:
            logger.error(f"Error generating threshold query for {thresholdsourceid}: {e}")
            return ""
    
    def _parse_condition(self, condition: str) -> Dict[str, Any]:
        """
        Parse the condition string to extract type, ranMarket, and Band information.
        
        Args:
            condition: The condition string from group_configurations
            
        Returns:
            Dictionary containing parsed type, ranMarket, and Band conditions
        """
        import re
        
        parsed = {
            'type': None,
            'ranMarket': None,
            'Band': None
        }
        
        try:
            # Extract type condition
            type_match = re.search(r"resource\.type\s*==\s*['\"]([^'\"]+)['\"]", condition)
            if type_match:
                parsed['type'] = type_match.group(1).lower()
            
            # Extract ranMarket condition
            ran_market_match = re.search(r"resource\.ranMarket\.like\(['\"]([^'\"]+)['\"]\)", condition)
            if ran_market_match:
                # Handle like patterns (e.g., '13*' becomes '13%')
                pattern = ran_market_match.group(1).replace('*', '%')
                parsed['ranMarket'] = {'type': 'like', 'value': pattern}
            else:
                # Handle OR conditions for ranMarket
                or_match = re.search(r"resource\.ranMarket\s*==\s*['\"]([^'\"]+)['\"]", condition)
                if or_match:
                    # Find all ranMarket values in OR conditions
                    ran_markets = re.findall(r"resource\.ranMarket\s*==\s*['\"]([^'\"]+)['\"]", condition)
                    if len(ran_markets) > 1:
                        parsed['ranMarket'] = {'type': 'in', 'values': ran_markets}
                    else:
                        parsed['ranMarket'] = {'type': 'equals', 'value': ran_markets[0]}
            
            # Extract Band condition
            band_match = re.search(r"resource\.Band\.like\(['\"]([^'\"]+)['\"]\)", condition)
            if band_match:
                pattern = band_match.group(1).replace('*', '%')
                parsed['Band'] = {'type': 'like', 'value': pattern}
            else:
                # Handle Band equals conditions
                band_equals_match = re.search(r"resource\.Band\s*==\s*['\"]([^'\"]+)['\"]", condition)
                if band_equals_match:
                    # Find all Band values in OR conditions
                    bands = re.findall(r"resource\.Band\s*==\s*['\"]([^'\"]+)['\"]", condition)
                    if len(bands) > 1:
                        parsed['Band'] = {'type': 'in', 'values': bands}
                    else:
                        parsed['Band'] = {'type': 'equals', 'value': bands[0]}
            
            logger.info(f"Parsed conditions: {parsed}")
            return parsed
            
        except Exception as e:
            logger.error(f"Error parsing condition '{condition}': {e}")
            return parsed
    
    def _build_enrichment_query(self, parsed_conditions: Dict[str, Any]) -> tuple[str, str]:
        """
        Build the enrichmentdetails query based on parsed conditions.
        
        Args:
            parsed_conditions: Dictionary containing parsed type, ranMarket, and Band conditions
            
        Returns:
            Generated SQL query string
        """
        query_parts = ["SELECT 1", "FROM enrichmentdetails b", "WHERE 1=1  and 2=2"]
        
        type = ""
        # Add type condition
        if parsed_conditions.get('type'):
            type =  parsed_conditions['type']
            if parsed_conditions['type'].lower() == 'sector':
                type = 'DU'
            query_parts.append(f"AND type = lower('{type}')")
        
        # Add ranMarket condition
        if parsed_conditions.get('ranMarket'):
            ran_market = parsed_conditions['ranMarket']
            if ran_market['type'] == 'like':
                query_parts.append(f"AND market like '{ran_market['value']}'")
            elif ran_market['type'] == 'in':
                values = "', '".join(ran_market['values'])
                query_parts.append(f"AND market in ('{values}')")
            elif ran_market['type'] == 'equals':
                query_parts.append(f"AND market = '{ran_market['value']}'")
        
        # Add Band condition
        if parsed_conditions.get('Band'):
            band = parsed_conditions['Band']
            if band['type'] == 'like':
                query_parts.append(f"AND band like '{band['value']}'")
            elif band['type'] == 'in':
                values = "', '".join(band['values'])
                query_parts.append(f"AND band in ('{values}')")
            elif band['type'] == 'equals':
                query_parts.append(f"AND band = '{band['value']}'")
        
        return " ".join(query_parts), type

    def _generate_schedule_query(self, schedule: str) -> str:
        """
        Generate SQL query for time scheduling based on schedule name.
        
        Args:
            schedule: The schedule name to query time_schedulings table
            
        Returns:
            WHERE clause string for timestamp filtering in UTC
        """
        try:
            # Query the time_schedulings table
            schedule_query = f"""
            SELECT time_period, tz 
            FROM time_schedulings 
            WHERE name = '{schedule}' AND enabled = true
            """
            
            logger.info(f"Querying time_schedulings for schedule: {schedule}")
            results = self.db.execute_query(schedule_query)
            
            if not results or len(results) == 0:
                logger.warning(f"No schedule found for: {schedule}")
                return ""
            
            schedule_data = results[0]
            time_period_str = schedule_data.get('time_period', '')
            tz_str = schedule_data.get('tz', '')
            
            logger.info(f"Retrieved time_period: {time_period_str}, tz: {tz_str}")
            
            if not time_period_str or not tz_str:
                logger.warning(f"Empty time_period or tz for schedule: {schedule}")
                return ""
            
            # Parse the time_period JSON
            try:
                # Check if time_period_str is already a list/dict or needs to be parsed
                if isinstance(time_period_str, (list, dict)):
                    time_periods = time_period_str
                else:
                    time_periods = json.loads(time_period_str)
            except (json.JSONDecodeError, TypeError) as e:
                logger.error(f"Error parsing time_period JSON: {e}")
                return ""
            
            if not time_periods or len(time_periods) == 0:
                logger.warning(f"No time periods found for schedule: {schedule}")
                return ""
            
            # Get the first time period (assuming single period for now)
            time_period = time_periods[0]
            from_ms = time_period.get('from', 0)
            to_ms = time_period.get('to', 0)
            
            if from_ms == 0 and to_ms == 0:
                logger.warning(f"Invalid time period values for schedule: {schedule}")
                return ""
            
            # Convert milliseconds to datetime objects in the specified timezone
            from_time = self._convert_milliseconds_to_datetime(from_ms, tz_str)
            to_time = self._convert_milliseconds_to_datetime(to_ms, tz_str)
            
            if not from_time or not to_time:
                logger.error(f"Failed to convert time periods to UTC for schedule: {schedule}")
                return ""
            
            # Format the timestamps for SQL
            from_time_str = from_time.strftime('%Y-%m-%d %H:%M:%S')
            to_time_str = to_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Generate the WHERE clause
            where_clause = f" AND timestamp >= '{from_time_str}' AND timestamp <= '{to_time_str}'"
            
            logger.info(f"Generated schedule WHERE clause: {where_clause}")
            return where_clause
            
        except Exception as e:
            logger.error(f"Error generating schedule query for {schedule}: {e}")
            return ""
    
    def _convert_milliseconds_to_datetime(self, milliseconds: int, timezone_str: str) -> Optional[datetime]:
        """
        Convert milliseconds since midnight to datetime in UTC.
        
        Args:
            milliseconds: Milliseconds since midnight
            timezone_str: Timezone string (e.g., 'America/Chicago', 'GMT', 'US/Central')
            
        Returns:
            Datetime object in UTC, or None if conversion fails
        """
        try:
            # Convert milliseconds to hours, minutes, seconds
            total_seconds = milliseconds // 1000
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            
            # Handle case where hours >= 24 (next day)
            if hours >= 24:
                hours = hours % 24
                # Get next day's date
                from datetime import timedelta
                today = datetime.now().date() + timedelta(days=1)
            else:
                # Get current date
                today = datetime.now().date()
            
            # Create datetime object for today with the calculated time
            from datetime import time
            local_time = datetime.combine(today, time(hour=hours, minute=minutes, second=seconds))
            
            # Handle timezone conversion
            try:
                # Try to get the timezone
                if timezone_str.upper() == 'GMT':
                    tz = pytz.timezone('GMT')
                else:
                    tz = pytz.timezone(timezone_str)
                
                # Localize the datetime to the specified timezone
                localized_time = tz.localize(local_time)
                
                # Convert to UTC
                utc_time = localized_time.astimezone(pytz.UTC)
                
                logger.info(f"Converted {milliseconds}ms in {timezone_str} to UTC: {utc_time}")
                return utc_time
                
            except pytz.exceptions.UnknownTimeZoneError:
                logger.error(f"Unknown timezone: {timezone_str}")
                return None
                
        except Exception as e:
            logger.error(f"Error converting milliseconds to datetime: {e}")
            return None

    def generate_metric_query(self, threshold_job: Dict[str, Any]) -> tuple[str, datetime]:
        """
        Generate SQL query for a specific threshold job.
        
        Args:
            threshold_job: Dictionary containing threshold job details
            
        Returns:
            Generated SQL query string
        """
        tablename = threshold_job.get('tablename', '')
        metricname = threshold_job.get('metricname', '')
        lowerlimit = threshold_job.get('lowerlimit')
        upperlimit = threshold_job.get('upperlimit')
        occurrence = threshold_job.get('occurrence')
        time_hours = threshold_job.get('time')
        execution_datetime = threshold_job.get('execution_datetime')
        current_datetime = datetime.now()
        thresholdgroup = threshold_job.get('threshold_group', '')
        schedule = threshold_job.get('schedule', '')

        # generate threshold query if it is not empty for the threshold group
        thresholdquery = ""
        if thresholdgroup != "[]":
            thresholdgroupjson = json.loads(thresholdgroup)
            for group in thresholdgroupjson:
                if group.get('source_id'):
                    thresholdsourceid = group.get('source_id')
                    thresholdquery, type = self._generate_threshold_query(thresholdsourceid)
                    if thresholdquery:
                        thresholdquery = "and exists (" + thresholdquery + ")"
                        # Check if the query contains 'du' or 'sector' type to determine join condition
                        if type and (type.lower() == 'du' or type.lower() == 'sector'):
                            thresholdquery = thresholdquery.replace("2=2", "Left(a.id, 11) = b.fullname")
                        else:
                            thresholdquery = thresholdquery.replace("2=2", "a.id = b.fullname")
                    logger.info(f"Generated threshold query for source id: {thresholdsourceid}")
                    logger.info(f"Threshold query: {thresholdquery}")
                    break
        
        # Generate schedule query if schedule is provided
        schedule_where_clause = ""
        if schedule:
            schedule_where_clause = self._generate_schedule_query(schedule)

        if tablename.lower() == 'ruleexecutionresults':
            return self._generate_rule_execution_query(threshold_job, execution_datetime, current_datetime, thresholdquery, schedule_where_clause), current_datetime
        
        # Build the base query
        query_parts = [
            f"SELECT id, timestamp, {metricname} as metric_value",
            f"FROM {tablename} a",
            f"WHERE created_at >= '{execution_datetime}' and created_at <= '{current_datetime}'"
        ]

        # add threshold query if it is not empty
        if thresholdquery:
            query_parts.append(thresholdquery)
        
        # add schedule query if it is not empty
        if schedule_where_clause:
            query_parts.append(schedule_where_clause)
        
        # Add metric value conditions with proper type casting
        logger.info(f"Lowerlimit: {lowerlimit}, Upperlimit: {upperlimit}")
        conditions = []
        if lowerlimit is not None and lowerlimit > 0:
            conditions.append(f"CAST({metricname} AS NUMERIC) <= {lowerlimit}")
        if upperlimit is not None and upperlimit > 0:
            conditions.append(f"CAST({metricname} AS NUMERIC) >= {upperlimit}")
        
        logger.info(f"Conditions: {conditions}")
        
        if conditions:
            query_parts.append(f"AND ({' AND '.join(conditions)})")
        
        # Add occurrence and time conditions if specified
        # if occurrence is not None or time_hours is not None:
        #     query_parts.append(self._occurrence_check(
        #         metricname, occurrence, time_hours, tablename
        #     ))
        
        # Order by timestamp for consecutive analysis
        query_parts.append("ORDER BY id, timestamp ASC")
        
        return " ".join(query_parts), current_datetime
    
    def _generate_rule_execution_query(self, threshold_job: Dict[str, Any], execution_datetime: datetime, current_datetime: datetime, thresholdquery: str, schedule_where_clause: str = "") -> str:
        """Generate query for ruleexecutionresults table."""
        metricname = threshold_job.get('metricname', '')
        lowerlimit = threshold_job.get('lowerlimit')
        upperlimit = threshold_job.get('upperlimit')
        occurrence = threshold_job.get('occurrence')
        time_hours = threshold_job.get('time')
        
        query_parts = [
            "SELECT \"Id\" as id, timestamp, udc_config_name as metric_name, udc_config_value as metric_value",
            "FROM ruleexecutionresults a",
            f"""WHERE replace(lower(udc_config_name),'.','_') = replace(lower('{metricname}'),'.','_') and created_at >= '{execution_datetime}' and created_at <= '{current_datetime}'""" + thresholdquery.replace("a.id", "a.\"Id\"") + schedule_where_clause
        ]
        
        # Add metric value conditions
        conditions = []
        if lowerlimit is not None and lowerlimit > 0:
            conditions.append(f"CAST(udc_config_value AS NUMERIC) <= {lowerlimit}")
        if upperlimit is not None and upperlimit > 0:
            conditions.append(f"CAST(udc_config_value AS NUMERIC) >= {upperlimit}")
        
        if conditions:
            query_parts.append(f"AND ({' AND '.join(conditions)})")
        
        # Add occurrence and time conditions if specified
        # if occurrence is not None or time_hours is not None:
        #     query_parts.append(self._occurrence_check(
        #         "CAST(udc_config_value AS NUMERIC)", occurrence, time_hours, "ruleExecutionResults"
        #     ))
        
        # Order by timestamp for consecutive analysis
        query_parts.append("ORDER BY id, timestamp ASC")
        
        return " ".join(query_parts)
    
    def _occurrence_check(self, id: str, timestamp: datetime, metric_column: str, occurrence: int, upperlimit: int, lowerlimit: int, tablename: str) -> int:
        """
        Implement consecutive conditions logic with specific query execution.
        
        Args:
            id: ID of the record to check
            timestamp: Timestamp of the record to check
            metric_column: Name of the metric column
            occurrence: Number of consecutive occurrences required
            upperlimit: Upper limit value
            lowerlimit: Lower limit value
            tablename: Name of the table
            
        Returns:
            Count of consecutive occurrences that meet the criteria
        """
        try:
            # Format timestamp for SQL query
            #timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            query = ""
            # Construct the query as specified in requirements
            if tablename.lower() == 'ruleexecutionresults':
                queryclause = f"""
                with ordered as 
                (SELECT 
                    "Id" as id,
                    timestamp,
                    udc_config_value::numeric metricvalue"""
                
                fromclause = f"""
                FROM {tablename} where "Id" = '{id}' and replace(lower(udc_config_name),'.','_') = replace(lower('{metric_column}'),'.','_') and timestamp::timestamp > now()- interval '15 days' )
                """
                finalclause = f"""
                select count(*) cnt from ordered where 1=1 and id = '{id}'  and timestamp = '{timestamp}'
                """
                occselectclause = f"""  """
                occwhereclause = f""" """
                for occ in range(1, occurrence):
                    occselectclause += f"""
                    ,LAG(udc_config_value, {occ}) OVER (PARTITION BY "Id", udc_config_name ORDER BY timestamp) AS prev{occ}
                    """
                    if lowerlimit is not None and lowerlimit > 0:
                        occwhereclause += f""" and prev{occ}::numeric <= {lowerlimit} """
                    if upperlimit is not None and upperlimit > 0:
                        occwhereclause += f""" and prev{occ}::numeric >= {upperlimit} """
            else:
                queryclause = f"""
                with ordered as 
                (SELECT 
                    id,
                    timestamp,
                    {metric_column}::numeric metricvalue"""
                
                fromclause = f"""
                FROM {tablename} where id = '{id}' and timestamp::timestamp > now()- interval '15 days' )
                """
                finalclause = f"""
                select count(*) cnt from ordered where 1=1 and id = '{id}' and timestamp = '{timestamp}'
                """
                occselectclause = f"""  """
                occwhereclause = f""" """
                for occ in range(1, occurrence):
                    occselectclause += f"""
                    ,LAG({metric_column}, {occ}) OVER (PARTITION BY id ORDER BY timestamp) AS prev{occ}
                    """
                    if lowerlimit is not None and lowerlimit > 0:
                        occwhereclause += f""" and prev{occ}::numeric <= {lowerlimit} """
                    if upperlimit is not None and upperlimit > 0:
                        occwhereclause += f""" and prev{occ}::numeric >= {upperlimit} """
                    
                    
            query = queryclause + occselectclause + fromclause + finalclause + occwhereclause
            logger.info(f"Executing consecutive conditions query for id: {id}, timestamp: {timestamp}")
            logger.info(f"Query: {query}")
            
            # Execute the query and get the count
            results = self.db.execute_query(query)
            
            if results and len(results) > 0:
                count = results[0].get('cnt', 0)
                logger.info(f"Consecutive conditions query returned count: {count}")
                return count
            else:
                logger.warning("Consecutive conditions query returned no results")
                return 0
                
        except Exception as e:
            logger.error(f"Error executing consecutive conditions query: {e}")
            return 0
    
    def _save_alarm(self, threshold_job: Dict[str, Any], record_data: Dict[str, Any], occurrence_count: int) -> str:
        """
        Save alarm information to the threshold_alarms table.
        
        Args:
            threshold_job: Dictionary containing threshold job details
            record_data: Dictionary containing the record that triggered the alarm
            occurrence_count: Number of consecutive occurrences that triggered the alarm
            
        Returns:
            Generated alarm ID
        """
        try:
            logger.info(f"Inside Saving alarm for threshold_job: {threshold_job}")
            # Generate unique alarm ID
            alarm_id = f"ALARM_{uuid.uuid4().hex[:8].upper()}"
            
            # Determine alarm severity based on category
            severity_mapping = {
                'critical': 'CRITICAL',
                'major': 'MAJOR', 
                'minor': 'MINOR',
                'warning': 'WARNING'
            }
            alarm_severity = severity_mapping.get(threshold_job.get('category', '').lower(), 'UNKNOWN')
            
            # Create alarm message
            alarm_message = f"Threshold violation detected for {threshold_job.get('metricname')} in {threshold_job.get('tablename')}. "
            alarm_message += f"Record ID: {record_data.get('id')}, Value: {record_data.get('metric_value')}, "
            alarm_message += f"Occurrence Count: {occurrence_count}"
            
            # Prepare alarm data
            alarm_data = {
                'alarm_id': alarm_id,
                'threshold_id': threshold_job.get('threshold_id'),
                'tablename': threshold_job.get('tablename'),
                'metricname': threshold_job.get('metricname'),
                'record_id': str(record_data.get('id')),
                'record_timestamp': record_data.get('timestamp'),
                'metric_value': record_data.get('metric_value'),
                'lowerlimit': threshold_job.get('lowerlimit'),
                'upperlimit': threshold_job.get('upperlimit'),
                'occurrence_count': occurrence_count,
                'alarm_severity': alarm_severity,
                'alarm_status': 'ACTIVE',
                'alarm_message': alarm_message
            }
            
            # Save to database
            self.db.insert_data('threshold_alarms', [alarm_data])
            
            logger.info(f"Alarm saved successfully: {alarm_id}")
            logger.info(f"Alarm details: {alarm_message}")
            
            return alarm_id
            
        except Exception as e:
            logger.error(f"Error saving alarm: {e}")
            return None
    
    def execute_query_and_count(self, query: str) -> tuple[list[Dict[str, Any]], int]:
        """
        Execute a query and return the count of records.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Number of records returned by the query
        """
        try:
            # Execute the query and get results
            results = self.db.execute_query(query)
            record_count = len(results)
            logger.info(f"Query executed successfully, returned {record_count} records")
            return results, record_count
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return [], 0
    
    def process_all_thresholds(self) -> List[Dict[str, Any]]:
        """
        Process all threshold jobs, generate queries, and execute them.
        
        Returns:
            List of dictionaries containing threshold job details, generated queries, and execution results
        """
        try:
            threshold_jobs = self.get_threshold_jobs()
            results = []
            logger.info(f"Inside process_all_thresholds")
            for job in threshold_jobs:
                try:
                    generated_query, execution_datetime = self.generate_metric_query(job)
                        
                    # Execute the query and count records
                    query_results, record_count = self.execute_query_and_count(generated_query) 
                    logger.info(f"Query results: {query_results}")
                    logger.info(f"Record count: {record_count}")
                    if query_results:
                        if job.get('occurrence') and job.get('occurrence') > 0:
                            for row in query_results:
                                rescount = self._occurrence_check(row['id'], row['timestamp'], job.get('metricname'), job.get('occurrence'), job.get('upperlimit'), job.get('lowerlimit'), job.get('tablename'))
                                if rescount > 0:
                                    alarm_id = self._save_alarm(job, row, rescount)
                                    if alarm_id:
                                        logger.info(f"Alarm triggered and saved: {alarm_id}")
                                
                        else:
                            # For records without occurrence check, save alarm directly
                            logger.info(f"Query results Count: {query_results.count}")
                            for row in query_results:
                                logger.info(f"Saving alarm row: {row}")
                                alarm_id = self._save_alarm(job, row, 1)  # occurrence count = 1 for direct threshold violations
                                logger.info(f"Alarm ID: {alarm_id}")
                                if alarm_id:
                                    logger.info(f"Direct threshold alarm saved: {alarm_id}")
                        

                    result = {
                        'threshold_id': job.get('threshold_id'),
                        'tablename': job.get('tablename'),
                        'metricname': job.get('metricname'),
                        'mode': job.get('mode'),
                        'category': job.get('category'),
                        'lowerlimit': job.get('lowerlimit'),
                        'upperlimit': job.get('upperlimit'),
                        'occurrence': job.get('occurrence'),
                        'clearoccurrence': job.get('clearoccurrence'),
                        'cleartime': job.get('cleartime'),
                        'time': job.get('time'),
                        'periodgranularity': job.get('periodgranularity'),
                        'schedule': job.get('schedule'),
                        'resource': job.get('resource'),
                        'threshold_group': job.get('threshold_group'),
                        'generated_sql_query': generated_query,
                        'record_count': record_count,
                        'execution_datetime': execution_datetime
                    }
                    results.append(result)
                    
                    logger.info(f"Generated and executed query for threshold_id: {job.get('threshold_id')} - {record_count} records returned")
                    
                except Exception as e:
                    logger.error(f"Error processing threshold_id {job.get('threshold_id')}: {e}")
                    # Still add the result with 0 record count for failed queries
                    execution_datetime = datetime.now()
                    result = {
                        'threshold_id': job.get('threshold_id'),
                        'tablename': job.get('tablename'),
                        'metricname': job.get('metricname'),
                        'mode': job.get('mode'),
                        'category': job.get('category'),
                        'lowerlimit': job.get('lowerlimit'),
                        'upperlimit': job.get('upperlimit'),
                        'occurrence': job.get('occurrence'),
                        'clearoccurrence': job.get('clearoccurrence'),
                        'cleartime': job.get('cleartime'),
                        'time': job.get('time'),
                        'periodgranularity': job.get('periodgranularity'),
                        'schedule': job.get('schedule'),
                        'resource': job.get('resource'),
                        'threshold_group': job.get('threshold_group'),
                        'generated_sql_query': generated_query if 'generated_query' in locals() else "Query generation failed",
                        'record_count': 0,
                        'execution_datetime': execution_datetime
                    }
                    results.append(result)
                    continue
            
            logger.info(f"Successfully processed {len(results)} threshold jobs")
            return results
            
        except Exception as e:
            logger.error(f"Error processing thresholds: {e}")
            raise
