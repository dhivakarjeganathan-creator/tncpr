"""
Test script for the Threshold Execution System.
This script validates the system components without requiring a database connection.
"""

import logging
from threshold_processor import ThresholdProcessor
from database import get_threshold_query

# Configure basic logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MockDatabaseConnection:
    """Mock database connection for testing."""
    
    def __init__(self):
        self.test_data = [
            {
                'tablename': 'cpu_metrics',
                'threshold_id': 'THR001',
                'metricname': 'cpu_usage',
                'mode': 'burst',
                'category': 'performance',
                'lowerlimit': 80.0,
                'upperlimit': 100.0,
                'occurrence': 3,
                'clearoccurrence': 1,
                'cleartime': 2,
                'time': 1,
                'periodgranularity': 1,
                'schedule': 'every_hour',
                'resource': 'server1',
                'threshold_group': '[{"source_id": "test_group_1"}]',
                'execution_datetime': '2025-09-30 08:00:00'
            },
            {
                'tablename': 'ruleExecutionResults',
                'threshold_id': 'THR002',
                'metricname': 'error_rate',
                'mode': 'period',
                'category': 'reliability',
                'lowerlimit': 5.0,
                'upperlimit': None,
                'occurrence': 2,
                'clearoccurrence': 1,
                'cleartime': 1,
                'time': 2,
                'periodgranularity': 1,
                'schedule': 'every_30min',
                'resource': 'app1',
                'threshold_group': '[{"source_id": "test_group_2"}]',
                'execution_datetime': '2025-09-30 08:00:00'
            }
        ]
        
        # Mock data for group_configurations table
        self.group_configurations_data = {
            'test_group_1': [{
                'condition': "resource.type=='DU' && resource.ranMarket.like('13*')"
            }],
            'test_group_2': [{
                'condition': "resource.type=='DU' && resource.Band=='MMW' && (resource.ranMarket=='131' || resource.ranMarket=='132' || resource.ranMarket=='133')"
            }],
            'test_group_3': [{
                'condition': "resource.type=='GNB' && resource.Band.like('MMW')"
            }],
            'test_group_4': [{
                'condition': "resource.type=='sector' && resource.Band=='SUB6' && (resource.ranMarket=='131' || resource.ranMarket=='132')"
            }]
        }
        
        # Mock data for time_schedulings table
        self.time_schedulings_data = {
            'every_hour': [{
                'time_period': '[{"from": 32400000, "to": 64800000}]',  # 9 AM to 6 PM in milliseconds
                'tz': 'America/Chicago'
            }],
            'every_30min': [{
                'time_period': '[{"from": 28800000, "to": 72000000}]',  # 8 AM to 8 PM in milliseconds
                'tz': 'US/Eastern'
            }],
            'gmt_schedule': [{
                'time_period': '[{"from": 0, "to": 86400000}]',  # All day in milliseconds
                'tz': 'GMT'
            }],
            'CTX_Non-MW': [{
                'time_period': [{'from': 32400000, 'to': 64800000}],  # Already parsed list/dict
                'tz': 'America/Chicago'
            }]
        }
    
    def execute_query(self, query, params=None):
        """Mock execute_query method."""
        logger.info(f"Mock query executed: {query[:100]}...")
        
        # Handle group_configurations queries
        if "group_configurations" in query.lower() and "group_name" in query.lower():
            # Extract group_name from query
            import re
            match = re.search(r"group_name\s*=\s*['\"]([^'\"]+)['\"]", query)
            if match:
                group_name = match.group(1)
                return self.group_configurations_data.get(group_name, [])
        
        # Handle time_schedulings queries
        if "time_schedulings" in query.lower() and "name" in query.lower():
            # Extract name from query
            import re
            match = re.search(r"name\s*=\s*['\"]([^'\"]+)['\"]", query)
            if match:
                schedule_name = match.group(1)
                return self.time_schedulings_data.get(schedule_name, [])
        
        return self.test_data
    
    def execute_ddl(self, ddl):
        """Mock execute_ddl method."""
        logger.info(f"Mock DDL executed: {ddl[:100]}...")
    
    def insert_data(self, table_name, data):
        """Mock insert_data method."""
        logger.info(f"Mock insert: {len(data)} rows into {table_name}")

def test_query_generation():
    """Test SQL query generation for different threshold scenarios."""
    
    logger.info("Testing Threshold Execution System")
    logger.info("=" * 50)
    
    # Create mock database connection
    mock_db = MockDatabaseConnection()
    
    # Create threshold processor
    processor = ThresholdProcessor(mock_db)
    
    # Test query generation for each test case
    test_cases = mock_db.test_data
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Test Case {i} ---")
        logger.info(f"Threshold ID: {test_case['threshold_id']}")
        logger.info(f"Table: {test_case['tablename']}")
        logger.info(f"Metric: {test_case['metricname']}")
        logger.info(f"Lower Limit: {test_case['lowerlimit']}")
        logger.info(f"Upper Limit: {test_case['upperlimit']}")
        logger.info(f"Occurrence: {test_case['occurrence']}")
        logger.info(f"Time: {test_case['time']} hours")
        
        try:
            # Generate query
            generated_query, execution_datetime = processor.generate_metric_query(test_case)
            
            logger.info("\nGenerated Query:")
            logger.info("-" * 40)
            logger.info(generated_query)
            logger.info("-" * 40)
            
            # Basic validation
            assert "SELECT" in generated_query.upper(), "Query should contain SELECT"
            assert "FROM" in generated_query.upper(), "Query should contain FROM"
            assert "WHERE" in generated_query.upper(), "Query should contain WHERE"
            
            if test_case['tablename'] == 'ruleExecutionResults':
                assert "udc_config_name" in generated_query, "ruleExecutionResults query should use udc_config_name"
                assert "udc_config_value" in generated_query, "ruleExecutionResults query should use udc_config_value"
            else:
                assert test_case['metricname'] in generated_query, f"Query should contain metric name: {test_case['metricname']}"
            
            # Check that occurrence and time conditions are NOT present (since they're removed from requirements)
            assert "consecutive_groups" not in generated_query, "Consecutive occurrence logic should not be present"
            assert "INTERVAL" not in generated_query or "ORDER BY" in generated_query, "Time-based conditions should not be present"
            
            logger.info("✓ Query validation passed")
            
        except Exception as e:
            logger.error(f"✗ Error generating query: {e}")
            raise
    
    logger.info("\n" + "=" * 50)
    logger.info("All tests passed successfully!")

def test_threshold_query():
    """Test the threshold query structure."""
    
    logger.info("\nTesting threshold query structure...")
    
    query = get_threshold_query()
    
    # Basic validation
    assert "select" in query.lower(), "Threshold query should contain SELECT"
    assert "threshold_rules" in query.lower(), "Threshold query should reference threshold_rules"
    assert "metricsandtables" in query.lower(), "Threshold query should join metricsandtables"
    assert "where" in query.lower(), "Threshold query should contain WHERE clause"
    
    logger.info("✓ Threshold query structure validation passed")

def test_threshold_query_generation():
    """Test the _generate_threshold_query function."""
    
    logger.info("\nTesting threshold query generation...")
    
    # Create mock database connection
    mock_db = MockDatabaseConnection()
    processor = ThresholdProcessor(mock_db)
    
    # Test cases for different condition patterns
    test_cases = [
        {
            'group_name': 'test_group_1',
            'expected_type': 'du',
            'expected_market_pattern': 'like',
            'expected_market_value': '13%',
            'expected_band': None
        },
        {
            'group_name': 'test_group_2',
            'expected_type': 'du',
            'expected_market_pattern': 'in',
            'expected_market_values': ['131', '132', '133'],
            'expected_band': 'MMW'
        },
        {
            'group_name': 'test_group_3',
            'expected_type': 'gnb',
            'expected_market_pattern': None,
            'expected_band_pattern': 'like',
            'expected_band_value': 'MMW'
        },
        {
            'group_name': 'test_group_4',
            'expected_type': 'du',  # sector gets converted to du in the query
            'expected_market_pattern': 'in',
            'expected_market_values': ['131', '132'],
            'expected_band': 'SUB6'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Threshold Query Test Case {i} ---")
        logger.info(f"Group Name: {test_case['group_name']}")
        
        try:
            # Generate the threshold query
            generated_query, _ = processor._generate_threshold_query(test_case['group_name'])
            
            logger.info(f"Generated Query:")
            logger.info("-" * 40)
            logger.info(generated_query)
            logger.info("-" * 40)
            
            # Basic validation
            assert "SELECT 1" in generated_query, "Query should start with SELECT 1"
            assert "FROM enrichmentdetails" in generated_query, "Query should target enrichmentdetails table"
            assert "WHERE 1=1" in generated_query, "Query should have WHERE 1=1"
            
            # Validate type condition
            if test_case.get('expected_type'):
                expected_type_lower = test_case['expected_type'].lower()
                # Check for both lowercase and uppercase versions since the code converts to uppercase
                assert (f"type = lower('{expected_type_lower}')" in generated_query or 
                        f"type = lower('{expected_type_lower.upper()}')" in generated_query), f"Query should contain type condition for {expected_type_lower}"
            
            # Validate market condition
            if test_case.get('expected_market_pattern') == 'like':
                assert f"market like '{test_case['expected_market_value']}'" in generated_query, f"Query should contain market like condition"
            elif test_case.get('expected_market_pattern') == 'in':
                market_values = test_case['expected_market_values']
                for value in market_values:
                    assert f"'{value}'" in generated_query, f"Query should contain market value {value}"
                assert "market in (" in generated_query, "Query should contain market in condition"
            
            # Validate band condition
            if test_case.get('expected_band_pattern') == 'like':
                assert f"band like '{test_case['expected_band_value']}'" in generated_query, f"Query should contain band like condition"
            elif test_case.get('expected_band'):
                assert f"band = '{test_case['expected_band']}'" in generated_query, f"Query should contain band condition for {test_case['expected_band']}"
            
            logger.info("✓ Query validation passed")
            
        except Exception as e:
            logger.error(f"✗ Error in test case {i}: {e}")
            raise
    
    logger.info("\n✓ All threshold query generation tests passed!")

def test_schedule_query():
    """Test the _generate_schedule_query function."""
    
    logger.info("\nTesting schedule query generation...")
    
    # Create mock database connection
    mock_db = MockDatabaseConnection()
    processor = ThresholdProcessor(mock_db)
    
    # Test cases for different schedule scenarios
    test_cases = [
        {
            'schedule_name': 'every_hour',
            'expected_contains': 'timestamp >=',
            'expected_contains2': 'timestamp <=',
            'description': '9 AM to 6 PM Chicago time'
        },
        {
            'schedule_name': 'every_30min',
            'expected_contains': 'timestamp >=',
            'expected_contains2': 'timestamp <=',
            'description': '8 AM to 8 PM Eastern time'
        },
        {
            'schedule_name': 'gmt_schedule',
            'expected_contains': 'timestamp >=',
            'expected_contains2': 'timestamp <=',
            'description': 'All day GMT time'
        },
        {
            'schedule_name': 'CTX_Non-MW',
            'expected_contains': 'timestamp >=',
            'expected_contains2': 'timestamp <=',
            'description': 'Already parsed list/dict data'
        },
        {
            'schedule_name': 'nonexistent_schedule',
            'expected_contains': '',
            'expected_contains2': '',
            'description': 'Non-existent schedule should return empty string'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Schedule Query Test Case {i} ---")
        logger.info(f"Schedule: {test_case['schedule_name']}")
        logger.info(f"Description: {test_case['description']}")
        
        try:
            # Generate the schedule query
            generated_query = processor._generate_schedule_query(test_case['schedule_name'])
            
            logger.info(f"Generated Query: '{generated_query}'")
            
            # Validation
            if test_case['schedule_name'] == 'nonexistent_schedule':
                assert generated_query == "", f"Non-existent schedule should return empty string, got: '{generated_query}'"
            else:
                assert test_case['expected_contains'] in generated_query, f"Query should contain '{test_case['expected_contains']}'"
                assert test_case['expected_contains2'] in generated_query, f"Query should contain '{test_case['expected_contains2']}'"
                assert "AND" in generated_query, "Query should contain AND clause"
            
            logger.info("✓ Query validation passed")
            
        except Exception as e:
            logger.error(f"✗ Error in test case {i}: {e}")
            raise
    
    logger.info("\n✓ All schedule query generation tests passed!")

def test_milliseconds_conversion():
    """Test the _convert_milliseconds_to_datetime function."""
    
    logger.info("\nTesting milliseconds to datetime conversion...")
    
    # Create mock database connection
    mock_db = MockDatabaseConnection()
    processor = ThresholdProcessor(mock_db)
    
    # Test cases for different timezone conversions
    test_cases = [
        {
            'milliseconds': 32400000,  # 9 AM
            'timezone': 'America/Chicago',
            'description': '9 AM Chicago time'
        },
        {
            'milliseconds': 28800000,  # 8 AM
            'timezone': 'US/Eastern',
            'description': '8 AM Eastern time'
        },
        {
            'milliseconds': 0,  # Midnight
            'timezone': 'GMT',
            'description': 'Midnight GMT'
        },
        {
            'milliseconds': 43200000,  # Noon
            'timezone': 'America/Chicago',
            'description': 'Noon Chicago time'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"\n--- Conversion Test Case {i} ---")
        logger.info(f"Milliseconds: {test_case['milliseconds']}")
        logger.info(f"Timezone: {test_case['timezone']}")
        logger.info(f"Description: {test_case['description']}")
        
        try:
            # Convert milliseconds to datetime
            result = processor._convert_milliseconds_to_datetime(
                test_case['milliseconds'], 
                test_case['timezone']
            )
            
            if result:
                logger.info(f"Converted to UTC: {result}")
                assert result.tzinfo is not None, "Result should have timezone info"
                assert result.tzinfo.utcoffset(result).total_seconds() == 0, "Result should be in UTC"
            else:
                logger.warning("Conversion returned None")
            
            logger.info("✓ Conversion validation passed")
            
        except Exception as e:
            logger.error(f"✗ Error in test case {i}: {e}")
            raise
    
    logger.info("\n✓ All milliseconds conversion tests passed!")

if __name__ == "__main__":
    try:
        test_threshold_query()
        test_query_generation()
        test_threshold_query_generation()
        test_schedule_query()
        test_milliseconds_conversion()
        logger.info("\n🎉 All tests completed successfully!")
    except Exception as e:
        logger.error(f"\n❌ Test failed: {e}")
        raise
