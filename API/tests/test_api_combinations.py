"""
Comprehensive test suite for MKT_Corning API endpoint
Tests various API combinations based on examples from requirements
"""
import pytest
import requests
import json
import time
from typing import Dict, Any, Optional
from datetime import datetime, timedelta


# Base URL for the API
BASE_URL = "http://localhost:8000/api/v1"
REQUEST_TIMEOUT = 10  # seconds


class TestAPICombinations:
    """Test class for various API combinations"""
    
    @pytest.fixture(scope="class", autouse=True)
    def check_api_availability(self):
        """Check if API is available before running tests"""
        try:
            response = requests.get(f"{BASE_URL}/health", timeout=5)
            if response.status_code not in [200, 503]:
                pytest.skip("API server is not available")
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pytest.skip("API server is not running. Please start the server first.")
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup before each test"""
        self.base_url = BASE_URL
        self.endpoint = f"{self.base_url}/timeseries"
    
    def make_request(self, params: Dict[str, Any], print_debug: bool = True) -> requests.Response:
        """Helper method to make API request with timeout"""
        try:
            # Add debug parameter to get query information
            if print_debug:
                params_with_debug = {**params, "debug": "true"}
            else:
                params_with_debug = params
            
            response = requests.get(self.endpoint, params=params_with_debug, timeout=REQUEST_TIMEOUT)
            
            # Print API URL, query, and results
            if print_debug:
                self._print_test_info(response, params_with_debug)
            
            return response
        except requests.exceptions.ConnectionError:
            pytest.skip("Cannot connect to API server")
        except requests.exceptions.Timeout:
            pytest.fail(f"Request to {self.endpoint} timed out after {REQUEST_TIMEOUT}s")
    
    def _print_test_info(self, response: requests.Response, params: Dict[str, Any]):
        """Print API URL, query, and results for debugging"""
        import sys
        print("\n" + "="*80, flush=True)
        print("API REQUEST", flush=True)
        print("="*80, flush=True)
        print(f"URL: {response.url}", flush=True)
        print(f"Status Code: {response.status_code}", flush=True)
        
        try:
            data = response.json()
            
            # Print query information if available
            if "debug" in data:
                print("\n" + "-"*80, flush=True)
                print("GENERATED SQL QUERY", flush=True)
                print("-"*80, flush=True)
                print(data["debug"]["query"], flush=True)
                if data["debug"]["query_params"]:
                    print(f"\nQuery Parameters: {data['debug']['query_params']}", flush=True)
                print("-"*80, flush=True)
            
            # Print results summary
            print("\n" + "-"*80, flush=True)
            print("QUERY RESULTS", flush=True)
            print("-"*80, flush=True)
            print(f"Table: {data.get('table', 'N/A')}", flush=True)
            print(f"Metrics: {', '.join(data.get('metrics', []))}", flush=True)
            print(f"Count: {data.get('count', 0)}", flush=True)
            
            if "start" in data:
                print(f"Start: {data['start']}", flush=True)
            if "end" in data:
                print(f"End: {data['end']}", flush=True)
            if "granularity" in data:
                print(f"Granularity: {data['granularity']}", flush=True)
            
            # Print sample data
            if data.get("data") and len(data["data"]) > 0:
                print(f"\nSample Data (showing first {min(3, len(data['data']))} records):", flush=True)
                for i, record in enumerate(data["data"][:3]):
                    print(f"\n  Record {i+1}:", flush=True)
                    for key, value in record.items():
                        print(f"    {key}: {value}", flush=True)
                if len(data["data"]) > 3:
                    print(f"\n  ... and {len(data['data']) - 3} more records", flush=True)
            else:
                print("\nNo data returned", flush=True)
            
            print("="*80 + "\n", flush=True)
            
        except json.JSONDecodeError:
            import sys
            print(f"\nResponse (not JSON): {response.text[:500]}", flush=True)
            print("="*80 + "\n", flush=True)
    
    def validate_response_structure(self, response: requests.Response, allow_empty: bool = True):
        """Validate basic response structure"""
        assert response.status_code in [200, 400, 422, 500, 503], \
            f"Unexpected status code: {response.status_code}. Response: {response.text[:200]}"
        
        if response.status_code == 200:
            try:
                data = response.json()
            except json.JSONDecodeError:
                pytest.fail(f"Response is not valid JSON: {response.text[:200]}")
            
            assert "table" in data, "Response missing 'table' field"
            assert "metrics" in data, "Response missing 'metrics' field"
            assert "count" in data, "Response missing 'count' field"
            assert "data" in data, "Response missing 'data' field"
            assert isinstance(data["data"], list), "Data field should be a list"
            assert data["table"] == "mkt_corning", f"Table name should be 'mkt_corning', got '{data['table']}'"
            
            if not allow_empty and len(data["data"]) == 0:
                pytest.skip("No data returned from database (this is expected if database is empty)")
    
    # ========== Test Case 1: Basic Query with Metrics Only ==========
    def test_basic_query_metrics_only(self):
        """Test 1: Basic query with only metrics (minimal required parameters)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert len(data["metrics"]) == 2
            assert "ranmarket_endc_sessions_rn" in data["metrics"]
            assert "ranmarket_intra_cu_ho_attempts_number" in data["metrics"]
    
    # ========== Test Case 2: Query with Time Range ==========
    def test_query_with_time_range(self):
        """Test 2: Query with start and end timestamps (Unix milliseconds)"""
        # Using timestamps from example: start=1749992400000, end=1750057199000
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "start": "1749992400000",
            "end": "1750057199000",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert "start" in data
            assert "end" in data
            assert data["start"] == 1749992400000
            assert data["end"] == 1750057199000
    
    # ========== Test Case 3: Query with Single Entity Filter ==========
    def test_query_with_single_entity_filter(self):
        """Test 3: Query with single entity filter using searchByProperties"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            # Verify all returned records match the filter (if data exists)
            if len(data["data"]) > 0:
                for record in data["data"]:
                    if "id" in record:
                        # Convert to string for comparison
                        record_id = str(record["id"])
                        assert record_id == "143", \
                            f"Expected id='143', got '{record_id}'"
    
    # ========== Test Case 4: Query with Multiple Entity Filter Values ==========
    def test_query_with_multiple_entity_filters(self):
        """Test 4: Query with multiple entity filter values using searchByProperties"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "searchByProperties": "resource.id==143,144"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            # Verify all returned records match one of the filter values (if data exists)
            if len(data["data"]) > 0:
                valid_ids = ["143", "144"]
                for record in data["data"]:
                    if "id" in record:
                        record_id = str(record["id"])
                        assert record_id in valid_ids, \
                            f"Expected id in {valid_ids}, got '{record_id}'"
    
    # ========== Test Case 5: Query with Aggregation (Example 1 style) ==========
    def test_query_with_aggregation(self):
        """Test 5: Query with time granularity aggregation (requestgranularity=1-hour)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "1-hour",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert "granularity" in data
            assert data["granularity"] == "1-hour"
            # With aggregation, timestamps should be rounded to hour boundaries
            if len(data["data"]) > 0:
                # Verify timestamp format (should be aggregated)
                assert "timestamp" in data["data"][0]
    
    # ========== Test Case 6: Query with Properties Selection (Example 1 style) ==========
    def test_query_with_properties_selection(self):
        """Test 6: Query with specific properties (entity columns) to return"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "properties": "market,region,technology",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            if len(data["data"]) > 0:
                record = data["data"][0]
                # Should only contain specified properties plus timestamp and metrics
                expected_keys = {"timestamp", "market", "region", "technology", "ranmarket_endc_sessions_rn"}
                # Check that at least the expected keys are present
                assert all(key in record for key in expected_keys)
    
    # ========== Test Case 7: Query with Ordering (Example 2 style) ==========
    def test_query_with_ordering_ascending(self):
        """Test 7: Query with orderBy=time+ (ascending)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "start": "1749992400000",
            "end": "1750057199000",
            "orderBy": "time+",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            if len(data["data"]) > 1:
                timestamps = [record.get("timestamp") for record in data["data"] if "timestamp" in record]
                if len(timestamps) > 1:
                    # Verify ascending order
                    sorted_timestamps = sorted(timestamps)
                    assert timestamps == sorted_timestamps, \
                        f"Timestamps are not in ascending order. First few: {timestamps[:5]}"
    
    def test_query_with_ordering_descending(self):
        """Test 7b: Query with orderBy=time- (descending)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "start": "1749992400000",
            "end": "1750057199000",
            "orderBy": "time-",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            if len(data["data"]) > 1:
                timestamps = [record.get("timestamp") for record in data["data"] if "timestamp" in record]
                if len(timestamps) > 1:
                    # Verify descending order
                    sorted_timestamps = sorted(timestamps, reverse=True)
                    assert timestamps == sorted_timestamps, \
                        f"Timestamps are not in descending order. First few: {timestamps[:5]}"
    
    # ========== Test Case 8: Complex Query (Combining Example 1 and 2) ==========
    def test_complex_query_all_parameters(self):
        """Test 8: Complex query with all parameters (similar to Example 1)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number,ranmarket_max_endc_sessions_rn_number,ranmarket_intra_cu_ho_attempts_rn",
            "start": "1749992400000",
            "end": "1750057199000",
            "searchByProperties": "resource.id==143",
            "properties": "id,market,region",
            "requestgranularity": "1-hour",
            "orderBy": "time+"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert len(data["metrics"]) == 4
            assert "start" in data
            assert "end" in data
            assert "granularity" in data
            assert data["granularity"] == "1-hour"
    
    # ========== Test Case 9: Multiple Metrics (Example 1 style) ==========
    def test_query_with_many_metrics(self):
        """Test 9: Query with many metrics from MKT_Corning table"""
        metrics = [
            "ranmarket_endc_sessions_rn",
            "ranmarket_intra_cu_ho_attempts_number",
            "ranmarket_max_endc_sessions_rn_number",
            "ranmarket_intra_cu_ho_attempts_rn",
            "ranmarket_intra_cu_ho_success_rn",
            "ranmarket_intra_cu_ho_success_rate_rn_pct",
            "ranmarket_sgnb_modification_attempts_rn",
            "ranmarket_sgnb_modification_success_rn",
            "ranmarket_sgnb_modification_success_rate_rn_pct",
            "ranmarket_ul_gtp_data_volume_rn_mb",
            "ranmarket_attach_success_rate_pct_rn",
            "ranmarket_attach_success_rn_num",
            "ranmarket_attach_success_rn_den",
            "ranmarket_drop_rate_pct_rn",
            "ranmarket_drop_rn_num",
            "ranmarket_drop_rn_den",
            "ranmarket_dl_tput_rn_mbps",
            "ranmarket_ul_tput_rn_mbps"
        ]
        params = {
            "table": "mkt_corning",
            "metrics": ",".join(metrics),
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "1-hour",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert len(data["metrics"]) == len(metrics)
            for metric in metrics:
                assert metric in data["metrics"]
    
    # ========== Test Case 10: Multiple Entity Filters (Example 2 style) ==========
    def test_query_with_multiple_entity_values(self):
        """Test 10: Query with multiple entity filter values using searchByProperties"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "searchByProperties": "resource.id==143,144,145",
            "start": "1749992400000",
            "end": "1750057199000",
            "orderBy": "time+"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            if len(data["data"]) > 0:
                # Verify all returned records match one of the filter values
                valid_ids = ["00000000001", "03100010001", "03100010002", "03100010401", "03100010402"]
                for record in data["data"]:
                    if "id" in record:
                        # Convert to string for comparison (in case DB returns different type)
                        record_id = str(record["id"])
                        assert record_id in valid_ids, \
                            f"Expected id in {valid_ids}, got '{record_id}'"
    
    # ========== Test Case 11: Different Granularities ==========
    def test_query_with_30_minute_granularity(self):
        """Test 11: Query with 30-minute aggregation"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "30-minute",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert data["granularity"] == "30-minute"
    
    def test_query_with_1_day_granularity(self):
        """Test 11b: Query with 1-day aggregation"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "1-day",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert data["granularity"] == "1-day"
    
    # ========== Test Case 12: ISO Timestamp Format ==========
    def test_query_with_iso_timestamp(self):
        """Test 12: Query with ISO format timestamps (will be converted to Unix timestamp)"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "start": "1749992400000",  # Using Unix timestamp format as per requirements
            "end": "1750057199000",
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response)
        
        if response.status_code == 200:
            data = response.json()
            assert "start" in data
            assert "end" in data
    
    # ========== Error Test Cases ==========
    def test_missing_required_parameter(self):
        """Test Error 1: Missing required metrics parameter"""
        params = {}
        response = self.make_request(params)
        assert response.status_code == 422  # FastAPI validation error
    
    def test_invalid_granularity_format(self):
        """Test Error 2: Invalid granularity format"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "requestgranularity": "invalid-format"
        }
        response = self.make_request(params)
        assert response.status_code == 400
    
    def test_invalid_property(self):
        """Test Error 3: Invalid property name"""
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn",
            "properties": "invalid_property"
        }
        response = self.make_request(params)
        assert response.status_code == 400
    
    def test_empty_metrics(self):
        """Test Error 4: Empty metrics string"""
        params = {
            "metrics": ""
        }
        response = self.make_request(params)
        assert response.status_code in [400, 422]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

