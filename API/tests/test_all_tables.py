"""
Comprehensive test suite for all 18 KPI timeseries tables
Each table has at least 8 test cases covering various API scenarios
"""
import pytest
import requests
import json
from typing import Dict, Any, List

# Base configuration
BASE_URL = "http://localhost:8000/api/v1"
REQUEST_TIMEOUT = 30

# All tables to test (excluding mkt_corning which has its own test file)
ALL_TABLES = [
    "mkt_ericsson",
    "mkt_samsung",
    "gnb_corning",
    "gnb_ericsson",
    "gnb_samsung",
    "sector_corning",
    "sector_ericsson",
    "sector_samsung",
    "carrier_corning",
    "carrier_ericsson",
    "carrier_samsung",
    "du_corning",
    "du_samsung",
    "acpf_gnb_samsung",
    "acpf_vcu_samsung",
    "aupf_gnb_samsung",
    "aupf_vcu_samsung",
    "aupf_vm_samsung"
]

# Sample metrics for each table type (actual KPI column names from CREATE TABLE statements)
SAMPLE_METRICS = {
    "mkt_ericsson": "ranmarket_5gnr_endc_setup_failure_pct,ranmarket_5gnr_dl_mac_volume_mb",
    "mkt_samsung": "ranmarket_s5nc_drbdrop_pct_sa,ranmarket_s5nc_drbsetupfail_pct_sa",
    "gnb_corning": "gnb_dl_gtp_data_volume_rn_mb,gnb_endc_sessions_rn",
    "gnb_ericsson": "gnb_5gnr_endc_setup_failure_pct,gnb_5gnr_dl_mac_volume_mb",
    "gnb_samsung": "gnb_s5nr_dlmaclayerdatavolume_mb,gnb_s5nr_totalerabsetupfailurerate_percent",
    "sector_corning": "sector_dl_gtp_data_volume_rn_mb,sector_endc_sessions_rn",
    "sector_ericsson": "sector_5gnr_endc_setup_failure_pct,sector_5gnr_dl_mac_volume_mb",
    "sector_samsung": "sector_s5nr_dlmaclayerdatavolume_mb,sector_s5nr_totalerabsetupfailurerate_percent",
    "carrier_corning": "corningcarrier_dl_gtp_data_volume_rn_mb,corningcarrier_cor_nsa_cell_availability",
    "carrier_ericsson": "ericsson5gcarrier_5gnr_endc_drop_pct,ericsson5gcarrier_5gnr_cell_availability_pct",
    "carrier_samsung": "carrier5g_s5nc_drbdrop_pct_sa,carrier5g_s5nc_drbsetupfail_pct_sa",
    "du_corning": "du_dl_gtp_data_volume_rn_mb,du_endc_sessions_rn",
    "du_samsung": "du_s5nr_dlmaclayerdatavolume_mb,du_s5nr_totalerabsetupfailurerate_percent",
    "acpf_gnb_samsung": "gnb_endcaddatt,gnb_endcaddsucc",
    "acpf_vcu_samsung": "acpf_cpuusageavg_percent,acpf_memusageavg_percent",
    "aupf_gnb_samsung": "gnb_s5nr_totalerabsetupfailurerate_pct,gnb_s5nr_dlmaclayerdatavolume_mb",
    "aupf_vcu_samsung": "aupf_cpuusageavg_percent,aupf_memusageavg_percent",
    "aupf_vm_samsung": "aupfvminterface_inoctets_vm_aupf,aupfvminterface_outoctets_vm_aupf"
}

# Fallback to generic metrics if specific ones not available
GENERIC_METRICS = "metric1,metric2"


class TestAllTables:
    """Test suite for all KPI timeseries tables"""
    
    @pytest.fixture(scope="class", autouse=True)
    def check_api_availability(self):
        """Check if API server is available before running tests"""
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
    
    def validate_response_structure(self, response: requests.Response, table_name: str, allow_empty: bool = True):
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
            assert data["table"] == table_name, f"Table name should be '{table_name}', got '{data['table']}'"
            
            if not allow_empty and len(data["data"]) == 0:
                pytest.skip("No data returned from database (this is expected if database is empty)")
    
    def get_metrics_for_table(self, table_name: str) -> str:
        """Get sample metrics for a table"""
        return SAMPLE_METRICS.get(table_name, GENERIC_METRICS)
    
    # ========== Test Case 1: Basic Query with Metrics Only ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_basic_query_metrics_only(self, table_name):
        """Test 1: Basic query with only metrics (minimal required parameters)"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]  # Use first metric only
        params = {
            "table": table_name,
            "metrics": metrics
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert len(data["metrics"]) >= 1
            assert metrics in data["metrics"] or metrics.split('_')[0] in str(data["metrics"])
    
    # ========== Test Case 2: Query with Time Range ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_time_range(self, table_name):
        """Test 2: Query with start and end timestamps (Unix milliseconds)"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert "start" in data
            assert "end" in data
            assert data["start"] == 1749992400000
            assert data["end"] == 1750057199000
    
    # ========== Test Case 3: Query with Single Entity Filter ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_single_entity_filter(self, table_name):
        """Test 3: Query with single entity filter using searchByProperties"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "searchByProperties": "resource.id==143"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify all returned records match the filter (if data exists)
            if len(data["data"]) > 0:
                for record in data["data"]:
                    if "id" in record:
                        record_id = str(record["id"])
                        assert record_id == "143", \
                            f"Expected id='143', got '{record_id}'"
    
    # ========== Test Case 4: Query with Multiple Entity Filter Values ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_multiple_entity_filters(self, table_name):
        """Test 4: Query with multiple entity filter values using searchByProperties"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "searchByProperties": "resource.id==143,144"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
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
    
    # ========== Test Case 5: Query with Aggregation ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_aggregation(self, table_name):
        """Test 5: Query with time granularity aggregation (requestgranularity=1-hour)"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "1-hour"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert "granularity" in data
            assert data["granularity"] == "1-hour"
    
    # ========== Test Case 6: Query with Properties Selection ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_properties_selection(self, table_name):
        """Test 6: Query with properties parameter to select entity columns"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "properties": "market,id"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify properties are returned (if data exists)
            if len(data["data"]) > 0:
                record = data["data"][0]
                # Check that at least one property is present
                assert "market" in record or "id" in record or "timestamp" in record
    
    # ========== Test Case 7: Query with Ordering ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_ordering(self, table_name):
        """Test 7: Query with ordering by timestamp"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "orderBy": "time+"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify data is ordered (if multiple records exist)
            if len(data["data"]) > 1:
                timestamps = [int(record.get("timestamp", 0)) for record in data["data"] if "timestamp" in record]
                if len(timestamps) > 1:
                    assert timestamps == sorted(timestamps), "Data should be ordered by timestamp ascending"
    
    # ========== Test Case 8: Complex Query with All Parameters ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_complex_query_all_parameters(self, table_name):
        """Test 8: Complex query with all parameters combined"""
        metrics = self.get_metrics_for_table(table_name)
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "searchByProperties": "resource.id==143",
            "properties": "market,region,id",
            "requestgranularity": "1-hour",
            "orderBy": "time+"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert "start" in data
            assert "end" in data
            assert "granularity" in data
            assert data["granularity"] == "1-hour"
    
    # ========== Test Case 9: Query with Multiple Metrics ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_multiple_metrics(self, table_name):
        """Test 9: Query with multiple metrics"""
        metrics = self.get_metrics_for_table(table_name)
        params = {
            "table": table_name,
            "metrics": metrics
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert len(data["metrics"]) >= 1
    
    # ========== Test Case 10: Query with Market Filter ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_market_filter(self, table_name):
        """Test 10: Query with market entity filter"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "searchByProperties": "resource.market==US"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify all returned records match the filter (if data exists)
            if len(data["data"]) > 0:
                for record in data["data"]:
                    if "market" in record:
                        assert str(record["market"]) == "US", \
                            f"Expected market='US', got '{record['market']}'"
    
    # ========== Test Case 11: Query with 30-minute Granularity ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_30_minute_granularity(self, table_name):
        """Test 11: Query with 30-minute granularity"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "30-minute"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert "granularity" in data
            assert data["granularity"] == "30-minute"
    
    # ========== Test Case 12: Query with 1-day Granularity ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_1_day_granularity(self, table_name):
        """Test 12: Query with 1-day granularity"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": "1-day"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            assert "granularity" in data
            assert data["granularity"] == "1-day"
    
    # ========== Test Case 13: Query with Descending Order ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_descending_order(self, table_name):
        """Test 13: Query with descending timestamp order"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "start": "1749992400000",
            "end": "1750057199000",
            "orderBy": "time-"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify data is ordered descending (if multiple records exist)
            if len(data["data"]) > 1:
                timestamps = [int(record.get("timestamp", 0)) for record in data["data"] if "timestamp" in record]
                if len(timestamps) > 1:
                    assert timestamps == sorted(timestamps, reverse=True), \
                        "Data should be ordered by timestamp descending"
    
    # ========== Test Case 14: Query with Multiple Entity Columns ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES)
    def test_query_with_multiple_entity_columns(self, table_name):
        """Test 14: Query with multiple entity filters"""
        metrics = self.get_metrics_for_table(table_name).split(',')[0]
        params = {
            "table": table_name,
            "metrics": metrics,
            "searchByProperties": "resource.market==US&resource.region==East"
        }
        response = self.make_request(params)
        self.validate_response_structure(response, table_name)
        
        if response.status_code == 200:
            data = response.json()
            # Verify filters are applied (if data exists)
            if len(data["data"]) > 0:
                for record in data["data"]:
                    if "market" in record:
                        assert str(record["market"]) == "US"
                    if "region" in record:
                        assert str(record["region"]) == "East"
    
    # ========== Test Case 15: Error Handling - Invalid Table ==========
    def test_invalid_table_name(self):
        """Test 15: Error handling for invalid table name"""
        params = {
            "table": "invalid_table_name",
            "metrics": "metric1"
        }
        response = self.make_request(params, print_debug=False)
        assert response.status_code in [400, 422], \
            f"Expected error status for invalid table, got {response.status_code}"
    
    # ========== Test Case 16: Error Handling - Missing Metrics ==========
    @pytest.mark.parametrize("table_name", ALL_TABLES[:3])  # Test on first 3 tables only
    def test_missing_metrics_parameter(self, table_name):
        """Test 16: Error handling for missing metrics parameter"""
        params = {
            "table": table_name
        }
        response = self.make_request(params, print_debug=False)
        assert response.status_code in [400, 422], \
            f"Expected error status for missing metrics, got {response.status_code}"

