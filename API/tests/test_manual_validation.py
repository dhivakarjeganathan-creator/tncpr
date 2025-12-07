"""
Manual test script for validating API combinations
Based on examples from apiinitialreq.txt
Run this script to test various API combinations manually
"""
import requests
import json
import sys
from datetime import datetime
from typing import Dict, Any


BASE_URL = "http://localhost:8000/api/v1"
ENDPOINT = f"{BASE_URL}/timeseries/mkt_corning"
REQUEST_TIMEOUT = 30  # seconds


def print_test_header(test_name: str):
    """Print formatted test header"""
    print("\n" + "="*80)
    print(f"TEST: {test_name}")
    print("="*80)


def print_response(response: requests.Response, show_data: bool = False):
    """Print formatted response with API URL, query, and results"""
    print("\n" + "="*80)
    print("API REQUEST")
    print("="*80)
    print(f"URL: {response.url}")
    print(f"Status Code: {response.status_code}")
    
    try:
        data = response.json()
        
        # Print query information if available
        if "debug" in data:
            print("\n" + "-"*80)
            print("GENERATED SQL QUERY")
            print("-"*80)
            print(data["debug"]["query"])
            if data["debug"]["query_params"]:
                print(f"\nQuery Parameters: {data['debug']['query_params']}")
            print("-"*80)
        
        # Print results summary
        print("\n" + "-"*80)
        print("QUERY RESULTS")
        print("-"*80)
        print(f"Table: {data.get('table', 'N/A')}")
        print(f"Metrics: {', '.join(data.get('metrics', []))}")
        print(f"Count: {data.get('count', 0)}")
        
        if "start" in data:
            print(f"Start: {data['start']}")
        if "end" in data:
            print(f"End: {data['end']}")
        if "granularity" in data:
            print(f"Granularity: {data['granularity']}")
        
        # Print sample data
        if show_data and "data" in data and len(data["data"]) > 0:
            print(f"\nSample Data (showing first {min(3, len(data['data']))} records):")
            for i, record in enumerate(data["data"][:3]):
                print(f"\n  Record {i+1}:")
                for key, value in record.items():
                    print(f"    {key}: {value}")
            if len(data["data"]) > 3:
                print(f"\n  ... and {len(data['data']) - 3} more records")
        elif show_data and "data" in data and len(data["data"]) == 0:
            print("\nNote: No data returned (database may be empty)")
        
        print("="*80 + "\n")
        
    except json.JSONDecodeError:
        print(f"\nResponse Text (not JSON): {response.text[:500]}")
        print("="*80 + "\n")
    except Exception as e:
        print(f"\nError parsing response: {e}")
        print(f"Response Text: {response.text[:500]}")
        print("="*80 + "\n")


def test_case_1_basic_metrics():
    """Test Case 1: Basic query with metrics only"""
    print_test_header("Basic Query - Metrics Only")
    
    params = {
        "metrics": "cpu_usage,memory_usage,network_throughput"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server. Is it running?")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_2_time_range():
    """Test Case 2: Query with time range (Example 2 style)"""
    print_test_header("Query with Time Range (Unix Timestamps)")
    
    params = {
        "metrics": "DU.S5NC_DRBDrop_pct_SA,DU.S5NC_NRDCAdd_MN_Succ_pct_SA,DU.S5NC_NGSetupFailure_pct_SA",
        "start": "1749992400000",
        "end": "1750057199000"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_3_entity_filters():
    """Test Case 3: Query with entity filters"""
    print_test_header("Query with Entity Filters")
    
    params = {
        "metrics": "cpu_usage",
        "market": "US",
        "region": "East"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_4_multiple_entity_values():
    """Test Case 4: Query with multiple entity filter values (Example 2 style)"""
    print_test_header("Query with Multiple Entity Filter Values")
    
    params = {
        "metrics": "DU.S5NC_DRBDrop_pct_SA,DU.S5NC_NRDCAdd_MN_Succ_pct_SA",
        "id": "00000000001,03100010001,03100010002,03100010401,03100010402,03101010201,03101010202,03101010203,03101010204",
        "start": "1749992400000",
        "end": "1750057199000"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_5_aggregation_example1():
    """Test Case 5: Query with aggregation (Example 1 style)"""
    print_test_header("Query with Aggregation - Example 1 Style")
    
    params = {
        "metrics": "SystemUsage.AvgCPU0Freq.number,SystemUsage.CPU0Power.number,SystemUsage.CPUICUtil.percent,SystemUsage.CPUUtil.percent,SystemUsage.IOBusUtil.percent,SystemUsage.JitterCount.number,SystemUsage.MemoryBusUtil.percent",
        "start": "1749992400000",
        "end": "1750057199000",
        "requestgranularity": "1-hour"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_6_properties_selection():
    """Test Case 6: Query with properties selection (Example 1 style)"""
    print_test_header("Query with Properties Selection - Example 1 Style")
    
    params = {
        "metrics": "SystemUsage.CPUUtil.percent,ThermalTemperatures.reading.celsius",
        "start": "1749992400000",
        "end": "1750057199000",
        "properties": "market,region,technology",
        "requestgranularity": "1-hour"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_7_ordering():
    """Test Case 7: Query with ordering (Example 2 style)"""
    print_test_header("Query with Ordering - Example 2 Style")
    
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number,ranmarket_max_endc_sessions_rn_number",
        "start": "1749992400000",
        "end": "1750057199000",
        "orderBy": "time+"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_8_complex_example1_full():
    """Test Case 8: Complex query matching Example 1 fully"""
    print_test_header("Complex Query - Example 1 Full Match")
    
    # Based on Example 1 from requirements - updated for mkt_corning table
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number,ranmarket_max_endc_sessions_rn_number,ranmarket_intra_cu_ho_attempts_rn,ranmarket_intra_cu_ho_success_rn,ranmarket_intra_cu_ho_success_rate_rn_pct,ranmarket_sgnb_modification_attempts_rn,ranmarket_sgnb_modification_success_rn,ranmarket_sgnb_modification_success_rate_rn_pct,ranmarket_ul_gtp_data_volume_rn_mb,ranmarket_attach_success_rate_pct_rn,ranmarket_attach_success_rn_num,ranmarket_attach_success_rn_den,ranmarket_drop_rate_pct_rn,ranmarket_drop_rn_num,ranmarket_drop_rn_den,ranmarket_dl_tput_rn_mbps,ranmarket_ul_tput_rn_mbps",
        "start": "1749992400000",
        "end": "1750057199000",
        "searchByProperties": "resource.market==US,EU",
        "properties": "market,region,technology",
        "requestgranularity": "1-hour"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_9_complex_example2_full():
    """Test Case 9: Complex query matching Example 2 fully"""
    print_test_header("Complex Query - Example 2 Full Match")
    
    # Based on Example 2 from requirements - updated for mkt_corning table
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number,ranmarket_max_endc_sessions_rn_number,ranmarket_intra_cu_ho_attempts_rn,ranmarket_intra_cu_ho_success_rn,ranmarket_intra_cu_ho_success_rate_rn_pct,ranmarket_sgnb_modification_attempts_rn,ranmarket_sgnb_modification_success_rn,ranmarket_sgnb_modification_success_rate_rn_pct",
        "searchByProperties": "resource.id==143,144,145,146,147,148,149,150,151",
        "start": "1749992400000",
        "end": "1750057199000",
        "orderBy": "time+"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_10_different_granularities():
    """Test Case 10: Test different granularity options"""
    print_test_header("Different Granularity Options")
    
    granularities = ["30-minute", "1-hour", "1-day"]
    
    for granularity in granularities:
        print(f"\n--- Testing granularity: {granularity} ---")
        params = {
            "table": "mkt_corning",
            "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
            "start": "1749992400000",
            "end": "1750057199000",
            "requestgranularity": granularity
        }
        
        try:
            # Add debug parameter to get query information
            params_with_debug = {**params, "table": "mkt_corning", "debug": "true"}
            response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
            print_response(response, show_data=False)
            print(f"Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Granularity in response: {data.get('granularity')}")
                print(f"Record count: {data.get('count')}")
        except requests.exceptions.ConnectionError:
            print("ERROR: Cannot connect to API server")
            raise
        except requests.exceptions.Timeout:
            print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
            raise


def test_case_11_all_entity_filters():
    """Test Case 11: Query with all entity filter types"""
    print_test_header("Query with All Entity Filter Types")
    
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn",
        "searchByProperties": "resource.market==US,EU&resource.region==East,West&resource.vcptype==Type1,Type2&resource.technology==5G,4G&resource.datacenter==DC1,DC2&resource.site==Site1,Site2&resource.id==12345,67890"
    }
    
    try:
        # Add debug parameter to get query information
        params_with_debug = {**params, "debug": "true"}
        response = requests.get(ENDPOINT, params=params_with_debug, timeout=REQUEST_TIMEOUT)
        print_response(response, show_data=True)
        assert response.status_code == 200, f"Expected status 200, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_case_12_error_cases():
    """Test Case 12: Error handling test cases"""
    print_test_header("Error Handling Test Cases")
    
    # Missing required parameter
    print("\n--- Test: Missing metrics parameter ---")
    try:
        response = requests.get(ENDPOINT, params={}, timeout=REQUEST_TIMEOUT)
        print(f"Status: {response.status_code} (Expected: 422)")
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise
    
    # Invalid granularity
    print("\n--- Test: Invalid granularity format ---")
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn",
        "requestgranularity": "invalid-format"
    }
    try:
        response = requests.get(ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        print(f"Status: {response.status_code} (Expected: 400)")
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise
    
    # Invalid property
    print("\n--- Test: Invalid property name ---")
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn",
        "properties": "invalid_property"
    }
    try:
        response = requests.get(ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        print(f"Status: {response.status_code} (Expected: 400)")
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def test_health_check():
    """Test health check endpoint"""
    print_test_header("Health Check")
    
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=REQUEST_TIMEOUT)
        print_response(response)
        assert response.status_code in [200, 503], \
            f"Health check should return 200 or 503, got {response.status_code}"
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server")
        raise
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after {REQUEST_TIMEOUT} seconds")
        raise


def check_api_health():
    """Check if API is available"""
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        return response.status_code in [200, 503]
    except:
        return False


def main():
    """Run all test cases"""
    print("\n" + "="*80)
    print("KPI TIMESERIES API - MANUAL VALIDATION TEST SUITE")
    print("="*80)
    print("\nMake sure the API server is running on http://localhost:8000")
    
    # Check API health first
    print("\nChecking API availability...")
    if not check_api_health():
        print("ERROR: API server is not available!")
        print("Please start the API server first:")
        print("  python main.py")
        sys.exit(1)
    print("✓ API server is reachable")
    
    print("\nPress Enter to continue or Ctrl+C to exit...")
    try:
        input()
    except KeyboardInterrupt:
        print("\nTest cancelled by user")
        return
    
    # Test health check first
    try:
        test_health_check()
    except Exception as e:
        print(f"\nERROR: Could not connect to API. Is the server running?")
        print(f"Error: {e}")
        return
    
    # Run all test cases
    test_cases = [
        test_case_1_basic_metrics,
        test_case_2_time_range,
        test_case_3_entity_filters,
        test_case_4_multiple_entity_values,
        test_case_5_aggregation_example1,
        test_case_6_properties_selection,
        test_case_7_ordering,
        test_case_8_complex_example1_full,
        test_case_9_complex_example2_full,
        test_case_10_different_granularities,
        test_case_11_all_entity_filters,
        test_case_12_error_cases,
    ]
    
    passed = 0
    failed = 0
    
    for test_func in test_cases:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            print(f"\n❌ TEST FAILED: {e}")
            failed += 1
        except Exception as e:
            print(f"\n❌ TEST ERROR: {e}")
            failed += 1
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total: {passed + failed}")
    print("="*80)


if __name__ == "__main__":
    main()

