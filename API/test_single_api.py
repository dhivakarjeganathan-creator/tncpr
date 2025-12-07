"""
Simple script to test a single API call and show the output
Run this to see the API URL, query, and results printed
"""
import requests
import json
import sys

BASE_URL = "http://localhost:8000/api/v1"
ENDPOINT = f"{BASE_URL}/timeseries"

def print_test_info(response, params):
    """Print API URL, query, and results"""
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
        print(f"\nResponse (not JSON): {response.text[:500]}", flush=True)
        print("="*80 + "\n", flush=True)
    except Exception as e:
        print(f"\nError: {e}", flush=True)
        print("="*80 + "\n", flush=True)


if __name__ == "__main__":
    print("="*80)
    print("SINGLE API TEST - Shows API URL, Query, and Results")
    print("="*80)
    print("\nMake sure the API server is running on http://localhost:8000")
    print("="*80 + "\n")
    
    # Test parameters
    params = {
        "table": "mkt_corning",
        "metrics": "ranmarket_endc_sessions_rn,ranmarket_intra_cu_ho_attempts_number",
        "start": "1749992400000",
        "end": "1750057199000",
        "debug": "true"
    }
    
    try:
        print("Making API request...", flush=True)
        response = requests.get(ENDPOINT, params=params, timeout=30)
        print_test_info(response, params)
    except requests.exceptions.ConnectionError:
        print("ERROR: Cannot connect to API server. Is it running?", flush=True)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}", flush=True)
        sys.exit(1)

