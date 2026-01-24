"""
Simple script to call the UI timeseries API.
"""
import os
import sys
import requests


def main() -> int:
    base_url = os.getenv("UIAPI_BASE_URL", "http://localhost:8001")
    endpoint = f"{base_url.rstrip('/')}/service/dataset/metric/uitimeseries"

    params = {
        "entityNames": "143",
        "metrics": "ranmarket.endc_sessions_rn,ranmarket.dl_gtp_data_volume_rn_mb",
        "start": "1668417415100",
        "end": "1768435415100",
        "metricDoubleValue": "true",
        "fillUpNull": "false",
        "sort": "timestamp",
        "properties": "NFName,NFType,transcelltype",
    }

    response = requests.get(endpoint, params=params, timeout=60)
    if response.status_code >= 400:
        print(f"status: {response.status_code}")
        print(response.text)
        return 1
    print(response.json())
    return 0


if __name__ == "__main__":
    sys.exit(main())
