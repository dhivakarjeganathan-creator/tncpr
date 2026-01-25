"""
Simple script to call the summary API.
"""
import os
import sys
import requests


def main() -> int:
    base_url = os.getenv("UIAPI_BASE_URL", "http://localhost:8001")
    endpoint = f"{base_url.rstrip('/')}/service/dataset/metric/summary"

    params = {
        "metrics": "GNB.5GNR_DL_MAC_Volume_MB",
        "start": "1767139200000",
        "end": "1768348799000",
        "searchByProperties": "resource.Market=='144' AND resource.type=='carrier'",
        "properties": "type,NFName",
        "flatten": "true",
        "metricDoubleValue": "true",
        "suppressSummary": "true",
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
