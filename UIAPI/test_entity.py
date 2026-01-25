"""
Simple script to call the entity API.
"""
import os
import sys
import requests


def main() -> int:
    base_url = os.getenv("UIAPI_BASE_URL", "http://localhost:8001")
    endpoint = f"{base_url.rstrip('/')}/service/dataset/metric/entity"

    params = {
        "searchByProperties": "resource.market=='144' AND resource.type=='carrier'",
        "properties": "NFName,Region,id",
        "format": "json",
        "flatten": "true",
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
