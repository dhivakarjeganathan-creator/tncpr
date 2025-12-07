"""
Pytest configuration and fixtures
"""
import pytest
import requests
import time


@pytest.fixture(scope="session")
def api_base_url():
    """Base URL for the API"""
    return "http://localhost:8000/api/v1"


@pytest.fixture(scope="session")
def wait_for_api(api_base_url):
    """Wait for API to be ready"""
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            response = requests.get(f"{api_base_url}/health", timeout=2)
            if response.status_code in [200, 503]:
                return True
        except:
            pass
        time.sleep(1)
    pytest.skip("API server is not available")


@pytest.fixture
def sample_timestamps():
    """Sample timestamps for testing"""
    return {
        "start": "1749992400000",  # Unix timestamp in milliseconds
        "end": "1750057199000"
    }


@pytest.fixture
def sample_metrics():
    """Sample metrics for testing"""
    return {
        "simple": "cpu_usage,memory_usage",
        "example1": "SystemUsage.AvgCPU0Freq.number,SystemUsage.CPU0Power.number,SystemUsage.CPUICUtil.percent",
        "example2": "DU.S5NC_DRBDrop_pct_SA,DU.S5NC_NRDCAdd_MN_Succ_pct_SA,DU.S5NC_NGSetupFailure_pct_SA"
    }

