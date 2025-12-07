"""
Quick test runner script
Provides options to run different test suites
"""
import sys
import subprocess
import argparse


def run_pytest_tests(verbose=False, specific_test=None, all_tables=True):
    """Run pytest test suite"""
    if all_tables:
        # Run tests for all tables (default)
        cmd = ["pytest", "tests/test_all_tables.py", "-s"]  # -s to show print output
    else:
        # Run tests for mkt_corning only
        cmd = ["pytest", "tests/test_api_combinations.py", "-s"]  # -s to show print output
    
    if verbose:
        cmd.append("-v")
    else:
        cmd.append("-q")
    
    if specific_test:
        if all_tables:
            cmd.append(f"::TestAllTables::{specific_test}")
        else:
            cmd.append(f"::TestAPICombinations::{specific_test}")
    
    print(f"Running: {' '.join(cmd)}")
    return subprocess.run(cmd)


def run_manual_tests():
    """Run manual validation test script"""
    print("Running manual validation tests...")
    return subprocess.run([sys.executable, "tests/test_manual_validation.py"])


def main():
    parser = argparse.ArgumentParser(description="Run KPI Timeseries API tests")
    parser.add_argument(
        "--type",
        choices=["pytest", "manual", "all"],
        default="all",
        help="Type of tests to run (default: all)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output for pytest"
    )
    parser.add_argument(
        "--test",
        help="Run specific pytest test (e.g., test_basic_query_metrics_only)"
    )
    parser.add_argument(
        "--mkt-corning-only",
        dest="mkt_corning_only",
        action="store_true",
        help="Run tests for mkt_corning only (default: all 18 tables)"
    )
    
    args = parser.parse_args()
    
    print("="*80)
    print("KPI TIMESERIES API - TEST RUNNER")
    print("="*80)
    print("\nMake sure the API server is running on http://localhost:8000")
    print("="*80 + "\n")
    
    results = []
    
    if args.type in ["pytest", "all"]:
        print("\n[1/2] Running pytest test suite...")
        # Default to all tables, unless --mkt-corning-only is specified
        all_tables = not args.mkt_corning_only
        result = run_pytest_tests(verbose=args.verbose, specific_test=args.test, all_tables=all_tables)
        results.append(("pytest", result.returncode))
    
    if args.type in ["manual", "all"]:
        print("\n[2/2] Running manual validation tests...")
        result = run_manual_tests()
        results.append(("manual", result.returncode))
    
    # Summary
    print("\n" + "="*80)
    print("TEST RUN SUMMARY")
    print("="*80)
    for test_type, return_code in results:
        status = "✓ PASSED" if return_code == 0 else "✗ FAILED"
        print(f"{test_type.upper()}: {status} (exit code: {return_code})")
    print("="*80)
    
    # Exit with non-zero if any test failed
    if any(code != 0 for _, code in results):
        sys.exit(1)


if __name__ == "__main__":
    main()

