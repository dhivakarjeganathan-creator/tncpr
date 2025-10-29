"""
Python 3.12 Compatibility Test
This script tests the streaming analytics loader for Python 3.12+ compatibility.
"""

import sys
import subprocess
import importlib.util

def test_python_version():
    """Test Python version compatibility."""
    print(f"Python version: {sys.version}")
    
    if sys.version_info >= (3, 12):
        print("✓ Python 3.12+ detected - testing compatibility")
        return True
    elif sys.version_info >= (3, 8):
        print("✓ Python version is compatible")
        return True
    else:
        print("✗ Python version too old")
        return False

def test_imports():
    """Test if all required modules can be imported."""
    print("\nTesting imports...")
    
    required_modules = [
        'json',
        'psycopg2',
        'psycopg2.extras',
        'datetime',
        'logging',
        'typing',
        'os'
    ]
    
    success = True
    for module in required_modules:
        try:
            __import__(module)
            print(f"✓ {module}")
        except ImportError as e:
            print(f"✗ {module}: {e}")
            success = False
    
    return success

def test_compatibility_module():
    """Test the Python 3.12 compatibility module."""
    print("\nTesting compatibility module...")
    
    try:
        from python312_compat import check_compatibility, ensure_setuptools
        print("✓ Compatibility module imported")
        
        if check_compatibility():
            print("✓ Compatibility check passed")
        else:
            print("✗ Compatibility check failed")
            return False
        
        if ensure_setuptools():
            print("✓ setuptools available")
        else:
            print("✗ setuptools not available")
            return False
        
        return True
    except ImportError as e:
        print(f"✗ Compatibility module import failed: {e}")
        return False

def test_config_module():
    """Test the configuration module."""
    print("\nTesting configuration module...")
    
    try:
        from config import Config
        print("✓ Config module imported")
        
        # Test configuration methods
        db_config = Config.get_db_config()
        print(f"✓ Database config: {list(db_config.keys())}")
        
        if Config.validate_config():
            print("✓ Configuration validation passed")
        else:
            print("✗ Configuration validation failed")
            return False
        
        return True
    except ImportError as e:
        print(f"✗ Config module import failed: {e}")
        return False

def test_loader_class():
    """Test the streaming analytics loader class."""
    print("\nTesting loader class...")
    
    try:
        from streaming_analytics_loader import StreamingAnalyticsLoader
        print("✓ Loader class imported")
        
        # Test class initialization
        loader = StreamingAnalyticsLoader()
        print("✓ Loader instance created")
        
        return True
    except ImportError as e:
        print(f"✗ Loader class import failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Loader class initialization failed: {e}")
        return False

def test_json_loading():
    """Test JSON file loading."""
    print("\nTesting JSON file loading...")
    
    try:
        import json
        with open('Streaming_analytics.json', 'r') as f:
            data = json.load(f)
        print(f"✓ JSON file loaded: {len(data)} records")
        return True
    except FileNotFoundError:
        print("✗ JSON file not found")
        return False
    except json.JSONDecodeError as e:
        print(f"✗ JSON parsing error: {e}")
        return False
    except Exception as e:
        print(f"✗ JSON loading failed: {e}")
        return False

def main():
    """Run all compatibility tests."""
    print("Python 3.12+ Compatibility Test")
    print("=" * 50)
    
    tests = [
        ("Python Version", test_python_version),
        ("Module Imports", test_imports),
        ("Compatibility Module", test_compatibility_module),
        ("Config Module", test_config_module),
        ("Loader Class", test_loader_class),
        ("JSON Loading", test_json_loading)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"✗ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("Test Results Summary:")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Python 3.12+ compatibility confirmed.")
        return True
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
