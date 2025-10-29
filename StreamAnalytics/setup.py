"""
Setup script for Streaming Analytics Loader
This script helps set up the environment and configuration.
"""

import os
import sys
from config import Config, create_env_file

def setup_environment():
    """Set up the environment and configuration."""
    print("Setting up Streaming Analytics Loader...")
    print("=" * 50)
    
    # Create .env file if it doesn't exist
    create_env_file()
    
    # Check if .env file exists
    if os.path.exists('.env'):
        print("✓ .env file found")
    else:
        print("✗ .env file not found")
        return False
    
    # Validate configuration
    if Config.validate_config():
        print("✓ Configuration is valid")
        Config.print_config()
        return True
    else:
        print("✗ Configuration validation failed")
        print("\nPlease update the values in .env file according to your PostgreSQL setup:")
        print("  - DB_HOST: Your PostgreSQL host")
        print("  - DB_NAME: Your database name")
        print("  - DB_USER: Your PostgreSQL username")
        print("  - DB_PASSWORD: Your PostgreSQL password")
        print("  - DB_PORT: Your PostgreSQL port (usually 5432)")
        return False

def check_dependencies():
    """Check if required dependencies are installed."""
    print("\nChecking dependencies...")
    
    required_packages = [
        'psycopg2',
        'dotenv'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\nMissing packages: {', '.join(missing_packages)}")
        print("Please install them using: pip install -r requirements.txt")
        return False
    
    return True

def check_python_version():
    """Check Python version compatibility."""
    import sys
    
    print(f"\nPython version: {sys.version}")
    
    if sys.version_info >= (3, 12):
        print("✓ Python 3.12+ detected - fully supported")
        print("   This version is now fully compatible with the streaming analytics loader")
        return True
    elif sys.version_info >= (3, 8):
        print("✓ Python version is compatible")
        return True
    else:
        print("✗ Python version too old. Please use Python 3.8 or higher")
        return False

def main():
    """Main setup function."""
    print("Streaming Analytics Loader Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python_version():
        print("\n✗ Python version not supported")
        print("   Please use Python 3.8 or higher")
        return
    
    # Check dependencies
    if not check_dependencies():
        print("\nPlease install missing dependencies and run setup again.")
        print("\nIf you encounter installation issues:")
        print("1. Try: pip install --upgrade pip")
        print("2. Try: pip install setuptools wheel")
        print("3. Check TROUBLESHOOTING.md for more help")
        return
    
    # Setup environment
    if not setup_environment():
        print("\nSetup incomplete. Please fix configuration issues and run setup again.")
        return
    
    print("\n" + "=" * 50)
    print("Setup completed successfully!")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Update the values in .env file according to your PostgreSQL setup")
    print("2. Make sure PostgreSQL is running and accessible")
    print("3. Run: python streaming_analytics_loader.py")
    print("4. Or run: python example_usage.py")
    print("\nIf you encounter issues, check TROUBLESHOOTING.md")

if __name__ == "__main__":
    main()
