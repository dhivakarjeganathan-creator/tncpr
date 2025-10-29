#!/usr/bin/env python3
"""
Setup script for JSON Data Loader
This script helps set up the environment and run the application.
"""

import os
import sys
import subprocess

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed!")
        print(f"Error: {e.stderr}")
        return False

def check_python():
    """Check Python version"""
    print("🐍 Checking Python version...")
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print(f"❌ Python 3.8+ required. Current version: {version.major}.{version.minor}")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True

def check_files():
    """Check if required files exist"""
    print("📁 Checking required files...")
    required_files = [
        'Group_configuration.json',
        'Time_scheduling.json',
        'requirements.txt',
        '.env'
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        print(f"❌ Missing files: {', '.join(missing_files)}")
        return False
    
    print("✅ All required files found")
    return True

def install_dependencies():
    """Install Python dependencies"""
    return run_command("pip install -r requirements.txt", "Installing dependencies")

def setup_database():
    """Set up database tables"""
    return run_command("python database.py", "Setting up database tables")

def load_data():
    """Load JSON data into database"""
    return run_command("python data_loader.py", "Loading JSON data")

def main():
    """Main setup function"""
    print("🚀 JSON Data Loader Setup")
    print("=" * 50)
    
    # Check Python version
    if not check_python():
        sys.exit(1)
    
    # Check required files
    if not check_files():
        print("\n❌ Please ensure all required files are present")
        sys.exit(1)
    
    # Install dependencies
    if not install_dependencies():
        print("\n❌ Failed to install dependencies")
        sys.exit(1)
    
    # Setup database
    if not setup_database():
        print("\n❌ Failed to setup database")
        sys.exit(1)
    
    # Load data
    if not load_data():
        print("\n❌ Failed to load data")
        sys.exit(1)
    
    print("\n🎉 Setup completed successfully!")
    print("\nYou can now:")
    print("- Run 'python query_data.py' to explore the data")
    print("- Use any PostgreSQL client to query the database")
    print("- Check the README.md for more information")

if __name__ == "__main__":
    main()
