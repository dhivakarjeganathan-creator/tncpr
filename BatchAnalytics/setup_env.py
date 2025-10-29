#!/usr/bin/env python3
"""
Setup script for Batch Processing Algorithm
Creates .env file with default configuration
"""

import os
import sys

def create_env_file():
    """Create a .env file with default configuration"""
    env_content = """# Database Configuration
DB_HOST=localhost
DB_NAME=hierarchy_db
DB_USER=postgres
DB_PASSWORD=test@1234
DB_PORT=5432
DB_MIN_CONNECTIONS=1
DB_MAX_CONNECTIONS=10

# Logging Configuration
LOG_LEVEL=INFO

# Batch Processing Configuration
MAX_CONCURRENT_JOBS=10
BATCH_SIZE=1000
JOB_TIMEOUT=3600

# Scheduler Configuration
SCHEDULER_CHECK_INTERVAL=60
"""
    
    env_file = '.env'
    
    if os.path.exists(env_file):
        print(f"⚠️  {env_file} already exists!")
        response = input("Do you want to overwrite it? (y/N): ").lower()
        if response != 'y':
            print("❌ Setup cancelled.")
            return False
    
    try:
        with open(env_file, 'w') as f:
            f.write(env_content)
        print(f"✅ Created {env_file} successfully!")
        print(f"\n📝 Please edit {env_file} and update the database parameters with your actual values:")
        print(f"   DB_HOST=your_host")
        print(f"   DB_NAME=your_database_name")
        print(f"   DB_USER=your_username")
        print(f"   DB_PASSWORD=your_password")
        print(f"   DB_PORT=your_port")
        print(f"   DB_MIN_CONNECTIONS=1")
        print(f"   DB_MAX_CONNECTIONS=10")
        return True
    except Exception as e:
        print(f"❌ Error creating {env_file}: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 Batch Processing Algorithm Setup")
    print("=" * 40)
    
    if create_env_file():
        print(f"\n🎉 Setup complete!")
        print(f"\nNext steps:")
        print(f"1. Edit .env file with your database connection")
        print(f"2. Run: python batch_processor.py")
    else:
        print(f"\n❌ Setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
