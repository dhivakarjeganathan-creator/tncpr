"""
Setup script for the Hierarchy Management System
"""
import os
import sys
import subprocess
from pathlib import Path

def install_requirements():
    """Install required Python packages"""
    print("Installing required packages...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✓ Requirements installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install requirements: {e}")
        return False

def create_env_file():
    """Create .env file from template"""
    env_file = Path(".env")
    env_example = Path("env_example.txt")
    
    if env_file.exists():
        print("✓ .env file already exists")
        return True
    
    if env_example.exists():
        try:
            with open(env_example, 'r') as f:
                content = f.read()
            
            with open(env_file, 'w') as f:
                f.write(content)
            
            print("✓ Created .env file from template")
            print("⚠️  Please update the database credentials in .env file")
            return True
        except Exception as e:
            print(f"✗ Failed to create .env file: {e}")
            return False
    else:
        print("✗ env_example.txt not found")
        return False

def create_csv_folder():
    """Create CSV folder if it doesn't exist"""
    csv_folder = Path("csv_files")
    csv_folder.mkdir(exist_ok=True)
    print("✓ CSV folder ready")
    return True

def check_postgresql():
    """Check if PostgreSQL is available"""
    try:
        import psycopg2
        print("✓ PostgreSQL driver (psycopg2) is available")
        return True
    except ImportError:
        print("✗ PostgreSQL driver (psycopg2) not found")
        return False

def main():
    """Main setup function"""
    print("Hierarchy Management System Setup")
    print("=" * 40)
    
    success = True
    
    # Install requirements
    if not install_requirements():
        success = False
    
    # Create .env file
    if not create_env_file():
        success = False
    
    # Create CSV folder
    if not create_csv_folder():
        success = False
    
    # Check PostgreSQL
    if not check_postgresql():
        success = False
    
    print("\n" + "=" * 40)
    if success:
        print("✓ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Update database credentials in .env file")
        print("2. Create PostgreSQL database: hierarchy_db")
        print("3. Run: python main.py")
    else:
        print("✗ Setup completed with errors")
        print("Please fix the issues above before proceeding")
    
    return success

if __name__ == "__main__":
    main()
