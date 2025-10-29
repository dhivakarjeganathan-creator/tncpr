"""
Installation script for Streaming Analytics Loader
Handles common installation issues including distutils problems.
"""

import subprocess
import sys
import os

def run_command(command, description):
    """Run a command and handle errors."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed: {e}")
        print(f"Error output: {e.stderr}")
        return False

def install_setuptools():
    """Install setuptools to fix distutils issues."""
    print("\nInstalling setuptools to fix distutils issues...")
    return run_command("pip install setuptools", "setuptools installation")

def install_requirements():
    """Install requirements with fallback options."""
    print("\nInstalling requirements...")
    
    # Try installing requirements normally
    if run_command("pip install -r requirements.txt", "requirements installation"):
        return True
    
    # If that fails, try installing packages individually
    print("\nTrying individual package installation...")
    
    packages = [
        "psycopg2-binary",
        "python-dotenv"
    ]
    
    success = True
    for package in packages:
        if not run_command(f"pip install {package}", f"installing {package}"):
            success = False
    
    return success

def check_python_version():
    """Check Python version and provide guidance."""
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
    """Main installation function."""
    print("Streaming Analytics Loader Installation")
    print("=" * 50)
    
    # Check Python version
    version_ok = check_python_version()
    
    if not version_ok:
        print("\n✗ Python version not supported")
        print("   Please use Python 3.8 or higher")
        return False
    
    # Install setuptools first for compatibility
    print("\nStep 1: Installing setuptools...")
    install_setuptools()
    
    # Install requirements
    print("\nStep 2: Installing requirements...")
    if not install_requirements():
        print("\n✗ Installation failed")
        print("\nTroubleshooting steps:")
        print("1. Try: pip install setuptools")
        print("2. Try: pip install --upgrade pip")
        print("3. Use Python 3.11 instead of 3.12+")
        print("4. Check TROUBLESHOOTING.md for more help")
        return False
    
    # Create .env file
    print("\nStep 3: Creating configuration...")
    if not os.path.exists('.env'):
        from config import create_env_file
        create_env_file()
        print("✓ .env file created")
    else:
        print("✓ .env file already exists")
    
    print("\n" + "=" * 50)
    print("Installation completed!")
    print("=" * 50)
    print("\nNext steps:")
    print("1. Update .env file with your database credentials")
    print("2. Make sure PostgreSQL is running")
    print("3. Run: python setup.py")
    print("4. Run: python streaming_analytics_loader.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
