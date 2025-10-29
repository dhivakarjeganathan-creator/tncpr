#!/usr/bin/env python3
"""
Virtual Environment Setup Script for ETL Processor

This script helps set up the virtual environment and install dependencies.
"""

import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(command)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {description}:")
        print(f"Return code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False


def main():
    """Main function to set up virtual environment."""
    print("ETL Processor Virtual Environment Setup")
    print("=" * 50)
    
    # Check if virtual environment already exists
    venv_path = Path("etl_env")
    if venv_path.exists():
        print("Virtual environment 'etl_env' already exists.")
        print("To activate it, run:")
        print("  Windows: etl_env\\Scripts\\activate")
        print("  Linux/Mac: source etl_env/bin/activate")
        print("\nTo reinstall dependencies, run:")
        print("  pip install -r requirements.txt")
        return 0
    
    # Create virtual environment
    print("Creating virtual environment...")
    if not run_command([sys.executable, "-m", "venv", "etl_env"], "Creating virtual environment"):
        print("Failed to create virtual environment")
        return 1
    
    # Install dependencies
    print("Installing dependencies...")
    pip_path = venv_path / "Scripts" / "pip.exe" if os.name == 'nt' else venv_path / "bin" / "pip"
    
    if not run_command([str(pip_path), "install", "-r", "requirements.txt"], "Installing dependencies"):
        print("Failed to install dependencies")
        return 1
    
    print("\n" + "="*60)
    print("Virtual environment setup completed successfully!")
    print("="*60)
    print("\nTo activate the virtual environment:")
    print("  Windows: etl_env\\Scripts\\activate")
    print("  Linux/Mac: source etl_env/bin/activate")
    print("\nTo run the ETL processor:")
    print("  python example_usage.py")
    print("\nTo run tests:")
    print("  python run_tests.py")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
