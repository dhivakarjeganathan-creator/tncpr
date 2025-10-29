"""
Python 3.12 Compatibility Module
This module provides compatibility fixes for Python 3.12+ features.
"""

import sys
import warnings

def ensure_setuptools():
    """Ensure setuptools is available for Python 3.12+ compatibility."""
    try:
        import setuptools
        return True
    except ImportError:
        try:
            import subprocess
            subprocess.run([sys.executable, "-m", "pip", "install", "setuptools"], 
                          check=True, capture_output=True)
            import setuptools
            return True
        except Exception:
            return False

def check_compatibility():
    """Check Python version compatibility and provide fixes."""
    if sys.version_info >= (3, 12):
        # Python 3.12+ specific checks
        if not ensure_setuptools():
            warnings.warn(
                "setuptools not found. Some packages may fail to install. "
                "Run: pip install setuptools",
                UserWarning
            )
        
        # Check for distutils replacement
        try:
            import distutils
            warnings.warn(
                "distutils is deprecated in Python 3.12+. "
                "Consider using setuptools instead.",
                DeprecationWarning
            )
        except ImportError:
            # This is expected in Python 3.12+
            pass
    
    return True

def get_install_command():
    """Get the appropriate pip install command for the current Python version."""
    if sys.version_info >= (3, 12):
        return [
            sys.executable, "-m", "pip", "install", 
            "--upgrade", "pip", "setuptools", "wheel"
        ]
    else:
        return [sys.executable, "-m", "pip", "install", "--upgrade", "pip"]

def install_compatibility_packages():
    """Install packages required for Python 3.12+ compatibility."""
    import subprocess
    
    try:
        cmd = get_install_command()
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✓ Compatibility packages installed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to install compatibility packages: {e}")
        print(f"Error output: {e.stderr}")
        return False

if __name__ == "__main__":
    print("Python 3.12+ Compatibility Check")
    print("=" * 40)
    
    if check_compatibility():
        print("✓ Compatibility check passed")
        
        if sys.version_info >= (3, 12):
            print("Installing compatibility packages...")
            if install_compatibility_packages():
                print("✓ All compatibility packages installed")
            else:
                print("✗ Some compatibility packages failed to install")
    else:
        print("✗ Compatibility check failed")
