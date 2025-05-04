import subprocess
import sys
import os
import platform

def check_installed_packages():
    """Check which required packages are already installed"""
    try:
        import importlib.util
        
        packages = [
            "streamlit", "pandas", "sqlite3", "requests", 
            "watchdog", "schedule", "threading", "time", "json"
        ]
        
        installed = []
        missing = []
        
        for package in packages:
            spec = importlib.util.find_spec(package)
            if spec is not None:
                installed.append(package)
            else:
                missing.append(package)
        
        # MySQL and PostgreSQL are optional
        try:
            import mysql.connector
            installed.append("mysql-connector-python")
        except ImportError:
            missing.append("mysql-connector-python")
        
        try:
            import psycopg2
            installed.append("psycopg2-binary")
        except ImportError:
            missing.append("psycopg2-binary")
        
        return installed, missing
    except Exception as e:
        print(f"Error checking installed packages: {e}")
        return [], ["streamlit", "pandas", "mysql-connector-python", "psycopg2-binary", 
                   "watchdog", "schedule", "requests"]

def pip_install(package):
    """Install a package using pip"""
    try:
        print(f"Installing {package}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--prefer-binary", package])
        return True
    except subprocess.CalledProcessError:
        print(f"Failed to install {package} using pip")
        return False

def conda_install(package):
    """Install a package using conda if available"""
    try:
        # Check if conda is available
        subprocess.check_call(["conda", "--version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Map pip package names to conda package names
        conda_packages = {
            "psycopg2-binary": "psycopg2",
            "mysql-connector-python": "mysql-connector-python"
        }
        
        conda_package = conda_packages.get(package, package)
        
        print(f"Installing {package} using conda...")
        subprocess.check_call(["conda", "install", "-y", conda_package])
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"Failed to install {package} using conda")
        return False

def install_dependencies():
    """Install all required dependencies"""
    installed, missing = check_installed_packages()
    
    print("Installed packages:", ", ".join(installed))
    print("Missing packages:", ", ".join(missing))
    
    failures = []
    
    for package in missing:
        if package in ["sqlite3", "threading", "time", "json"]:
            # These are standard library packages, skip
            continue
            
        # First try pip
        if pip_install(package):
            continue
        
        # If pip fails, try conda
        if conda_install(package):
            continue
        
        # If both fail, add to failures
        failures.append(package)
    
    if failures:
        print("\nFailed to install the following packages:")
        for package in failures:
            print(f"- {package}")
        
        print("\nAlternative installation methods:")
        print("1. For Streamlit: npm install -g streamlit")
        print("2. For pandas: conda install pandas")
        print("3. For psycopg2: conda install psycopg2")
        print("4. For MySQL Connector: conda install mysql-connector-python")
        
        return False
    else:
        print("\nAll required packages installed successfully!")
        return True

def check_streamlit_installation():
    """Check if streamlit is installed and can be run"""
    try:
        # Check if streamlit is in PATH
        if platform.system() == "Windows":
            result = subprocess.run(["where", "streamlit"], capture_output=True, text=True)
        else:
            result = subprocess.run(["which", "streamlit"], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Streamlit is installed and in PATH")
            return True
        
        # Check if streamlit can be imported
        import streamlit
        print("Streamlit can be imported but might not be in PATH")
        return True
    except (subprocess.CalledProcessError, ImportError):
        print("Streamlit is not installed or not in PATH")
        return False

if __name__ == "__main__":
    print("Installing dependencies for Multi-Database Query System")
    print("======================================================")
    
    success = install_dependencies()
    
    if success:
        streamlit_ok = check_streamlit_installation()
        if not streamlit_ok:
            print("\nImportant: Streamlit may not be in your PATH.")
            print("You might need to add the Python Scripts directory to your PATH.")
            if platform.system() == "Windows":
                print(f"Try: $env:PATH += ';{os.path.join(sys.prefix, 'Scripts')}'")
            else:
                print(f"Try: export PATH=$PATH:{os.path.join(sys.prefix, 'bin')}")
        
        print("\nSetup completed. You can now run the application with:")
        print("python run.py")
    else:
        print("\nSetup incomplete. Please install the missing dependencies manually.")
        sys.exit(1) 