import os
import subprocess
import sys
import time
import platform

def check_requirements():
    """Check if all required packages are installed"""
    try:
        # Try importing required packages
        import streamlit
        import pandas
        import sqlite3
        import threading  # Required for background processing
        import requests  # Required for Ollama API
        
        # Optional packages - we'll just warn if they're missing
        try:
            import mysql.connector
        except ImportError:
            print("Warning: mysql-connector-python not installed. MySQL databases will not be supported.")
        
        try:
            import psycopg2
        except ImportError:
            print("Warning: psycopg2-binary not installed. PostgreSQL databases will not be supported.")
            
        return True
    except ImportError as e:
        print(f"Missing required package: {e}")
        return False

def install_requirements():
    """Install required packages using our custom installer"""
    print("Installing required packages...")
    try:
        # Use our custom installer script that handles multiple installation methods
        result = subprocess.run([sys.executable, "install_dependencies.py"], check=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as e:
        print(f"Error installing requirements: {e}")
        return False

def setup_databases():
    """Create sample databases if they don't exist"""
    # Create databases directory if it doesn't exist
    if not os.path.exists('databases'):
        os.makedirs('databases')
        print("Created databases directory.")
    
    # Only create databases if they don't exist
    if not os.path.exists('databases/sales.db') or \
       not os.path.exists('databases/customers.db') or \
       not os.path.exists('databases/inventory.db'):
        print("Setting up sample databases...")
        try:
            import create_sample_dbs
            print("Sample databases created successfully.")
            return True
        except Exception as e:
            print(f"Error creating sample databases: {e}")
            return False
    return True

def setup_db_config():
    """Set up initial database configuration if it doesn't exist"""
    if not os.path.exists('db_config.json'):
        print("Creating initial database configuration...")
        try:
            # Import app modules here to avoid circular imports
            from app import scan_for_databases
            scan_for_databases(show_messages=False)
            print("Initial database configuration created.")
            return True
        except Exception as e:
            print(f"Error setting up database configuration: {e}")
            # Create a minimal config file
            import json
            with open('db_config.json', 'w') as f:
                json.dump([], f)
            return False
    return True

def setup_ollama():
    """Set up Ollama with DeepSeek model"""
    try:
        print("\nChecking Ollama setup...")
        
        # Check if Ollama is installed
        try:
            if platform.system() == "Windows":
                result = subprocess.run(["where", "ollama"], capture_output=True, text=True)
            else:
                result = subprocess.run(["which", "ollama"], capture_output=True, text=True)
            
            if result.returncode != 0:
                print("Ollama is not installed. You'll need to install it to use LLM features.")
                print("Get it from: https://ollama.com/download")
                return False
        except:
            print("Ollama is not installed. You'll need to install it to use LLM features.")
            print("Get it from: https://ollama.com/download")
            return False
        
        # Check if Ollama is running
        try:
            import requests
            response = requests.get("http://localhost:11434/api/tags")
            if response.status_code != 200:
                print("Ollama service is not running. You'll need to start it to use LLM features.")
                print("Run 'ollama serve' in a separate terminal.")
                return False
        except:
            print("Ollama service is not running. You'll need to start it to use LLM features.")
            print("Run 'ollama serve' in a separate terminal.")
            return False
        
        # Check if DeepSeek model is available
        try:
            response = requests.get("http://localhost:11434/api/tags")
            models = response.json().get("models", [])
            deepseek_available = any("deepseek-r1" in model.get("name", "") for model in models)
            
            if not deepseek_available:
                print("DeepSeek-r1 model is not available. Pulling it now (this may take a while)...")
                subprocess.run(["ollama", "pull", "deepseek-r1:1.5b"], check=True)
                print("DeepSeek-r1 model pulled successfully.")
            else:
                print("DeepSeek-r1 model is already available.")
            
            return True
        except Exception as e:
            print(f"Error checking or pulling DeepSeek model: {e}")
            print("You can pull it manually with: ollama pull deepseek-r1:1.5b")
            return False
            
    except Exception as e:
        print(f"Error setting up Ollama: {e}")
        return False

def monitor_startup():
    """Monitor the startup of the Streamlit app to ensure background processes start"""
    # Wait for metadata file to be created, which indicates the app has started
    # and performed its initial database scan
    start_time = time.time()
    timeout = 60  # 60 seconds timeout
    
    print("Waiting for initial database scan to complete...")
    while not os.path.exists('db_metadata.json'):
        if time.time() - start_time > timeout:
            print("Warning: Timeout waiting for initial database scan.")
            break
        time.sleep(1)
    
    if os.path.exists('db_metadata.json'):
        print("Initial database scan completed successfully.")
        return True
    return False

def run_app():
    """Run the Streamlit app"""
    print("Starting Streamlit app...")
    try:
        # Start the app in a separate process
        process = subprocess.Popen(["streamlit", "run", "app.py"])
        
        # Monitor startup to ensure background processes start
        monitor_startup()
        
        # Wait for the app to exit
        try:
            process.wait()
        except KeyboardInterrupt:
            print("Shutting down...")
            process.terminate()
            process.wait()
    except FileNotFoundError:
        print("Error: Streamlit command not found. Trying to run with Python module...")
        try:
            process = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "app.py"])
            monitor_startup()
            try:
                process.wait()
            except KeyboardInterrupt:
                print("Shutting down...")
                process.terminate()
                process.wait()
        except Exception as e:
            print(f"Error starting Streamlit: {e}")
            print("Please make sure Streamlit is installed and try again.")
            return False
    except Exception as e:
        print(f"Error starting Streamlit: {e}")
        return False
    return True

if __name__ == "__main__":
    print("Multi-Database Query System Setup")
    print("=================================")
    
    # Check requirements
    if not check_requirements():
        print("Would you like to install the required packages? (y/n)")
        choice = input().lower().strip()
        if choice == 'y':
            if not install_requirements():
                print("Failed to install requirements. Please install them manually.")
                sys.exit(1)
        else:
            print("Please install the required packages and try again.")
            sys.exit(1)
    
    # Set up databases
    if not setup_databases():
        print("Failed to set up sample databases. The app may not work correctly.")
    
    # Set up database configuration
    setup_db_config()
    
    # Set up Ollama (optional)
    ollama_setup = setup_ollama()
    if not ollama_setup:
        print("\nOllama setup incomplete. The app will run with simplified SQL generation.")
        print("You can set up Ollama later by running 'python setup_ollama.py'")
    
    # Ask if user wants to continue
    print("\nReady to start the application.")
    print("Note: The application will start Streamlit in a new window.")
    
    # Run the app
    run_app() 