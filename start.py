import subprocess
import sys
import os
import time
import webbrowser

def start_app():
    """Start the application with error handling"""
    print("Starting Multi-Database Query System...")
    
    # Check if dependencies are installed
    try:
        import streamlit
        import pandas
    except ImportError:
        print("Missing required dependencies.")
        print("Running dependency installer...")
        
        # Run dependency installer
        try:
            if os.path.exists("install_dependencies.py"):
                subprocess.run([sys.executable, "install_dependencies.py"], check=True)
            else:
                print("Installing basic dependencies...")
                subprocess.run([sys.executable, "-m", "pip", "install", "--prefer-binary", "streamlit", "pandas"], check=True)
        except subprocess.CalledProcessError:
            print("Error installing dependencies.")
            print("Please run 'python install_dependencies.py' manually.")
            return False
    
    # Create the databases directory if it doesn't exist
    if not os.path.exists("databases"):
        os.makedirs("databases")
    
    # Create the sample databases
    if not os.path.exists("databases/sales.db") or \
       not os.path.exists("databases/customers.db") or \
       not os.path.exists("databases/inventory.db"):
        print("Creating sample databases...")
        try:
            subprocess.run([sys.executable, "create_sample_dbs.py"], check=True)
        except subprocess.CalledProcessError:
            print("Error creating sample databases.")
            print("The application may not work correctly without sample data.")
    
    # Try to start the application
    print("\nStarting the application...")
    
    # Force Streamlit to open in the browser and use port 8888
    # This port is less likely to be in use or blocked
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "false"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["STREAMLIT_SERVER_PORT"] = "8888"  # Use port 8888 instead of 5000
    
    streamlit_port = 8888  # Change to port 8888
    streamlit_url = f"http://localhost:{streamlit_port}"
    
    # Kill any existing Streamlit processes first (Windows)
    try:
        if os.name == 'nt':  # Windows
            subprocess.run("taskkill /f /im streamlit.exe", shell=True, stderr=subprocess.DEVNULL)
    except:
        pass  # Ignore errors if no processes found
        
    try:
        # First try to use streamlit command
        print(f"The Streamlit app will be available at {streamlit_url}")
        print("If it doesn't open automatically, please open this URL in your browser.")
        print("Please wait, this may take a few moments...")
        
        # Wait a moment for any previous Streamlit instance to shut down
        time.sleep(2)
        
        # Try to open the browser after a short delay
        def open_browser():
            time.sleep(5)  # Give Streamlit more time to start
            try:
                webbrowser.open(streamlit_url)
                print("Browser opened automatically.")
            except:
                print("Could not open browser automatically. Please open the URL manually.")
        
        # Start browser opening in a separate thread to not block
        import threading
        threading.Thread(target=open_browser, daemon=True).start()
        
        # Run Streamlit with the appropriate command and specify port
        # Use subprocess.call to wait for the process to complete
        process = subprocess.Popen(["streamlit", "run", "app.py", "--server.port", str(streamlit_port)])
        
        # Keep app running until user presses Ctrl+C
        print("\nPress Ctrl+C to exit the application...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")
            process.terminate()
        
        return True
    except FileNotFoundError:
        # If that fails, try python -m streamlit
        try:
            print("Using alternative method to start Streamlit...")
            print(f"The Streamlit app will be available at {streamlit_url}")
            print("Please wait, this may take a few moments...")
            
            # Try to open the browser after a short delay
            def open_browser():
                time.sleep(5)  # Give Streamlit more time to start
                try:
                    webbrowser.open(streamlit_url)
                    print("Browser opened automatically.")
                except:
                    print("Could not open browser automatically. Please open the URL manually.")
            
            # Start browser opening in a separate thread to not block
            import threading
            threading.Thread(target=open_browser, daemon=True).start()
            
            # Run Streamlit with Python module and specify port
            process = subprocess.Popen([sys.executable, "-m", "streamlit", "run", "app.py", "--server.port", str(streamlit_port)])
            
            # Keep app running until user presses Ctrl+C
            print("\nPress Ctrl+C to exit the application...")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nShutting down...")
                process.terminate()
                
            return True
        except Exception as e:
            print(f"Error starting Streamlit: {e}")
            print("\nTroubleshooting guide:")
            print("1. Make sure Streamlit is installed: pip install streamlit")
            print("2. Try running 'python run.py'")
            print("3. If all else fails, try running 'streamlit run app.py --server.port 8888' manually")
            print(f"4. Open {streamlit_url} in your browser")
            return False

if __name__ == "__main__":
    start_app() 