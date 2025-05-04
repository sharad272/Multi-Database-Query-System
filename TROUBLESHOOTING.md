# Troubleshooting Guide

## Common Issues and Solutions

### Streamlit Not Opening in Browser

If Streamlit doesn't automatically open in your browser:

1. Manually open your browser and go to: http://localhost:8888
2. If that doesn't work, check if Streamlit is running by looking for messages in the terminal
3. Make sure no other application is using port 8888
4. Try accessing the app using your machine's IP address instead of localhost: http://YOUR_IP:8888

### Installation Issues

#### Dependency Installation Errors

If you're seeing errors related to installing dependencies:

1. Try running the dedicated installer: `python install_dependencies.py`
2. For specific packages, try installing them manually:
   ```
   pip install --prefer-binary streamlit pandas
   ```
3. If you have Conda, try using it instead:
   ```
   conda install streamlit pandas
   ```

#### Numpy Installation Issues on Windows

If you're seeing errors related to Numpy compilation:

1. Try installing a pre-compiled binary:
   ```
   pip install --prefer-binary numpy
   ```
2. Or use conda:
   ```
   conda install numpy
   ```

### Streamlit Command Not Found

If you get "streamlit command not found" errors:

1. Make sure Streamlit is installed:
   ```
   pip install streamlit
   ```
2. Try running with Python module syntax:
   ```
   python -m streamlit run app.py --server.port 8888
   ```
3. Add the Python Scripts directory to your PATH:
   - Windows: `$env:PATH += ";C:\Path\to\Python\Scripts"`
   - Mac/Linux: `export PATH=$PATH:/path/to/python/bin`

### Port Already In Use

If you see an error that port 8888 is already in use:

1. Find and close any applications using the port
2. Try using a different port:
   ```
   streamlit run app.py --server.port 9999
   ```
3. On Windows, you can find what's using a port with:
   ```
   netstat -ano | findstr :8888
   ```
4. On Linux/Mac:
   ```
   lsof -i :8888
   ```

### Database Connection Issues

If you see database-related errors:

1. Make sure the sample databases were created:
   ```
   python create_sample_dbs.py
   ```
2. Check if the 'databases' directory exists and contains .db files
3. Verify that SQLite is working by running:
   ```
   python -c "import sqlite3; print('SQLite is working')"
   ```

### Ollama / LLM Issues

If the LLM features aren't working:

1. Make sure Ollama is installed and running:
   ```
   ollama serve
   ```
2. Pull the required model:
   ```
   ollama pull deepseek-r1:1.5b
   ```
3. Check Ollama API is working by opening: http://localhost:11434/api/tags

### Windows-Specific Issues

If you're having issues on Windows:

1. Try running as Administrator
2. Ensure Python is properly added to your PATH
3. If using Visual Studio, make sure C++ build tools are installed
4. Try using Windows PowerShell instead of Command Prompt

### Mac/Linux-Specific Issues

If you're having issues on Mac or Linux:

1. Make sure you have execution permissions:
   ```
   chmod +x start.sh
   ```
2. If getting SSL errors, try updating certificates:
   ```
   pip install --upgrade certifi
   ```

## Still Having Issues?

If you're still experiencing problems:

1. Check that your Python version is 3.8 or newer
2. Try creating a new virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Look for specific error messages in the terminal and search for solutions

For detailed debugging, run with Streamlit's debug mode:
```
streamlit run app.py --server.port 8888 --logger.level=debug
```

### Firewall Issues

If you suspect firewall issues:

1. Check if your firewall is blocking port 8888
2. Try temporarily disabling your firewall to test
3. Add an exception for Streamlit or Python in your firewall settings 