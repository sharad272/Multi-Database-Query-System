@echo off
echo Starting Multi-Database Query System...
echo ======================================

:: Check if Python is installed
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Error: Python not found. Please install Python and try again.
    pause
    exit /b
)

echo.
echo The application will start in a moment...
echo Streamlit will be available at http://localhost:8888
echo If browser doesn't open automatically, please open this URL manually.
echo.

:: Kill any existing Streamlit processes first
taskkill /f /im streamlit.exe >nul 2>nul

:: Run the simplified start script
python start.py

:: If that fails, try the run script
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Trying alternative startup method...
    python run.py
)

:: If everything fails, show error
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo Failed to start the application.
    echo.
    echo Troubleshooting steps:
    echo 1. Try running 'python install_dependencies.py' first
    echo 2. Try running 'python -m streamlit run app.py --server.port 8888' directly
    echo 3. Open http://localhost:8888 in your browser
    echo 4. Check TROUBLESHOOTING.md for more solutions
)

pause 