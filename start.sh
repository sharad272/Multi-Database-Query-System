#!/bin/bash

echo "Starting Multi-Database Query System..."
echo "======================================"

# Check if Python is installed
if ! command -v python3 &> /dev/null
then
    echo "Error: Python3 not found. Please install Python and try again."
    read -p "Press Enter to continue..."
    exit 1
fi

echo ""
echo "The application will start in a moment..."
echo "Streamlit will be available at http://localhost:8888"
echo "If browser doesn't open automatically, please open this URL manually."
echo ""

# Kill any existing Streamlit processes
pkill -f "streamlit run" 2>/dev/null || true

# Run the simplified start script
python3 start.py

# If that fails, try the run script
if [ $? -ne 0 ]; then
    echo "Trying alternative startup method..."
    python3 run.py
fi

# If everything fails, show error
if [ $? -ne 0 ]; then
    echo ""
    echo "Failed to start the application."
    echo "Troubleshooting steps:"
    echo "1. Try running 'python3 install_dependencies.py' first."
    echo "2. Try running 'python3 -m streamlit run app.py --server.port 8888' directly."
    echo "3. Open http://localhost:8888 in your browser."
    echo "4. Check TROUBLESHOOTING.md for more solutions."
fi

read -p "Press Enter to continue..." 