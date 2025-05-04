import subprocess
import sys
import os
import platform
import requests
import time

def check_ollama_installed():
    """Check if Ollama is installed"""
    try:
        if platform.system() == "Windows":
            result = subprocess.run(["where", "ollama"], capture_output=True, text=True)
        else:
            result = subprocess.run(["which", "ollama"], capture_output=True, text=True)
        
        return result.returncode == 0
    except:
        return False

def check_ollama_running():
    """Check if Ollama service is running"""
    try:
        response = requests.get("http://localhost:11434/api/tags")
        return response.status_code == 200
    except:
        return False

def start_ollama_service():
    """Start the Ollama service"""
    try:
        if platform.system() == "Windows":
            # On Windows, start Ollama in a new command window
            subprocess.Popen(["start", "cmd", "/c", "ollama", "serve"], shell=True)
        else:
            # On Linux/Mac, start Ollama in the background
            subprocess.Popen(["ollama", "serve"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait for service to start
        print("Starting Ollama service...")
        for _ in range(10):  # Try 10 times with 1-second delays
            time.sleep(1)
            if check_ollama_running():
                print("Ollama service started successfully.")
                return True
        
        print("Failed to start Ollama service.")
        return False
    except Exception as e:
        print(f"Error starting Ollama service: {e}")
        return False

def pull_deepseek_model():
    """Pull the DeepSeek model"""
    try:
        print("Pulling DeepSeek-r1 model (this may take a while)...")
        subprocess.run(["ollama", "pull", "deepseek-r1:1.5b"], check=True)
        print("DeepSeek-r1 model pulled successfully.")
        return True
    except Exception as e:
        print(f"Error pulling DeepSeek-r1 model: {e}")
        return False

def install_ollama():
    """Provide instructions for installing Ollama"""
    print("Ollama is not installed. Please install it first:")
    
    if platform.system() == "Windows":
        print("Download and install from: https://ollama.com/download/windows")
    elif platform.system() == "Darwin":  # macOS
        print("Run: curl -fsSL https://ollama.com/install.sh | sh")
    else:  # Linux
        print("Run: curl -fsSL https://ollama.com/install.sh | sh")
    
    return False

def main():
    print("Setting up Ollama with DeepSeek-r1 model")
    print("===========================================")
    
    # Check if Ollama is installed
    if not check_ollama_installed():
        install_ollama()
        return False
    
    # Check if Ollama service is running
    if not check_ollama_running():
        if not start_ollama_service():
            print("Please start Ollama manually with 'ollama serve' and try again.")
            return False
    
    # Pull the DeepSeek model
    if not pull_deepseek_model():
        print("Failed to pull DeepSeek-r1 model.")
        return False
    
    print("\nSetup completed successfully!")
    print("You can now use the DeepSeek-r1 model with our application.")
    return True

if __name__ == "__main__":
    success = main()
    if not success:
        sys.exit(1) 