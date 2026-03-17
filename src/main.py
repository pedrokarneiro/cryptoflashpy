import os
import sys

def setup_paths():
    """Sets up absolute paths for the data directories."""
    # This gets the directory where main.py is located (/app/src)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define data paths relative to the script location
    data_raw = os.path.join(base_dir, '..', 'data', 'raw')
    data_processed = os.path.join(base_dir, '..', 'data', 'processed')
    
    return data_raw, data_processed

def main():
    raw_path, proc_path = setup_paths()
    
    print(f"--- Cryptoflashpy Initialized ---")
    print(f"Python Version: {sys.version}")
    print(f"Raw Data Path: {os.path.abspath(raw_path)}")
    print(f"Processed Data Path: {os.path.abspath(proc_path)}")
    
    # Simple check to see if paths exist
    if os.path.exists(raw_path):
        print("✅ Data directory found.")
    else:
        print("❌ Data directory missing!")

if __name__ == "__main__":
    main()