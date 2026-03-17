import os
import sys
from kaggle.api.kaggle_api_extended import KaggleApi

def main():
    print("--- Cryptoflashpy: Kaggle Connection Test ---")
    
    # 1. Initialize and Authenticate
    # The API automatically reads KAGGLE_USERNAME and KAGGLE_KEY from the environment
    try:
        api = KaggleApi()
        api.authenticate()
        print("✅ Authentication Successful!")
    except Exception as e:
        print(f"❌ Authentication Failed: {e}")
        return

    # 2. Define our raw data path
    # Inside container: /app/src -> ../data/raw becomes /app/data/raw
    raw_data_dir = os.path.join('..', 'data', 'raw')
    
    # 3. Discovery: Search for crypto datasets
    print("\n[1] Searching for crypto datasets on Kaggle...")
    try:
        datasets = api.dataset_list(search='cryptocurrency', sort_by='votes')
        
        if not datasets:
            print(" No datasets found matching 'cryptocurrency'.")
        else:
            for ds in datasets[:5]:
                # Use getattr to safely handle 'size' or 'totalBytes'
                size = getattr(ds, 'size', 'N/A')
                print(f" - {ds.ref} (Size: {size})")
    except Exception as e:
        print(f"❌ Error during search: {e}")

    # 4. Check local folder visibility (Persistence Test)
    print(f"\n[2] Checking local path: {os.path.abspath(raw_data_dir)}")
    if os.path.exists(raw_data_dir):
        print(" ✅ Local 'raw' directory is mapped and visible.")
        
        # Try to write a small test file to verify write permissions
        test_file = os.path.join(raw_data_dir, '.write_test')
        try:
            with open(test_file, 'w') as f:
                f.write('connection test success')
            print(" ✅ Write permission verified.")
            os.remove(test_file) # Clean up
        except Exception as e:
            print(f" ❌ Permission error: {e}")
    else:
        print(" ❌ Local 'raw' directory NOT found. Check your volume mapping.")

if __name__ == "__main__":
    main()