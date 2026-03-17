import os
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

def download_bitcoin_data():
    api = KaggleApi()
    api.authenticate()
    
    dataset = "mczielinski/bitcoin-historical-data"
    target_dir = os.path.join('..', 'data', 'raw', 'bitcoin')
    
    # Create subdirectory for cleanliness
    os.makedirs(target_dir, exist_ok=True)
    
    print(f"--- Starting Download: {dataset} ---")
    api.dataset_download_files(dataset, path=target_dir, unzip=True)
    print(f"✅ Bitcoin data saved and unzipped to: {target_dir}")


    # Standardize Column Names to Lowercase
    old_file = os.path.join(target_dir, 'btcusd_1-min_data.csv')
    if os.path.exists(old_file):
        print("🔍 Normalizing column names to lowercase...")
        df = pd.read_csv(old_file)
        df.columns = [c.lower() for c in df.columns]
        df.to_csv(old_file, index=False)
        print("✅ Column names normalized.")

    # Standardizing the filename for the pipeline
    old_file = os.path.join(target_dir, 'btcusd_1-min_data.csv')
    new_file = os.path.join(target_dir, 'bitcoin_historical_data.csv')
    
    if os.path.exists(old_file):
        os.rename(old_file, new_file)
        print(f"🔄 Filename changed to: bitcoin_historical_data.csv")
    else:
        # Check if the file was already renamed or has a slightly different name in the zip
        print(f"⚠️ Warning: Original CSV file not found. Please check {target_dir}")

if __name__ == "__main__":
    download_bitcoin_data()
