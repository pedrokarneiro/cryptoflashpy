import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime # Added for timestamping
import time # Added for duration calculation

def process_bitcoin():
    # Record start time
    start_time = time.time()
    start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"--- Bitcoin Transformation Started: {start_timestamp} ---")
    
    raw_file = os.path.join('..', 'data', 'raw', 'bitcoin', 'bitcoin_historical_data.csv')
    
    if not os.path.exists(raw_file):
        print(f" ❌ Error: Bitcoin raw file not found at {raw_file}")
        return

    # 1. Load Data
    print(" - Loading CSV...")
    df = pd.read_csv(raw_file)

    # 2. Handle Timestamps and column order
    if 'timestamp' in df.columns:
        print(" - Converting Unix timestamps to datetime objects...")
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    
    # ...ensure column order (in case we have other cryptocurrencies with different schemas in the future)
    schema_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    df = df[schema_cols]
    
    # 3. Handle Null Values
    null_count = df.isnull().any(axis=1).sum()
    if null_count > 0:
        print(f" - Found {null_count:,} rows with missing values. Dropping them...")
        df = df.dropna()

    # 4. Save to MongoDB
    total_records = len(df)
    print(f" - Preparing {total_records:,} records for MongoDB...")
    
    try:
        # Connection to MongoDB container
        client = MongoClient('mongodb://mongodb:27017/')
        db = client['cryptoflash_db']
        collection = db['bitcoin_history']

        # Clear old records
        print(" - Clearing old records in 'bitcoin_history'...")
        collection.delete_many({})

        # 5. CHUNKED INSERTION (Memory Efficient)
        chunk_size = 50000 
        
        # tqdm tweak: mininterval=5 prevents flooding the pipeline log
        with tqdm(total=total_records, desc="💾 Injecting Bitcoin Data", unit="rows", mininterval=5) as pbar:
            for i in range(0, total_records, chunk_size):
                # Slice the DataFrame and convert ONLY this chunk to dicts
                chunk = df.iloc[i : i + chunk_size].to_dict('records')
                if chunk:
                    collection.insert_many(chunk)
                    pbar.update(len(chunk))

        # Record end time
        end_time = time.time()
        end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration_minutes = (end_time - start_time) / 60

        print(f"\n--- Bitcoin Transformation Finished: {end_timestamp} ---")
        print(f"⏱️ Total time elapsed: {duration_minutes:.2f} minutes")
        print(f" ✅ MongoDB injection complete. Collection: {collection.full_name}")
        
    except Exception as e:
        print(f" ❌ MongoDB Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    process_bitcoin()