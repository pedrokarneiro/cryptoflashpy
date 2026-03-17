import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime
import time

def process_ethereum():
    # Record start time
    start_time = time.time()
    start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"--- Ethereum Transformation Started: {start_timestamp} ---")
    
    # Path Configuration
    raw_file = os.path.join('..', 'data', 'raw', 'ethereum', 'ethereum_historical_data.csv')
    
    if not os.path.exists(raw_file):
        print(f" ❌ Error: Ethereum raw file not found at {raw_file}")
        return

    # 1. Load Data
    print(" - Loading Ethereum CSV...")
    df = pd.read_csv(raw_file)

    # 2. Standardize Column Names & Types
    if 'date' in df.columns:
        print(" - Converting 'Date' strings to datetime objects...")
        df['timestamp'] = pd.to_datetime(df['date'])
        df = df.drop(columns=['date'])
    
    # Ensure columns are ordered with Timestamp first
    cols = ['timestamp'] + [c for c in df.columns if c != 'timestamp']
    df = df[cols]

    # 3. Save to MongoDB
    total_records = len(df)
    print(f" - Preparing {total_records:,} records for MongoDB...")
    
    try:
        # Connection to MongoDB container
        client = MongoClient('mongodb://mongodb:27017/')
        db = client['cryptoflash_db']
        collection = db['ethereum_history']

        # Clear old records
        print(" - Clearing old records in 'ethereum_history'...")
        collection.delete_many({})

        # 4. CHUNKED INSERTION (Memory Efficient)
        chunk_size = 50000
        
        # tqdm tweak: mininterval=5 prevents flooding the pipeline log
        with tqdm(total=total_records, desc="💾 Injecting Ethereum Data", unit="rows", mininterval=5) as pbar:
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

        print(f"\n--- Ethereum Transformation Finished: {end_timestamp} ---")
        print(f"⏱️ Total time elapsed: {duration_minutes:.2f} minutes")
        print(f" ✅ Ethereum processing complete. Collection: {collection.full_name}")
        
    except Exception as e:
        print(f" ❌ MongoDB Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    process_ethereum()
    