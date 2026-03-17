import os
import pandas as pd
from pymongo import MongoClient
import sys

def run_checks():
    print("--- 🛠️ CryptoFlash Environment Sanity Check ---")

    # 1. Check Pandas
    print(f"✅ Pandas Version: {pd.__version__}")

    # 2. Check Environment Variables
    user = os.getenv('KAGGLE_USERNAME')
    print(f"✅ Kaggle User: {user if user else '❌ NOT FOUND'}")

    # 3. Check MongoDB Connection
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://mongodb:27017/')
    print(f"🔗 Attempting connection to: {mongo_uri}")
    
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        # The 'admin' command is a fast way to ping the server
        client.admin.command('ping')
        print("✅ MongoDB: Connected Successfully!")
    except Exception as e:
        print(f"❌ MongoDB: Connection Failed. Error: {e}")
    finally:
        client.close()

    print("--- Check Complete ---")

if __name__ == "__main__":
    run_checks()