"""
check_env.py  —  CryptoFlash environment sanity check
======================================================
Raises SystemExit(1) on any critical failure so that run_pipeline.py aborts.
"""
import os
import sys
import pandas as pd
from pymongo import MongoClient


def run_checks():
    print("--- 🛠️  CryptoFlash Environment Sanity Check ---")
    errors = []

    # 1. Pandas ────────────────────────────────────────────────────────────────
    print(f"✅ Pandas Version: {pd.__version__}")

    # 2. Kaggle credentials ────────────────────────────────────────────────────
    kaggle_user = os.getenv("KAGGLE_USERNAME")
    kaggle_key  = os.getenv("KAGGLE_KEY")

    if not kaggle_user or not kaggle_key:
        from pathlib import Path
        import json
        kaggle_json_path = Path.home() / ".kaggle" / "kaggle.json"
        if kaggle_json_path.exists():
            try:
                with open(kaggle_json_path, "r") as f:
                    creds = json.load(f)
                    if not kaggle_user:
                        kaggle_user = creds.get("username")
                        os.environ["KAGGLE_USERNAME"] = kaggle_user or ""
                    if not kaggle_key:
                        kaggle_key = creds.get("key")
                        os.environ["KAGGLE_KEY"] = kaggle_key or ""
            except Exception as e:
                print(f"⚠️  Failed to read ~/.kaggle/kaggle.json: {e}")

    if kaggle_user:
        print(f"✅ Kaggle User: {kaggle_user}")
    else:
        print("❌ KAGGLE_USERNAME not set — downloads will fail.")
        errors.append("KAGGLE_USERNAME missing")

    if kaggle_key:
        print("✅ Kaggle Key: [set]")
    else:
        print("❌ KAGGLE_KEY not set — downloads will fail.")
        errors.append("KAGGLE_KEY missing")

    # 3. MongoDB ───────────────────────────────────────────────────────────────
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        # Check if local MongoDB is up first, otherwise default to docker hostname
        try:
            client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=1000)
            client.admin.command("ping")
            mongo_uri = "mongodb://localhost:27017/"
            client.close()
        except Exception:
            mongo_uri = "mongodb://mongodb:27017/"

    print(f"🔗 Attempting MongoDB connection: {mongo_uri}")
    client = None
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        os.environ["MONGO_URI"] = mongo_uri  # Ensure other subprocesses use the same URI
        print("✅ MongoDB: Connected successfully!")
    except Exception as e:
        print(f"❌ MongoDB: Connection failed → {e}")
        errors.append(f"MongoDB unreachable: {e}")
    finally:
        if client:
            client.close()

    print("--- Check Complete ---")

    if errors:
        msg = f"Environment check failed ({len(errors)} issue(s)): " + "; ".join(errors)
        raise EnvironmentError(msg)


if __name__ == "__main__":
    try:
        run_checks()
    except EnvironmentError as e:
        print(f"\n🚨 {e}")
        sys.exit(1)