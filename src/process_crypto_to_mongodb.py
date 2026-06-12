"""
process_crypto_to_mongodb.py  —  Generic 5-asset CSV → MongoDB (market_candles)
================================================================================
Writes into the shared `market_candles` collection with a `symbol` field so the
Java MarketReplayService can query by symbol using the compound index
{ symbol: 1, timestamp: 1 }.

Usage:
  python process_crypto_to_mongodb.py          # process ALL 5 assets
  python process_crypto_to_mongodb.py btc      # process single asset by currency code
  python process_crypto_to_mongodb.py --help
"""
import os
import sys
import argparse
import time
from datetime import datetime

import pandas as pd
from pymongo import MongoClient, ASCENDING
from tqdm import tqdm

from market_assets import MARKET_ASSETS, asset_by_currency, raw_csv_path, MarketAsset

# ── Constants ──────────────────────────────────────────────────────────────────

MONGO_URI       = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME         = "cryptoflash_db"
COLLECTION_NAME = "market_candles"   # shared collection — matches Java MarketCandle
CHUNK_SIZE      = 50_000
SCHEMA_COLS     = ["timestamp", "open", "high", "low", "close", "volume"]


# ── Timestamp handling ─────────────────────────────────────────────────────────

def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the `timestamp` column to UTC datetime regardless of input format:
      - Unix int (seconds)  →  pd.to_datetime(..., unit='s')
      - Unix int (millis)   →  pd.to_datetime(..., unit='ms')
      - ISO / date string   →  pd.to_datetime(..., utc=True)
    """
    ts = df["timestamp"]

    if pd.api.types.is_integer_dtype(ts) or (
        pd.api.types.is_float_dtype(ts) and ts.dropna().iloc[0] > 1e12
    ):
        # Distinguish seconds vs milliseconds by magnitude
        sample = ts.dropna().iloc[0]
        if sample > 1e12:
            print("   - Detected Unix millisecond timestamps → converting…")
            df["timestamp"] = pd.to_datetime(ts, unit="ms", utc=True)
        else:
            print("   - Detected Unix second timestamps → converting…")
            df["timestamp"] = pd.to_datetime(ts, unit="s", utc=True)
    else:
        print("   - Detected string timestamps → parsing with UTC…")
        df["timestamp"] = pd.to_datetime(ts, utc=True)

    # Strip timezone for MongoDB storage (stored as naive UTC)
    df["timestamp"] = df["timestamp"].dt.tz_localize(None)
    return df


# ── Per-asset pipeline ─────────────────────────────────────────────────────────

def process_asset(asset: MarketAsset, client: MongoClient, replace: bool = True) -> bool:
    """Load, clean, and insert one asset into market_candles. Returns True on success."""
    start_time = time.time()
    start_ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'='*60}")
    print(f"  🚀 [{asset.symbol}] Processing started: {start_ts}")
    print(f"{'='*60}")

    csv_path = raw_csv_path(asset)
    if not csv_path.exists():
        print(f"  ❌ [{asset.symbol}] CSV not found at {csv_path}")
        print(f"       Run: python get_crypto_data.py {asset.currency}")
        return False

    # 1. Load ──────────────────────────────────────────────────────────────────
    print(f"  📂 [{asset.symbol}] Loading {csv_path.name} …")
    df = pd.read_csv(csv_path, low_memory=False)
    print(f"       Rows loaded: {len(df):,}")

    # 2. Column check ──────────────────────────────────────────────────────────
    missing = [c for c in SCHEMA_COLS if c not in df.columns]
    if missing:
        print(f"  ❌ [{asset.symbol}] Missing required columns: {missing}")
        print(f"       Available: {list(df.columns)}")
        print(f"       Hint: run get_crypto_data.py --force to re-normalise column names.")
        return False

    df = df[SCHEMA_COLS].copy()

    # 3. Timestamps ────────────────────────────────────────────────────────────
    df = parse_timestamps(df)

    # 4. Type enforcement ──────────────────────────────────────────────────────
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5. Drop nulls ────────────────────────────────────────────────────────────
    null_count = df.isnull().any(axis=1).sum()
    if null_count > 0:
        print(f"  🗑️  [{asset.symbol}] Dropping {null_count:,} rows with missing values…")
        df = df.dropna()

    # 6. Sort chronologically ──────────────────────────────────────────────────
    df = df.sort_values("timestamp").reset_index(drop=True)

    # 7. Add symbol column (required by Java MarketReplayService) ──────────────
    df.insert(0, "symbol", asset.symbol)

    total_records = len(df)
    print(f"  📊 [{asset.symbol}] Clean rows to insert: {total_records:,}")
    print(f"       Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")

    # 8. MongoDB insert ────────────────────────────────────────────────────────
    db         = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    if replace:
        print(f"  🗄️  [{asset.symbol}] Removing existing documents in {COLLECTION_NAME} …")
        deleted = collection.delete_many({"symbol": asset.symbol}).deleted_count
        print(f"       Removed {deleted:,} old documents.")

    # Ensure compound index exists (idempotent)
    collection.create_index(
        [("symbol", ASCENDING), ("timestamp", ASCENDING)],
        name="symbol_ts_idx",
        background=True,
    )

    print(f"  💾 [{asset.symbol}] Inserting {total_records:,} records …")
    errors = 0
    with tqdm(total=total_records, desc=f"  [{asset.symbol}]", unit="rows", mininterval=5) as pbar:
        for i in range(0, total_records, CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE].to_dict("records")
            if not chunk:
                continue
            try:
                collection.insert_many(chunk, ordered=False)
            except Exception as e:
                errors += 1
                print(f"\n  ⚠️  [{asset.symbol}] Chunk {i//CHUNK_SIZE} error: {e}")
            pbar.update(len(chunk))

    elapsed = (time.time() - start_time) / 60
    print(f"\n  ✅ [{asset.symbol}] Done — {total_records:,} rows, {errors} chunk errors.")
    print(f"  ⏱️  [{asset.symbol}] Elapsed: {elapsed:.2f} min")
    return errors == 0


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Process raw crypto CSVs into MongoDB market_candles collection"
    )
    parser.add_argument(
        "currency", nargs="?", default=None,
        help="Currency code (btc|eth|ltc|xrp|bch). Omit to process all 5."
    )
    parser.add_argument(
        "--append", action="store_true",
        help="Append instead of replacing existing documents for the symbol."
    )
    args = parser.parse_args()

    if args.currency:
        try:
            assets = [asset_by_currency(args.currency)]
        except KeyError as e:
            print(f"❌ {e}")
            sys.exit(1)
    else:
        assets = MARKET_ASSETS

    print(f"🔗 Connecting to MongoDB at {MONGO_URI} …")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print("✅ MongoDB connected.")
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        sys.exit(1)

    successes, failures = [], []
    try:
        for asset in assets:
            ok = process_asset(asset, client, replace=not args.append)
            (successes if ok else failures).append(asset.symbol)
    finally:
        client.close()

    print(f"\n{'='*60}")
    print(f"  📊 Processing Summary")
    print(f"{'='*60}")
    if successes:
        print(f"  ✅ Success ({len(successes)}): {', '.join(successes)}")
    if failures:
        print(f"  ❌ Failed  ({len(failures)}): {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
