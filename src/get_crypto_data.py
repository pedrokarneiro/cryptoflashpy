"""
get_crypto_data.py  —  Generic 5-asset Kaggle downloader
=========================================================
Usage:
  python get_crypto_data.py          # download ALL 5 assets
  python get_crypto_data.py btc      # download a single asset by currency code
  python get_crypto_data.py --force  # re-download even if files already exist
"""
import os
import sys
import argparse
import glob
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from market_assets import MARKET_ASSETS, asset_by_currency, raw_asset_dir, raw_csv_path, MarketAsset


# ── Column normalisation ───────────────────────────────────────────────────────

REQUIRED_SCHEMA = ["timestamp", "open", "high", "low", "close", "volume"]

# Maps known alias column names → canonical name
COLUMN_ALIASES: dict[str, str] = {
    "unix":         "timestamp",
    "unix_time":    "timestamp",
    "date":         "timestamp",
    "time":         "timestamp",
    "open time":    "timestamp",
    "open_time":    "timestamp",
    "vol":          "volume",
    "vol_btc":      "volume",
    "vol_eth":      "volume",
    "vol_ltc":      "volume",
    "vol_xrp":      "volume",
    "vol_bch":      "volume",
    "volume_(btc)": "volume",
    "volume_(eth)": "volume",
    "volume_(ltc)": "volume",
    "volume_(xrp)": "volume",
    "volume_(bch)": "volume",
}


def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip whitespace, apply known aliases."""
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=COLUMN_ALIASES)
    return df


def find_raw_csv(target_dir: str, preferred_name: str | None) -> str | None:
    """
    Locate the raw CSV inside `target_dir`.
    1. Try the preferred name first (as given by MarketAsset.raw_filename).
    2. Fall back to any single .csv in the directory.
    """
    if preferred_name:
        candidate = os.path.join(target_dir, preferred_name)
        if os.path.exists(candidate):
            return candidate

    # Glob fallback
    csvs = glob.glob(os.path.join(target_dir, "*.csv"))
    if len(csvs) == 1:
        return csvs[0]
    if len(csvs) > 1:
        # Try to pick one that looks most like OHLCV data by size
        csvs.sort(key=os.path.getsize, reverse=True)
        print(f"  ⚠️  Multiple CSVs found — using largest: {os.path.basename(csvs[0])}")
        return csvs[0]

    return None


# ── Per-asset pipeline ─────────────────────────────────────────────────────────

def download_asset(api: KaggleApi, asset: MarketAsset, force: bool = False) -> bool:
    """Download, normalise columns, and rename one asset. Returns True on success."""
    canonical = raw_csv_path(asset)
    target_dir = str(raw_asset_dir(asset))

    # Skip if already done
    if canonical.exists() and not force:
        print(f"  ✅ [{asset.symbol}] Already downloaded at {canonical}. Use --force to re-download.")
        return True

    os.makedirs(target_dir, exist_ok=True)
    print(f"\n{'='*60}")
    print(f"  📥 [{asset.symbol}] Downloading: {asset.kaggle_dataset}")
    print(f"{'='*60}")

    try:
        api.dataset_download_files(asset.kaggle_dataset, path=target_dir, unzip=True, quiet=False)
        print(f"  ✅ [{asset.symbol}] Download complete → {target_dir}")
    except Exception as e:
        print(f"  ❌ [{asset.symbol}] Download failed: {e}")
        return False

    # Locate the actual CSV inside the unzipped directory
    raw_file = find_raw_csv(target_dir, asset.raw_filename)
    if raw_file is None:
        print(f"  ❌ [{asset.symbol}] No CSV found in {target_dir}. Check kaggle_dataset or raw_filename in market_assets.py")
        return False

    # Normalise column names
    print(f"  🔍 [{asset.symbol}] Normalising columns in {os.path.basename(raw_file)} …")
    try:
        df = pd.read_csv(raw_file, low_memory=False)
        df = normalise_columns(df)

        # Validate that required columns exist
        missing = [c for c in REQUIRED_SCHEMA if c not in df.columns]
        if missing:
            print(f"  ⚠️  [{asset.symbol}] Missing columns after normalisation: {missing}")
            print(f"       Available columns: {list(df.columns)}")
            print(f"       Add aliases in COLUMN_ALIASES or fix market_assets.py → raw_filename")
            # Still save what we have so the user can inspect it
        else:
            print(f"  ✅ [{asset.symbol}] All required columns present.")

        df.to_csv(raw_file, index=False)
        print(f"  💾 [{asset.symbol}] Column-normalised CSV saved.")
    except Exception as e:
        print(f"  ❌ [{asset.symbol}] Column normalisation failed: {e}")
        return False

    # Rename to canonical filename
    if raw_file != str(canonical):
        os.rename(raw_file, canonical)
        print(f"  🔄 [{asset.symbol}] Renamed → {canonical.name}")
    else:
        print(f"  ✅ [{asset.symbol}] File already has canonical name.")

    return True


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Download Kaggle crypto datasets for CryptoFlash")
    parser.add_argument(
        "currency", nargs="?", default=None,
        help="Currency code to download (btc|eth|ltc|xrp|bch). Omit for all 5."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download even if the canonical file already exists."
    )
    args = parser.parse_args()

    api = KaggleApi()
    api.authenticate()

    if args.currency:
        try:
            assets = [asset_by_currency(args.currency)]
        except KeyError as e:
            print(f"❌ {e}")
            sys.exit(1)
    else:
        assets = MARKET_ASSETS

    successes, failures = [], []
    for asset in assets:
        ok = download_asset(api, asset, force=args.force)
        (successes if ok else failures).append(asset.symbol)

    print(f"\n{'='*60}")
    print(f"  📊 Download Summary")
    print(f"{'='*60}")
    if successes:
        print(f"  ✅ Success ({len(successes)}): {', '.join(successes)}")
    if failures:
        print(f"  ❌ Failed  ({len(failures)}): {', '.join(failures)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
