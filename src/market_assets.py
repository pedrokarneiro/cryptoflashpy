from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MarketAsset:
    symbol: str           # e.g. "BTCUSD"
    currency: str         # e.g. "btc"  — used as MongoDB sub-dir and wallet currency
    raw_prefix: str       # legacy: same as currency
    replay_buyer_id: str
    replay_seller_id: str
    kaggle_dataset: str        # Kaggle dataset slug for download
    raw_filename: str          # Filename as it appears inside the downloaded zip
    canonical_filename: str    # Standardised filename after rename


# ── 5 supported market assets ──────────────────────────────────────────────────
#
# Kaggle slugs and raw filenames to verify / replace if you use different sources:
#   BTC  – mczielinski/bitcoin-historical-data    → btcusd_1-min_data.csv
#   ETH  – viniciusqroz/ethereum-historical-data  → ethusd_1min_ohlc.csv
#   LTC  – mczielinski/litecoin-historical-data   → ltcusd_1-min_data.csv
#   XRP  – quanticdata/xrp-ripple-historical-data → xrp_usd_d.csv
#   BCH  – prasoonkottarathil/bitcoin-cash-price-daily → BCH_USD_d.csv
#
# If a raw_filename is None the downloader will scan the zip for the first .csv.
# ──────────────────────────────────────────────────────────────────────────────
MARKET_ASSETS = [
    MarketAsset(
        symbol="BTCUSD", currency="btc", raw_prefix="btc",
        replay_buyer_id="SIM_BUYER_BTCUSD", replay_seller_id="SIM_SELLER_BTCUSD",
        kaggle_dataset="mczielinski/bitcoin-historical-data",
        raw_filename="btcusd_1-min_data.csv",
        canonical_filename="btc_historical_data.csv",
    ),
    MarketAsset(
        symbol="ETHUSD", currency="eth", raw_prefix="eth",
        replay_buyer_id="SIM_BUYER_ETHUSD", replay_seller_id="SIM_SELLER_ETHUSD",
        kaggle_dataset="viniciusqroz/ethereum-historical-data",
        raw_filename="ethusd_1min_ohlc.csv",
        canonical_filename="eth_historical_data.csv",
    ),
    MarketAsset(
        symbol="LTCUSD", currency="ltc", raw_prefix="ltc",
        replay_buyer_id="SIM_BUYER_LTCUSD", replay_seller_id="SIM_SELLER_LTCUSD",
        kaggle_dataset="prasoonkottarathil/litecoin-historical-and-tradeprint-dataset",
        raw_filename="LTC_USD_1MIN.csv",
        canonical_filename="ltc_historical_data.csv",
    ),
    MarketAsset(
        symbol="XRPUSD", currency="xrp", raw_prefix="xrp",
        replay_buyer_id="SIM_BUYER_XRPUSD", replay_seller_id="SIM_SELLER_XRPUSD",
        kaggle_dataset="imranbukhari/comprehensive-xrpusd-1m-data",
        raw_filename="XRPUSD_1m_Binance.csv",
        canonical_filename="xrp_historical_data.csv",
    ),
    MarketAsset(
        symbol="BCHUSD", currency="bch", raw_prefix="bch",
        replay_buyer_id="SIM_BUYER_BCHUSD", replay_seller_id="SIM_SELLER_BCHUSD",
        kaggle_dataset="mjdskaggle/5-years-of-crypto-data-as-of-632024",
        raw_filename="BCH-USD.csv",
        canonical_filename="bch_historical_data.csv",
    ),
]

# Quick lookup helpers
_BY_CURRENCY = {a.currency: a for a in MARKET_ASSETS}
_BY_SYMBOL   = {a.symbol:   a for a in MARKET_ASSETS}


def asset_by_currency(currency: str) -> MarketAsset:
    """Return the MarketAsset matching `currency` (e.g. 'btc'). Raises KeyError."""
    a = _BY_CURRENCY.get(currency.lower())
    if a is None:
        valid = list(_BY_CURRENCY.keys())
        raise KeyError(f"Unknown currency '{currency}'. Valid options: {valid}")
    return a


def asset_by_symbol(symbol: str) -> MarketAsset:
    """Return the MarketAsset matching `symbol` (e.g. 'BTCUSD'). Raises KeyError."""
    a = _BY_SYMBOL.get(symbol.upper())
    if a is None:
        valid = list(_BY_SYMBOL.keys())
        raise KeyError(f"Unknown symbol '{symbol}'. Valid options: {valid}")
    return a


# ── Path helpers ───────────────────────────────────────────────────────────────

def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def raw_monthly_dir() -> Path:
    return project_root() / "data" / "raw" / "crypto_monthly"


def raw_asset_dir(asset: MarketAsset) -> Path:
    return project_root() / "data" / "raw" / asset.currency


def raw_csv_path(asset: MarketAsset) -> Path:
    return raw_asset_dir(asset) / asset.canonical_filename
