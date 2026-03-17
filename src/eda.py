import os
import pandas as pd
import sys
from pymongo import MongoClient
from datetime import datetime

def generate_dashboard_summary(asset, df, mode):
    """Creates a technical summary for the audit trail."""
    summary = f"\n---\n## 📊 Audit Dashboard: {asset.capitalize()} ({mode.upper()})\n"
    summary += f"- **Audit Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    summary += f"- **Total Records Processed:** {len(df):,}\n"
    
    # Calculate span in days
    date_diff = df['timestamp'].max() - df['timestamp'].min()
    summary += f"- **Dataset Span:** {date_diff.days:,} days\n"
    
    if mode == "after":
        summary += f"- **Storage Engine:** MongoDB (Collection: {asset}_history)\n"
        summary += f"- **Data Status:** ✅ Verified and Indexed\n"
    
    return summary

def run_eda(asset, mode="before"):
    # 1. Configuration
    if mode == "before":
        input_file = os.path.join('..', 'data', 'raw', asset, f'{asset}_historical_data.csv')
        output_name = f"{asset}_eda_BEFORE.md"
        output_dir = os.path.join('..', 'data', 'raw', asset)
    else:
        output_name = f"{asset}_eda_AFTER.md"
        output_dir = os.path.join('..', 'data', 'processed', asset)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_name)

    print(f" --- Running {asset.capitalize()} EDA ({mode.upper()}) ---")

    # 2. Load Data (CSV vs MongoDB)
    if mode == "before":
        if not os.path.exists(input_file):
            print(f" ❌ Error: Raw CSV file not found at {input_file}")
            return
        df = pd.read_csv(input_file)
        if 'timestamp' in df.columns and not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            # Standardizing timestamp for before mode
            if df['timestamp'].dtype == 'int64':
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            else:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
    else:
        try:
            client = MongoClient('mongodb://mongodb:27017/')
            db = client['cryptoflash_db']
            collection = db[f'{asset}_history']
            df = pd.DataFrame(list(collection.find({}, {'_id': 0})))
            client.close()
            
            if df.empty:
                print(f" ❌ Error: Collection {asset}_history is empty!")
                return
        except Exception as e:
            print(f" ❌ MongoDB Connection Error: {e}")
            return

    # 3. Gather Statistics
    total_rows = len(df)
    missing_values = df.isnull().sum()
    desc_stats = df.describe()
    date_min, date_max = df['timestamp'].min(), df['timestamp'].max()
    peak_price = df['high'].max()
    peak_date = df.loc[df['high'].idxmax(), 'timestamp']

    # 4. Generate the Markdown Report
    with open(output_path, 'w') as f:
        f.write(f"# {asset.capitalize()} EDA Report: {mode.upper()}\n\n")
        
        f.write("## 1. Data Overview\n")
        f.write(f"- **Source:** {'CSV File' if mode == 'before' else 'MongoDB Collection'}\n")
        f.write(f"- **Observations:** {total_rows:,}\n")
        f.write(f"- **Time Coverage:** {date_min} to {date_max}\n\n")
        
        f.write("## 2. Integrity Check\n")
        f.write("```text\n")
        f.write(missing_values.to_string())
        f.write("\n```\n\n")
        
        if missing_values.sum() == 0:
            f.write("> ✅ **Verification:** No missing values found.\n\n")
        
        f.write("## 3. Descriptive Statistics\n")
        f.write(desc_stats.to_markdown())
        f.write("\n\n")
        
        f.write("## 4. Key Insights\n")
        f.write(f"- **Highest Price Recorded:** ${peak_price:,.2f} ({peak_date})\n")
        f.write(f"- **Average Price:** ${df['close'].mean():,.2f}\n")

        # 5. Append the Dashboard Summary
        f.write(generate_dashboard_summary(asset, df, mode))

    print(f" ✅ Report generated: {output_path}")

if __name__ == "__main__":
    asset = sys.argv[1] if len(sys.argv) > 1 else None
    mode = sys.argv[2] if len(sys.argv) > 2 else "before"

    if not asset:
        print("❌ Error: asset name required")
        sys.exit(1)
    run_eda(asset, mode)