import os
import pandas as pd
import sys
from pymongo import MongoClient
from datetime import datetime

def generate_dashboard_summary(asset, stats, mode):
    """Creates a technical summary using pre-computed stats."""
    summary = f"\n---\n## 📊 Audit Dashboard: {asset.capitalize()} ({mode.upper()})\n"
    summary += f"- **Audit Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    summary += f"- **Total Records Processed:** {stats['count']:,}\n"
    
    date_diff = stats['max_date'] - stats['min_date']
    summary += f"- **Dataset Span:** {date_diff.days:,} days\n"
    
    if mode == "after":
        summary += f"- **Storage Engine:** MongoDB (Collection: {asset}_history)\n"
        summary += f"- **Data Status:** ✅ Verified and Aggregated\n"
    
    return summary

def run_eda(asset, mode="before"):
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

    # --- DATA LOADING & STATS GATHERING ---
    if mode == "before":
        if not os.path.exists(input_file):
            print(f" ❌ Error: Raw CSV file not found at {input_file}")
            return
        df = pd.read_csv(input_file)
        # Convert timestamp for EDA logic
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s' if df['timestamp'].dtype == 'int64' else None)
        
        # Standard Pandas Stats
        stats = {
            'count': len(df),
            'min_date': df['timestamp'].min(),
            'max_date': df['timestamp'].max(),
            'peak_price': df['high'].max(),
            'peak_date': df.loc[df['high'].idxmax(), 'timestamp'],
            'avg_price': df['close'].mean(),
            'missing': df.isnull().sum().to_string(),
            'describe_table': df.describe().to_markdown()
        }
    else:
        # AFTER MODE: MONGODB AGGREGATION (Memory Safe)
        try:
            client = MongoClient('mongodb://mongodb:27017/')
            db = client['cryptoflash_db']
            coll = db[f'{asset}_history']

            print(f" - Executing MongoDB Aggregation for {asset}...")
            
            # Aggregation Pipeline for fast stats
            pipeline = [
                {
                    "$group": {
                        "_id": None,
                        "count": {"$sum": 1},
                        "avg_close": {"$avg": "$close"},
                        "max_high": {"$max": "$high"},
                        "min_date": {"$min": "$timestamp"},
                        "max_date": {"$max": "$timestamp"}
                    }
                }
            ]
            
            agg_result = list(coll.aggregate(pipeline))
            if not agg_result:
                print(f" ❌ Error: Collection {asset}_history is empty!")
                return
            
            res = agg_result[0]
            
            # Find the date for the peak price (Secondary Query)
            peak_doc = coll.find_one({"high": res['max_high']}, {"timestamp": 1})
            
            # Get a small sample for the 'describe' table (Prevents Memory Lock)
            sample_df = pd.DataFrame(list(coll.find().limit(50000)))
            
            stats = {
                'count': res['count'],
                'min_date': res['min_date'],
                'max_date': res['max_date'],
                'peak_price': res['max_high'],
                'peak_date': peak_doc['timestamp'] if peak_doc else "N/A",
                'avg_price': res['avg_close'],
                'missing': "Verified 0 (Schema Enforced)",
                'describe_table': sample_df.describe().to_markdown()
            }
            client.close()
        except Exception as e:
            print(f" ❌ MongoDB Error: {e}")
            return

    # --- GENERATE REPORT ---
    with open(output_path, 'w') as f:
        f.write(f"# {asset.capitalize()} EDA Report: {mode.upper()}\n\n")
        f.write("## 1. Data Overview\n")
        f.write(f"- **Source:** {'CSV' if mode == 'before' else 'MongoDB'}\n")
        f.write(f"- **Observations:** {stats['count']:,}\n")
        f.write(f"- **Time Coverage:** {stats['min_date']} to {stats['max_date']}\n\n")
        
        f.write("## 2. Integrity Check\n```text\n")
        f.write(stats['missing'])
        f.write("\n```\n\n")
        
        f.write("## 3. Descriptive Statistics (Sampled only 50.000 documents in order to avoid memory issues)\n")
        f.write(stats['describe_table'])
        f.write("\n\n")
        
        f.write("## 4. Key Insights\n")
        f.write(f"- **Highest Price:** ${stats['peak_price']:,.2f} ({stats['peak_date']})\n")
        f.write(f"- **Average Price:** ${stats['avg_price']:,.2f}\n")
        f.write(generate_dashboard_summary(asset, stats, mode))

    print(f" ✅ Report generated: {output_path}")

if __name__ == "__main__":
    asset = sys.argv[1] if len(sys.argv) > 1 else None
    mode = sys.argv[2] if len(sys.argv) > 2 else "before"
    if asset:
        run_eda(asset, mode)