import subprocess
import sys
import time
from check_env import run_checks

def run_script(script_name):
    print(f"\n🚀 Executing: {script_name}...")
    try:
        # We use 'python' to run the sub-scripts
        result = subprocess.run(['python', script_name], check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error in {script_name}. Pipeline halted.")
        return False

def main():
    print("--- 🛰️ Starting CryptoFlash Pipeline Orchestration ---")
    
    # 1. First, run the Sanity Check
    try:
        run_checks()
    except Exception as e:
        print(f"🚨 Environment check failed: {e}")
        sys.exit(1)

    # 2. Define the Pipeline Order
    pipeline = [
        "get_bitcoin_data.py",
        "get_ethereum_data.py",
        "process_bitcoin.py",
        "process_ethereum.py"
    ]

    # 3. Execution Loop
    start_time = time.time()
    for script in pipeline:
        if not run_script(script):
            sys.exit(1)
    
    end_time = time.time()
    duration = (end_time - start_time) / 60
    
    print("\n" + "="*40)
    print(f"✨ ALL TASKS COMPLETE!")
    print(f"⏱️ Total Execution Time: {duration:.2f} minutes")
    print("="*40)

if __name__ == "__main__":
    main()