import subprocess
import sys
import os
import time
from check_env import run_checks  # This ensures our environment is ready

def run_task(script, args=[]):
    """Executes a script, captures output, and logs results to the file."""
    cmd = [sys.executable, script] + args
    print(f"\n[RUNNING] {' '.join(cmd)}")
    
    # We use capture_output=True so the terminal stays clean, but the log gets the details
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Save output to log
    with open("pipeline_execution.log", "a") as log:
        log.write(f"\n--- {script} {' '.join(args)} ---\n")
        log.write(result.stdout)
        if result.stderr:
            log.write(f"ERRORS:\n{result.stderr}")
            
    return result.returncode == 0

def main():
    # 1. Initialize Logs
    if os.path.exists("pipeline_execution.log"):
        os.remove("pipeline_execution.log")

    print("🚀 Starting Data Audit Pipeline...")

    # 2. THE SANITY CHECK
    # This is the "Professional Gatekeeper": if this fails, nothing else runs.
    try:
        run_checks()
    except Exception as e:
        print(f"🚨 Environment check failed: {e}")
        print("Pipeline aborted. Check if MongoDB is running and libraries are installed.")
        sys.exit(1)

    # 3. Define the Pipeline Tasks
    # Note: Ensure the script names match your folder exactly
    tasks = [
        ("get_bitcoin_data.py", []),
        ("get_ethereum_data.py", []),
        ("eda.py", ["bitcoin", "before"]),
        ("eda.py", ["ethereum", "before"]),
        ("process_bitcoin_to_mongodb.py", []), 
        ("process_ethereum_to_mongodb.py", []),
        ("eda.py", ["bitcoin", "after"]),
        ("eda.py", ["ethereum", "after"])
    ]

    # 4. Execution Loop
    start_time = time.time()
    
    for script, args in tasks:
        # Check if the script exists before trying to run it
        if not os.path.exists(script):
            print(f"⚠️ Warning: {script} not found in the current directory. Skipping...")
            continue

        if not run_task(script, args):
            print(f"❌ Failed at {script}. Review 'pipeline_execution.log' for the traceback.")
            sys.exit(1) # Break the pipeline on any error
        else:
            print(f"✅ Task {script} finished successfully.")

    # 5. Final Summary
    duration = (time.time() - start_time) / 60
    print("\n" + "="*40)
    print(f"✨ ALL TASKS COMPLETE!")
    print(f"⏱️ Total Execution Time: {duration:.2f} minutes")
    print("📋 Full details saved in: pipeline_execution.log")
    print("="*40)

if __name__ == "__main__":
    main()