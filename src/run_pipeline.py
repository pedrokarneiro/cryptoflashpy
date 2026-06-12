"""
run_pipeline.py  —  CryptoFlash 5-asset orchestrator
=====================================================
Runs: env check → download all 5 assets → process each to MongoDB → EDA before + after.
Aborts the full pipeline if any step fails.
"""
import subprocess
import sys
import os
import time

from check_env import run_checks
from market_assets import MARKET_ASSETS


def run_task(script: str, args: list[str] = []) -> bool:
    """Execute a script as a subprocess. Returns True on success."""
    cmd = [sys.executable, script] + args
    label = " ".join(cmd)
    print(f"\n[RUNNING] {label}")

    result = subprocess.run(cmd, capture_output=True, text=True)

    with open("pipeline_execution.log", "a") as log:
        log.write(f"\n--- {label} ---\n")
        log.write(result.stdout)
        if result.stderr:
            log.write(f"STDERR:\n{result.stderr}")

    if result.returncode != 0:
        # Echo stderr to console so failures are visible immediately
        if result.stderr:
            print(result.stderr[-2000:])  # last 2000 chars to avoid flooding
    return result.returncode == 0


def main():
    # 1. Reset log ─────────────────────────────────────────────────────────────
    if os.path.exists("pipeline_execution.log"):
        os.remove("pipeline_execution.log")

    print("🚀 Starting CryptoFlash 5-Asset Pipeline…\n")

    # 2. Gatekeeper ────────────────────────────────────────────────────────────
    try:
        run_checks()
    except EnvironmentError as e:
        print(f"🚨 Environment check failed: {e}")
        print("Pipeline aborted. Check MongoDB, KAGGLE_USERNAME, and KAGGLE_KEY.")
        sys.exit(1)

    # 3. Build task list ───────────────────────────────────────────────────────
    #    Phase 1: Download all 5 assets (one call to the generic script each)
    #    Phase 2: Process each to MongoDB
    #    Phase 3: EDA before + after for each asset
    tasks: list[tuple[str, list[str]]] = []

    # Download phase (all 5 assets in one invocation is also valid, but running
    # per-asset gives cleaner pipeline failure isolation)
    for asset in MARKET_ASSETS:
        tasks.append(("get_crypto_data.py", [asset.currency]))

    # Process + EDA phase
    for asset in MARKET_ASSETS:
        tasks.append(("eda.py", [asset.currency, "before"]))
        tasks.append(("process_crypto_to_mongodb.py", [asset.currency]))
        tasks.append(("eda.py", [asset.currency, "after"]))

    # 4. Execution loop ────────────────────────────────────────────────────────
    start_time = time.time()
    completed  = 0

    for script, args in tasks:
        if not os.path.exists(script):
            print(f"⚠️  Warning: {script} not found in the current directory. Skipping…")
            continue

        if not run_task(script, args):
            print(f"\n❌ Pipeline FAILED at: {script} {' '.join(args)}")
            print("   Review 'pipeline_execution.log' for the full traceback.")
            sys.exit(1)

        completed += 1
        print(f"✅ [{completed}/{len(tasks)}] {script} {' '.join(args)} — OK")

    # 5. Summary ───────────────────────────────────────────────────────────────
    duration = (time.time() - start_time) / 60
    print("\n" + "=" * 60)
    print(f"✨ ALL {completed} TASKS COMPLETE!")
    print(f"⏱️  Total Execution Time: {duration:.2f} minutes")
    print(f"📋 Full details in: pipeline_execution.log")
    print("=" * 60)


if __name__ == "__main__":
    main()