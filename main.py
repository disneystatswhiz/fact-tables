# main.py
import os
import sys
import subprocess
import time
import shutil
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parent
INPUT_DIR = REPO_DIR / "input"
WORK_DIR = REPO_DIR / "work"

def run_scripts_in_order():
    # Clean slate (ignore if missing)
    shutil.rmtree(INPUT_DIR, ignore_errors=True)
    shutil.rmtree(WORK_DIR,  ignore_errors=True)

    start_time = time.perf_counter()

    def run_py(script_name: str):
        script_path = REPO_DIR / script_name
        subprocess.run([sys.executable, str(script_path)], check=True, cwd=str(REPO_DIR))

    # Run in order
    run_py("report.py")
    run_py("update.py")
    run_py("latest.py")

    elapsed_minutes = (time.perf_counter() - start_time) / 60
    print(f"\n[OK] Updated wait_time_fact_table in {elapsed_minutes:.1f} minutes.\n")

if __name__ == "__main__":
    run_scripts_in_order()
