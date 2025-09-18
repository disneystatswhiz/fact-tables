import os
import subprocess
import time

def run_scripts_in_order():
    # List all files in the current directory
    files = os.listdir(".")

    # Filter only Python scripts that start with 01 through 08
    scripts = [f for f in files if f.endswith(".py") and f[:2].isdigit() and 1 <= int(f[:2]) <= 8]

    # Sort scripts by their numeric prefix
    scripts.sort()

    start_time = time.perf_counter()

    # Run each script
    for script in scripts:
        print(f"Running {script}...")
        subprocess.run(["python", script], check=True)

    end_time = time.perf_counter()
    elapsed_minutes = (end_time - start_time) / 60
    print(f"\n✅ Updated wait time fact table in {elapsed_minutes:.1f} minutes.")

if __name__ == "__main__":
    run_scripts_in_order()
