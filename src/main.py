# src/main.py
import sys, subprocess, time, shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]   # .../fact-tables
SRC_DIR  = ROOT_DIR / "src"
INPUT_DIR = ROOT_DIR / "input"
WORK_DIR  = ROOT_DIR / "work"

def run_scripts_in_order():
    # Clean slate (ignore if missing)
    shutil.rmtree(INPUT_DIR, ignore_errors=True)
    shutil.rmtree(WORK_DIR,  ignore_errors=True)

    start_time = time.perf_counter()

    def run_py(path_relative_to_src: str):
        script_path = SRC_DIR / path_relative_to_src
        subprocess.run([sys.executable, str(script_path)], check=True, cwd=str(ROOT_DIR))

    # Run in order (paths are relative to src/)
    run_py("utils/report.py")
    run_py("utils/update.py")
    run_py("utils/latest.py")

    elapsed_minutes = (time.perf_counter() - start_time) / 60
    print(f"\n[OK] Updated wait_time_fact_table in {elapsed_minutes:.1f} minutes.\n")

if __name__ == "__main__":
    run_scripts_in_order()
