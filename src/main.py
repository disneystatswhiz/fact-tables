# src/main.py
import sys
import subprocess
import shutil
import logging
from pathlib import Path
from datetime import datetime
import time

# --- Paths ---
ROOT_DIR  = Path(__file__).resolve().parents[1]   # .../fact-tables
SRC_DIR   = ROOT_DIR / "src"
INPUT_DIR = ROOT_DIR / "input"
WORK_DIR  = ROOT_DIR / "work"
LOG_DIR   = ROOT_DIR / "logs"

# --- Logging ---
def setup_logging() -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = LOG_DIR / f"fact_table.log"
    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[logging.FileHandler(log_path, encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
    )
    logging.info("Logging to %s", log_path)
    return log_path

# --- Helpers ---
def clean_slate():
    """Remove transient folders so each run starts fresh."""
    shutil.rmtree(INPUT_DIR, ignore_errors=True)
    shutil.rmtree(WORK_DIR,  ignore_errors=True)
    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)

def run_py(path_relative_to_src: str):
    """Run a Python script located under src/ using the current interpreter."""
    script_path = SRC_DIR / path_relative_to_src
    logging.info("[STEP] %s", path_relative_to_src)
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=str(ROOT_DIR))

def run_once() -> int:
    start = time.perf_counter()
    logging.info("=== Fact Table Update: START ===")
    try:
        clean_slate()
        for rel in ("utils/report.py", "utils/update.py", "utils/latest.py"):
            run_py(rel)
        mins = (time.perf_counter() - start) / 60
        logging.info("=== Fact Table Update: COMPLETE in %.1f min ===", mins)
        return 0
    except subprocess.CalledProcessError as e:
        logging.exception("A step failed (exit code %s). Aborting.", e.returncode)
        return e.returncode or 1
    except Exception:
        logging.exception("Unexpected error. Aborting.")
        return 1

def main() -> int:
    setup_logging()
    return run_once()

if __name__ == "__main__":
    sys.exit(main())
