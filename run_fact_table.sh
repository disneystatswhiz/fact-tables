#!/usr/bin/env bash
set -euo pipefail

# Always start in the repo root (where this file lives)
cd "$(dirname "$0")"

# Encoding: prevent cp1252 issues on Windows
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

# Auto-create/refresh venv if missing
if [ ! -d "venv" ]; then
  echo "[INFO] Creating venv..."
  python -m venv venv
fi

# Activate venv (Windows Git Bash path)
source venv/Scripts/activate

# Install deps if requirements.txt exists but packages may be missing
if [ -f requirements.txt ]; then
  echo "[INFO] Ensuring dependencies..."
  python -m pip install -r requirements.txt -q
fi

# Run the pipeline
echo "[INFO] Running pipeline..."
python src/main.py
exit_code=$?

# Surface the log file location (last file in logs/)
last_log="$(ls -1t logs/run_*.log 2>/dev/null | head -n 1 || true)"
[ -n "$last_log" ] && echo "[INFO] Log: $last_log"

exit $exit_code
