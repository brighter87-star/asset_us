#!/bin/bash
# Daily sync wrapper script for US stocks
# Usage: ./cron/run_daily_sync.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Activate virtual environment
source venv/bin/activate

# Run daily sync
python cron/daily_sync.py "$@"

# Deactivate
deactivate
