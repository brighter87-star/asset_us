#!/bin/bash
# Setup cron job for daily_sync.py
# Run this script once to add the cron job

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Cron entry: 20:00 ET Mon-Fri (after after-market close)
CRON_ENTRY="0 20 * * * TZ=America/New_York cd $PROJECT_DIR && python cron/daily_sync.py >> $PROJECT_DIR/logs/daily_sync.log 2>&1"

# Create logs directory if not exists
mkdir -p "$PROJECT_DIR/logs"

# Check if cron entry already exists for THIS project (asset_us)
if crontab -l 2>/dev/null | grep -q "asset_us.*daily_sync.py"; then
    echo "Cron job for asset_us already exists. Current entry:"
    crontab -l | grep "asset_us.*daily_sync"
    echo ""
    read -p "Replace existing entry? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Remove only asset_us entry and add new one
        (crontab -l 2>/dev/null | grep -v "asset_us.*daily_sync.py"; echo "$CRON_ENTRY") | crontab -
        echo "Cron job updated."
    else
        echo "Cancelled."
        exit 0
    fi
else
    # Add new cron entry (preserves existing entries from other projects)
    (crontab -l 2>/dev/null; echo "$CRON_ENTRY") | crontab -
    echo "Cron job added."
fi

# Cron entry: 20:10 ET (notebook, runs after daily_sync)
NOTEBOOK_CRON="10 20 * * * TZ=America/New_York cd $PROJECT_DIR && python cron/run_notebook.py >> $PROJECT_DIR/logs/notebook.log 2>&1"

if crontab -l 2>/dev/null | grep -q "asset_us.*run_notebook.py"; then
    (crontab -l 2>/dev/null | grep -v "asset_us.*run_notebook.py"; echo "$NOTEBOOK_CRON") | crontab -
    echo "Notebook cron job updated."
else
    (crontab -l 2>/dev/null; echo "$NOTEBOOK_CRON") | crontab -
    echo "Notebook cron job added."
fi

echo ""
echo "Current crontab:"
crontab -l | grep -E "daily_sync|run_notebook" || echo "(none)"
echo ""
echo "Schedule:"
echo "  20:00 ET - daily_sync.py  (DB 동기화)"
echo "  20:10 ET - run_notebook.py (차트 이미지 저장, 거래일만)"
echo "Logs: $PROJECT_DIR/logs/daily_sync.log"
echo "      $PROJECT_DIR/logs/notebook.log"
