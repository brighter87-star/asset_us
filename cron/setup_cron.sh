#!/bin/bash
# Setup cron job for daily_sync.py
# Run this script once to add the cron job

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Cron entry: 20:00 ET Mon-Fri (after after-market close)
CRON_ENTRY="0 20 * * 1-5 TZ=America/New_York cd $PROJECT_DIR && python cron/daily_sync.py >> $PROJECT_DIR/logs/daily_sync.log 2>&1"

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

echo ""
echo "Current crontab:"
crontab -l | grep "daily_sync" || echo "(none)"
echo ""
echo "Schedule: 20:00 ET (Mon-Fri)"
echo "Logs: $PROJECT_DIR/logs/daily_sync.log"
