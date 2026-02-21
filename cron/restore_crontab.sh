#!/bin/bash
# Restore crontab for both asset and asset_us projects
# Run on server: bash restore_crontab.sh

# Detect project paths (adjust if needed)
ASSET_DIR="${ASSET_DIR:-/home/ubuntu/asset}"
ASSET_US_DIR="${ASSET_US_DIR:-/home/ubuntu/asset_us}"

# Create logs directories
mkdir -p "$ASSET_DIR/logs"
mkdir -p "$ASSET_US_DIR/logs"

# Set up crontab with both entries
cat << EOF | crontab -
# asset (Korean stocks) - 15:35 KST Mon-Fri (after market close)
35 6 * * 1-5 cd $ASSET_DIR && python cron/daily_sync.py >> $ASSET_DIR/logs/daily_sync.log 2>&1

# asset_us (US stocks) - 20:00 ET daily (script auto-skips weekends)
0 20 * * * TZ=America/New_York cd $ASSET_US_DIR && python cron/daily_sync.py >> $ASSET_US_DIR/logs/daily_sync.log 2>&1
EOF

echo "Crontab restored:"
crontab -l
