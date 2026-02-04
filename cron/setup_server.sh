#!/bin/bash
# Server setup script for US stock asset management system
# Run this ONCE on a new server
#
# Usage:
#   chmod +x cron/setup_server.sh
#   ./cron/setup_server.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "========================================"
echo "US Stock Asset Management Server Setup"
echo "========================================"

cd "$PROJECT_DIR"

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "[ERROR] .env file not found!"
    echo "Please create .env file with your credentials."
    echo ""
    echo "Required .env variables:"
    echo "  APP_KEY=<your_kis_app_key>"
    echo "  SECRET_KEY=<your_kis_secret_key>"
    echo "  CANO=<your_account_number>"
    echo "  ACNT_PRDT_CD=<your_product_code>"
    echo "  BASE_URL=https://openapi.koreainvestment.com:9443"
    echo "  DB_HOST=<your_db_host>"
    echo "  DB_NAME=asset_us"
    echo "  DB_USER=<your_db_user>"
    echo "  DB_PASSWORD=<your_db_password>"
    echo "  TELEGRAM_BOT_TOKEN=<optional>"
    echo "  TELEGRAM_CHAT_ID=<optional>"
    exit 1
fi

# Check if watchlist.csv exists
if [ ! -f "watchlist.csv" ]; then
    echo "[WARN] watchlist.csv not found, creating default..."
    echo "ticker,target_price,stop_loss_pct" > watchlist.csv
    echo "AAPL,200," >> watchlist.csv
fi

# Check if settings.csv exists
if [ ! -f "settings.csv" ]; then
    echo "[WARN] settings.csv not found, creating default..."
    echo "key,value" > settings.csv
    echo "UNIT,1" >> settings.csv
    echo "STOP_LOSS_PCT,7.0" >> settings.csv
    echo "PRICE_BUFFER_PCT,0.5" >> settings.csv
fi

# Create virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "[1/4] Creating virtual environment..."
    python3 -m venv venv
else
    echo "[1/4] Virtual environment already exists"
fi

# Activate and install dependencies
echo "[2/4] Installing dependencies..."
source venv/bin/activate
pip install -r requirements.txt

# Run initial sync to populate database
echo "[3/4] Running initial sync..."
python cron/daily_sync.py

# Setup cron for daily sync
# US market closes at 4:00 PM ET
# - EST (Nov-Mar): 4:00 PM ET = 6:00 AM KST next day
# - EDT (Mar-Nov): 4:00 PM ET = 5:00 AM KST next day
# We'll run at 6:05 AM KST to be safe
echo "[4/4] Setting up cron..."
SYNC_CRON_CMD="5 6 * * 2-6 cd $PROJECT_DIR && $PROJECT_DIR/venv/bin/python cron/daily_sync.py >> /var/log/asset_us_sync.log 2>&1"

# Check if cron job already exists
if crontab -l 2>/dev/null | grep -q "asset_us.*daily_sync.py"; then
    echo "Daily sync cron job already exists"
else
    # Add cron job
    (crontab -l 2>/dev/null; echo "$SYNC_CRON_CMD") | crontab -
    echo "Daily sync cron job added: runs at 6:05 AM KST on Tue-Sat"
fi

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Cron schedule:"
echo "  Daily sync: Tue-Sat at 6:05 AM KST (after US market close)"
echo ""
echo "Log files:"
echo "  Sync: /var/log/asset_us_sync.log"
echo ""
echo "Manual commands:"
echo "  python cron/daily_sync.py              # Run daily sync"
echo "  python cron/daily_sync.py --date 2026-02-04  # Sync specific date"
echo "  python auto_trade.py                   # Run live trading bot"
echo "  python view_portfolio.py               # View portfolio"
echo ""
echo "To run the trading bot (manually or via screen/tmux):"
echo "  cd $PROJECT_DIR"
echo "  source venv/bin/activate"
echo "  python auto_trade.py"
echo ""

deactivate
