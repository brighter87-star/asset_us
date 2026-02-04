#!/usr/bin/env python3
"""
Daily synchronization script for US stock asset management system.
Syncs all data from Korea Investment Securities API to database.

This script is idempotent - safe to run multiple times.
Designed to be run by cron after US market close (4:00 PM ET = ~6:00 AM KST).

Usage:
    python cron/daily_sync.py              # Sync today's data
    python cron/daily_sync.py --date 2026-02-04  # Sync specific date
"""

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import get_connection
from services.kis_service import KISAPIClient
from services.data_sync_service import (
    sync_trade_history_from_kis,
    sync_holdings_from_kis,
)
from services.lot_service import construct_daily_lots, update_lot_metrics
from services.portfolio_service import create_portfolio_snapshot, create_daily_portfolio_snapshot
from services.market_index_service import sync_market_index


def daily_sync(target_date: date = None):
    """
    Run daily synchronization for all data.

    Args:
        target_date: Date to sync. If None, uses today.
    """
    if target_date is None:
        target_date = date.today()

    print("=" * 80)
    print(f"Daily Sync (US Stocks) - {target_date}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    conn = get_connection()

    try:
        # 1. Sync trade history (idempotent - INSERT IGNORE)
        print("\n[1/7] Syncing trade history...")
        trade_count = sync_trade_history_from_kis(
            conn,
            start_date=target_date.strftime("%Y%m%d")
        )
        print(f"      Trade records: {trade_count}")

        # 2. Sync holdings
        print("\n[2/7] Syncing holdings...")
        holdings_count = sync_holdings_from_kis(conn, snapshot_date=target_date)
        print(f"      Holdings records: {holdings_count}")

        # 3. Construct/update daily lots
        print("\n[3/7] Constructing daily lots...")
        construct_daily_lots(conn)
        print(f"      Lots constructed")

        # 4. Update lot metrics
        print("\n[4/7] Updating lot metrics...")
        lot_count = update_lot_metrics(conn, target_date)
        print(f"      Lots updated: {lot_count}")

        # 5. Create portfolio snapshot (per-position)
        print("\n[5/7] Creating portfolio snapshot...")
        portfolio_count = create_portfolio_snapshot(conn, target_date)
        print(f"      Portfolio positions: {portfolio_count}")

        # 6. Create daily portfolio snapshot (summary for TWR/MWR)
        print("\n[6/7] Creating daily portfolio summary...")
        daily_snapshot_ok = create_daily_portfolio_snapshot(conn, target_date)
        print(f"      Summary snapshot: {'created' if daily_snapshot_ok else 'skipped'}")

        # 7. Sync market index (S&P 500, NASDAQ)
        print("\n[7/7] Syncing market index...")
        try:
            index_count = sync_market_index(conn, start_date=target_date, end_date=target_date)
            print(f"      Index records: {index_count}")
        except Exception as e:
            print(f"      Warning: Market index sync failed: {e}")

        print("\n" + "=" * 80)
        print(f"Daily Sync Complete!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

    except Exception as e:
        print(f"\n[ERROR] Daily sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Daily sync for US stock asset management")
    parser.add_argument(
        "--date",
        type=str,
        help="Target date (YYYY-MM-DD). Default: today",
    )
    args = parser.parse_args()

    target_date = None
    if args.date:
        target_date = datetime.strptime(args.date, "%Y-%m-%d").date()

    daily_sync(target_date)


if __name__ == "__main__":
    main()
