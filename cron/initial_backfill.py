#!/usr/bin/env python3
"""
Initial backfill script for US stock asset management system.
Run this ONCE when setting up a new server to populate historical data.

This script:
1. Creates all database tables
2. Syncs all trade history from start_date
3. Syncs current holdings and account summary
4. Backfills market index data (S&P 500, NASDAQ)
5. Constructs lots and updates metrics
6. Creates portfolio snapshots

Usage:
    python cron/initial_backfill.py
    python cron/initial_backfill.py --start-date 2026-01-02
"""

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from db.connection import get_connection
from scripts.init_database import init_database
from services.data_sync_service import (
    sync_trade_history_from_kis,
    sync_holdings_from_kis,
    sync_account_summary_from_kis,
)
from services.lot_service import rebuild_daily_lots, update_lot_metrics
from services.portfolio_service import create_portfolio_snapshot, create_daily_portfolio_snapshot
from services.market_index_service import sync_market_index


def backfill_daily_portfolio_snapshots(conn, start_date: date, end_date: date) -> int:
    """
    Backfill daily_portfolio_snapshot for date range.
    Uses holdings data to reconstruct historical snapshots.
    """
    count = 0
    current = start_date

    while current <= end_date:
        try:
            if create_daily_portfolio_snapshot(conn, current):
                count += 1
                print(f"    Created snapshot for {current}")
        except Exception as e:
            print(f"    Warning: Failed to create snapshot for {current}: {e}")
        current += timedelta(days=1)

    return count


def initial_backfill(start_date: date):
    """
    Run initial backfill for all historical data.

    Args:
        start_date: Start date for backfill
    """
    end_date = date.today()

    print("=" * 80)
    print("INITIAL BACKFILL (US Stocks)")
    print(f"Date range: {start_date} ~ {end_date}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Step 1: Initialize database tables
    print("\n[STEP 1] Initializing database tables...")
    init_database()

    conn = get_connection()

    try:
        # Step 2: Sync all trade history
        print("\n[STEP 2] Syncing trade history...")
        trade_count = sync_trade_history_from_kis(
            conn,
            start_date=start_date.strftime("%Y%m%d")
        )
        print(f"         Total trades: {trade_count}")

        # Step 3: Sync current holdings
        print("\n[STEP 3] Syncing current holdings...")
        holdings_count = sync_holdings_from_kis(conn)
        print(f"         Holdings: {holdings_count}")

        # Step 4: Sync account summary
        print("\n[STEP 4] Syncing account summary...")
        summary_count = sync_account_summary_from_kis(conn)
        print(f"         Summary: {summary_count}")

        # Step 5: Rebuild daily lots from trade history
        print("\n[STEP 5] Rebuilding daily lots...")
        rebuild_daily_lots(conn)
        print("         Lots rebuilt")

        # Step 6: Update lot metrics
        print("\n[STEP 6] Updating lot metrics...")
        lot_count = update_lot_metrics(conn, end_date)
        print(f"         Lots updated: {lot_count}")

        # Step 7: Create portfolio snapshot (today)
        print("\n[STEP 7] Creating portfolio snapshot (today)...")
        portfolio_count = create_portfolio_snapshot(conn, end_date)
        print(f"         Portfolio positions: {portfolio_count}")

        # Step 8: Create daily portfolio snapshot (today)
        print("\n[STEP 8] Creating daily portfolio summary...")
        create_daily_portfolio_snapshot(conn, end_date)
        print("         Summary created")

        # Step 9: Sync market index (S&P 500, NASDAQ)
        print("\n[STEP 9] Syncing market index (S&P 500/NASDAQ)...")
        try:
            index_count = sync_market_index(conn, start_date=start_date, end_date=end_date)
            print(f"         Index records: {index_count}")
        except Exception as e:
            print(f"         Warning: Market index sync failed: {e}")
            print("         You may need to install yfinance: pip install yfinance")
            index_count = 0

        # Get actual counts from DB
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM account_trade_history")
            trade_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM holdings WHERE snapshot_date = %s", (end_date,))
            holdings_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM daily_lots")
            lot_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM portfolio_snapshot")
            portfolio_count = cur.fetchone()[0]

            # Check if new tables exist
            try:
                cur.execute("SELECT COUNT(*) FROM daily_portfolio_snapshot")
                daily_snapshot_count = cur.fetchone()[0]
            except:
                daily_snapshot_count = 0

            try:
                cur.execute("SELECT COUNT(*) FROM market_index")
                index_count = cur.fetchone()[0]
            except:
                index_count = 0

        print("\n" + "=" * 80)
        print("INITIAL BACKFILL COMPLETE!")
        print(f"Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        # Summary (actual DB counts)
        print("\nSummary (DB counts):")
        print(f"  - Trade history: {trade_count} records")
        print(f"  - Holdings (today): {holdings_count} records")
        print(f"  - Lots: {lot_count} records")
        print(f"  - Portfolio positions: {portfolio_count} records")
        print(f"  - Daily snapshots: {daily_snapshot_count} records")
        print(f"  - Market index: {index_count} records")

    except Exception as e:
        print(f"\n[ERROR] Initial backfill failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Initial backfill for US stock asset management")
    parser.add_argument(
        "--start-date",
        type=str,
        default="2026-01-02",
        help="Start date for backfill (YYYY-MM-DD). Default: 2026-01-02",
    )
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    initial_backfill(start_date)


if __name__ == "__main__":
    main()
