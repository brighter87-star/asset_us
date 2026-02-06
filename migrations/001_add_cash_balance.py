#!/usr/bin/env python
"""
Migration 001: Add cash_balance column to account_summary table.

총자산 = 현금(cash_balance) + 주식평가액(aset_evlt_amt)

Usage:
    python migrations/001_add_cash_balance.py
"""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from db.connection import get_connection


def migrate():
    """Add cash_balance column to account_summary if not exists."""
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            # Check if column exists
            cur.execute("""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = DATABASE()
                AND table_name = 'account_summary'
                AND column_name = 'cash_balance'
            """)
            exists = cur.fetchone()[0] > 0

            if exists:
                print("[OK] cash_balance column already exists")
                return

            # Add column
            cur.execute("""
                ALTER TABLE account_summary
                ADD COLUMN cash_balance DECIMAL(20,4) DEFAULT 0 AFTER aset_evlt_amt
            """)
            conn.commit()
            print("[OK] Added cash_balance column to account_summary")

            # Update existing rows: set tot_est_amt = aset_evlt_amt (no cash data)
            # Note: Historical data won't have cash, only current/future syncs will
            print("    Note: Historical records have cash_balance=0 (no historical cash data)")

    except Exception as e:
        print(f"[ERROR] Migration failed: {e}")
        raise
    finally:
        conn.close()


def rollback():
    """Remove cash_balance column."""
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE account_summary DROP COLUMN cash_balance")
            conn.commit()
            print("[OK] Removed cash_balance column")
    except Exception as e:
        print(f"[ERROR] Rollback failed: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "rollback":
        rollback()
    else:
        migrate()
