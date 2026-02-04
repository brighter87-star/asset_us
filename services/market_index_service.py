"""
Market index service for fetching S&P 500 and NASDAQ data.
Uses yfinance for free market data.
"""

from datetime import date, datetime, timedelta
from typing import Optional

import pymysql


def sync_market_index(
    conn: pymysql.connections.Connection,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> int:
    """
    Sync S&P 500 and NASDAQ index data to database.

    Args:
        conn: Database connection
        start_date: Start date for sync (default: 30 days ago)
        end_date: End date for sync (default: today)

    Returns:
        Number of records synced
    """
    try:
        import yfinance as yf
    except ImportError:
        print("yfinance not installed. Run: pip install yfinance")
        return 0

    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    # Fetch S&P 500 (^GSPC) and NASDAQ (^IXIC)
    sp500 = yf.Ticker("^GSPC")
    nasdaq = yf.Ticker("^IXIC")

    # Get historical data
    sp500_hist = sp500.history(start=start_date, end=end_date + timedelta(days=1))
    nasdaq_hist = nasdaq.history(start=start_date, end=end_date + timedelta(days=1))

    if sp500_hist.empty and nasdaq_hist.empty:
        print(f"No market data found for {start_date} to {end_date}")
        return 0

    # Merge data by date
    all_dates = set()
    sp500_dict = {}
    nasdaq_dict = {}

    for idx, row in sp500_hist.iterrows():
        d = idx.date()
        all_dates.add(d)
        prev_close = sp500_dict.get(d - timedelta(days=1), {}).get("close")
        sp500_dict[d] = {
            "close": row["Close"],
            "change": row["Close"] - prev_close if prev_close else 0,
            "change_pct": ((row["Close"] / prev_close) - 1) * 100 if prev_close else 0,
        }

    for idx, row in nasdaq_hist.iterrows():
        d = idx.date()
        all_dates.add(d)
        prev_close = nasdaq_dict.get(d - timedelta(days=1), {}).get("close")
        nasdaq_dict[d] = {
            "close": row["Close"],
            "change": row["Close"] - prev_close if prev_close else 0,
            "change_pct": ((row["Close"] / prev_close) - 1) * 100 if prev_close else 0,
        }

    # Insert or update records
    insert_sql = """
        INSERT INTO market_index (
            index_date,
            sp500_close, sp500_change, sp500_change_pct,
            nasdaq_close, nasdaq_change, nasdaq_change_pct
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            sp500_close = VALUES(sp500_close),
            sp500_change = VALUES(sp500_change),
            sp500_change_pct = VALUES(sp500_change_pct),
            nasdaq_close = VALUES(nasdaq_close),
            nasdaq_change = VALUES(nasdaq_change),
            nasdaq_change_pct = VALUES(nasdaq_change_pct),
            updated_at = CURRENT_TIMESTAMP
    """

    count = 0
    with conn.cursor() as cur:
        for d in sorted(all_dates):
            sp500_data = sp500_dict.get(d, {})
            nasdaq_data = nasdaq_dict.get(d, {})

            cur.execute(
                insert_sql,
                (
                    d,
                    sp500_data.get("close"),
                    sp500_data.get("change"),
                    sp500_data.get("change_pct"),
                    nasdaq_data.get("close"),
                    nasdaq_data.get("change"),
                    nasdaq_data.get("change_pct"),
                ),
            )
            count += 1

    conn.commit()
    print(f"Synced {count} market index records from {start_date} to {end_date}")
    return count


def get_market_index(
    conn: pymysql.connections.Connection,
    index_date: date,
) -> dict:
    """Get market index data for a specific date."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM market_index WHERE index_date = %s
            """,
            (index_date,),
        )
        return cur.fetchone() or {}


def get_market_index_range(
    conn: pymysql.connections.Connection,
    start_date: date,
    end_date: date,
) -> list:
    """Get market index data for a date range."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT * FROM market_index
            WHERE index_date BETWEEN %s AND %s
            ORDER BY index_date
            """,
            (start_date, end_date),
        )
        return cur.fetchall()
