"""
Watchlist Manager - Add/remove items from watchlist with auto-dating.

Commands:
    add      - Add item to watchlist
    remove   - Remove item from watchlist
    update   - Update existing item
    list     - List all items
    cleanup  - Remove stocks that have been stopped out (sold after added_date)
"""

import argparse
import pandas as pd
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

WATCHLIST_PATH = Path(__file__).parent / "watchlist.csv"

# US Eastern timezone for consistent date handling with trade records
ET = ZoneInfo("America/New_York")


def get_today_et() -> date:
    """
    Get the date for the upcoming/current US market session.

    Logic:
    - Before 8 PM ET: current US date (market is open or will open today)
    - After 8 PM ET: next US date (today's session ended, next is tomorrow)
    """
    now_et = datetime.now(ET)
    if now_et.hour >= 20:  # After 8 PM ET, use tomorrow
        return (now_et + timedelta(days=1)).date()
    return now_et.date()


def parse_date(date_str: str) -> date:
    """
    Parse date string in various formats.

    Supported formats:
    - YYYY-MM-DD, YYYY/MM/DD (full date)
    - MM-DD, MM/DD, M/D, M-D (without year, assumes current year)
    - MM.DD, M.D (dot separator)

    Examples: 2026-02-05, 2/5, 02/05, 2.5, 02-05
    """
    date_str = date_str.strip()
    current_year = datetime.now().year

    # Try various formats
    formats_with_year = [
        "%Y-%m-%d",   # 2026-02-05
        "%Y/%m/%d",   # 2026/02/05
        "%Y.%m.%d",   # 2026.02.05
    ]

    formats_without_year = [
        "%m-%d",      # 02-05
        "%m/%d",      # 02/05
        "%m.%d",      # 02.05
    ]

    # Try formats with year first
    for fmt in formats_with_year:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    # Try formats without year
    for fmt in formats_without_year:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed.replace(year=current_year).date()
        except ValueError:
            continue

    raise argparse.ArgumentTypeError(
        f"Invalid date format: {date_str}. "
        f"Use YYYY-MM-DD, MM/DD, M/D, MM.DD, etc."
    )


def load_watchlist() -> pd.DataFrame:
    """Load watchlist from CSV."""
    if WATCHLIST_PATH.exists():
        return pd.read_csv(WATCHLIST_PATH)
    return pd.DataFrame(columns=["ticker", "target_price", "stop_loss_pct", "max_units", "added_date"])


def save_watchlist(df: pd.DataFrame):
    """Save watchlist to CSV."""
    df.to_csv(WATCHLIST_PATH, index=False)


def add_item(ticker: str, target_price: float, max_units: int = 1, stop_loss_pct: float = None, added_date: date = None):
    """Add item to watchlist with specified or auto-dated added_date."""
    df = load_watchlist()
    ticker = ticker.upper()

    # Check if already exists
    if ticker in df["ticker"].values:
        print(f"[WARN] {ticker} already in watchlist. Use 'update' to modify.")
        return

    # Use provided date or today (US Eastern time)
    add_date = added_date if added_date else get_today_et()

    new_row = {
        "ticker": ticker,
        "target_price": target_price,
        "stop_loss_pct": stop_loss_pct if stop_loss_pct else "",
        "max_units": max_units,
        "added_date": str(add_date),
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_watchlist(df)
    print(f"[OK] Added {ticker} @ ${target_price:.2f} (max_units={max_units}, added={add_date})")


def remove_item(ticker: str):
    """Remove item from watchlist."""
    df = load_watchlist()
    ticker = ticker.upper()

    if ticker not in df["ticker"].values:
        print(f"[WARN] {ticker} not in watchlist.")
        return

    df = df[df["ticker"] != ticker]
    save_watchlist(df)
    print(f"[OK] Removed {ticker} from watchlist")


def update_item(ticker: str, target_price: float = None, max_units: int = None, stop_loss_pct: float = None, reset_date: bool = True, specific_date: date = None):
    """Update existing item in watchlist. Resets added_date to today (US ET) when target_price is updated."""
    df = load_watchlist()
    ticker = ticker.upper()

    if ticker not in df["ticker"].values:
        print(f"[WARN] {ticker} not in watchlist. Use 'add' to create.")
        return

    idx = df[df["ticker"] == ticker].index[0]

    # Determine date to use
    if specific_date:
        new_date = specific_date
    elif reset_date:
        new_date = get_today_et()
    else:
        new_date = None

    if target_price is not None:
        df.loc[idx, "target_price"] = target_price
        if new_date:
            df.loc[idx, "added_date"] = str(new_date)
    if max_units is not None:
        df.loc[idx, "max_units"] = max_units
    if stop_loss_pct is not None:
        df.loc[idx, "stop_loss_pct"] = stop_loss_pct

    save_watchlist(df)
    date_str = str(new_date) if new_date else "(unchanged)"
    print(f"[OK] Updated {ticker} @ ${target_price:.2f} (date={date_str})")


def list_items():
    """List all items in watchlist."""
    df = load_watchlist()

    if df.empty:
        print("Watchlist is empty.")
        return

    print(f"\n{'Ticker':<8} {'Target($)':>12} {'Max':>5} {'SL%':>6} {'Added':>12}")
    print("-" * 50)

    for _, row in df.iterrows():
        ticker = row["ticker"]
        target = row["target_price"]
        max_units = int(row.get("max_units", 1)) if pd.notna(row.get("max_units")) else 1
        sl = row.get("stop_loss_pct", "")
        sl_str = f"{sl:.1f}" if pd.notna(sl) and sl != "" else "-"
        added = row.get("added_date", "")
        added_str = str(added) if pd.notna(added) and added != "" else "-"

        print(f"{ticker:<8} {target:>12,.2f} {max_units:>5} {sl_str:>6} {added_str:>12}")

    print("-" * 50)
    print(f"Total: {len(df)} items")


def get_sells_from_db(ticker: str, since_date: date) -> list:
    """
    Get sell records from DB for a ticker since a specific date.
    Returns list of (trade_date, quantity, price) tuples.
    """
    try:
        from db.connection import get_connection
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT trade_date, cntr_qty, cntr_uv
                FROM account_trade_history
                WHERE stk_cd = %s
                  AND trade_date >= %s
                  AND io_tp_nm LIKE '%%매도%%'
                ORDER BY trade_date
            """, (ticker, since_date))
            rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"[ERROR] Failed to query DB: {e}")
        return []


def cleanup_stopped_out(dry_run: bool = True):
    """
    Find and remove stocks that have been stopped out.
    A stock is stopped out if there's a sell record on or after its added_date.

    Args:
        dry_run: If True, only show what would be removed without removing.
    """
    df = load_watchlist()

    if df.empty:
        print("Watchlist is empty.")
        return

    stopped_out = []
    kept = []

    print("\n[Scanning for stopped-out stocks...]")
    print("-" * 70)

    for _, row in df.iterrows():
        ticker = row["ticker"]
        added_date_str = row.get("added_date", "")

        # Skip if no added_date
        if pd.isna(added_date_str) or added_date_str == "":
            kept.append(ticker)
            continue

        try:
            added_dt = datetime.strptime(str(added_date_str), "%Y-%m-%d").date()
        except ValueError:
            print(f"[WARN] {ticker}: Invalid added_date '{added_date_str}', skipping")
            kept.append(ticker)
            continue

        # Check for sells since added_date
        sells = get_sells_from_db(ticker, added_dt)

        if sells:
            # Found sell records - this is a stopped-out stock
            total_qty = sum(s[1] or 0 for s in sells)
            avg_price = sum((s[1] or 0) * float(s[2] or 0) for s in sells) / total_qty if total_qty > 0 else 0
            sell_dates = [str(s[0]) for s in sells]

            stopped_out.append({
                "ticker": ticker,
                "added_date": added_date_str,
                "sell_dates": sell_dates,
                "total_qty": total_qty,
                "avg_sell_price": avg_price,
            })
            print(f"  [STOP] {ticker}: Added {added_date_str}, Sold on {', '.join(sell_dates)} ({total_qty} shares @ ${avg_price:.2f})")
        else:
            kept.append(ticker)

    print("-" * 70)

    if not stopped_out:
        print("\nNo stopped-out stocks found.")
        return

    print(f"\nFound {len(stopped_out)} stopped-out stock(s):")
    for item in stopped_out:
        print(f"  - {item['ticker']}")

    if dry_run:
        print(f"\n[DRY RUN] Would remove {len(stopped_out)} stocks from watchlist.")
        print("Run with --execute to actually remove them.")
    else:
        # Actually remove them
        df_clean = df[df["ticker"].isin(kept)]
        save_watchlist(df_clean)
        print(f"\n[OK] Removed {len(stopped_out)} stopped-out stocks from watchlist.")
        print(f"     Remaining: {len(df_clean)} stocks")


def main():
    parser = argparse.ArgumentParser(description="Manage watchlist items")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # add command
    add_parser = subparsers.add_parser("add", help="Add item to watchlist")
    add_parser.add_argument("ticker", type=str, help="Stock ticker (e.g., AAPL)")
    add_parser.add_argument("target_price", type=float, help="Target price for breakout")
    add_parser.add_argument("--max-units", type=int, default=1, help="Max units to buy (default: 1)")
    add_parser.add_argument("--stop-loss", type=float, help="Custom stop loss %")
    add_parser.add_argument("--date", type=parse_date, help="Added date (YYYY-MM-DD format, default: today)")

    # remove command
    remove_parser = subparsers.add_parser("remove", help="Remove item from watchlist")
    remove_parser.add_argument("ticker", type=str, help="Stock ticker")

    # update command
    update_parser = subparsers.add_parser("update", help="Update item in watchlist (resets added_date to today US ET)")
    update_parser.add_argument("ticker", type=str, help="Stock ticker")
    update_parser.add_argument("target_price", type=float, help="New target price")
    update_parser.add_argument("--max-units", type=int, help="New max units")
    update_parser.add_argument("--stop-loss", type=float, help="New stop loss %")
    update_parser.add_argument("--date", type=parse_date, help="Set specific date (YYYY-MM-DD)")
    update_parser.add_argument("--no-date-reset", action="store_true", help="Don't reset added_date")

    # list command
    subparsers.add_parser("list", help="List all items in watchlist")

    # cleanup command
    cleanup_parser = subparsers.add_parser("cleanup", help="Remove stopped-out stocks (sold after added_date)")
    cleanup_parser.add_argument("--execute", action="store_true", help="Actually remove (default: dry-run)")

    args = parser.parse_args()

    if args.command == "add":
        add_item(args.ticker, args.target_price, args.max_units, args.stop_loss, args.date)
    elif args.command == "remove":
        remove_item(args.ticker)
    elif args.command == "update":
        update_item(args.ticker, args.target_price, args.max_units, args.stop_loss,
                    reset_date=not args.no_date_reset, specific_date=args.date)
    elif args.command == "list":
        list_items()
    elif args.command == "cleanup":
        cleanup_stopped_out(dry_run=not args.execute)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
