"""
Watchlist Manager - Add/remove items from watchlist with auto-dating.
"""

import argparse
import pandas as pd
from datetime import date
from pathlib import Path

WATCHLIST_PATH = Path(__file__).parent / "watchlist.csv"


def load_watchlist() -> pd.DataFrame:
    """Load watchlist from CSV."""
    if WATCHLIST_PATH.exists():
        return pd.read_csv(WATCHLIST_PATH)
    return pd.DataFrame(columns=["ticker", "target_price", "stop_loss_pct", "max_units", "added_date"])


def save_watchlist(df: pd.DataFrame):
    """Save watchlist to CSV."""
    df.to_csv(WATCHLIST_PATH, index=False)


def add_item(ticker: str, target_price: float, max_units: int = 1, stop_loss_pct: float = None):
    """Add item to watchlist with auto-dated added_date."""
    df = load_watchlist()
    ticker = ticker.upper()

    # Check if already exists
    if ticker in df["ticker"].values:
        print(f"[WARN] {ticker} already in watchlist. Use 'update' to modify.")
        return

    new_row = {
        "ticker": ticker,
        "target_price": target_price,
        "stop_loss_pct": stop_loss_pct if stop_loss_pct else "",
        "max_units": max_units,
        "added_date": str(date.today()),
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_watchlist(df)
    print(f"[OK] Added {ticker} @ ${target_price:.2f} (max_units={max_units}, added={date.today()})")


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


def update_item(ticker: str, target_price: float = None, max_units: int = None, stop_loss_pct: float = None):
    """Update existing item in watchlist."""
    df = load_watchlist()
    ticker = ticker.upper()

    if ticker not in df["ticker"].values:
        print(f"[WARN] {ticker} not in watchlist. Use 'add' to create.")
        return

    idx = df[df["ticker"] == ticker].index[0]

    if target_price is not None:
        df.loc[idx, "target_price"] = target_price
    if max_units is not None:
        df.loc[idx, "max_units"] = max_units
    if stop_loss_pct is not None:
        df.loc[idx, "stop_loss_pct"] = stop_loss_pct

    save_watchlist(df)
    print(f"[OK] Updated {ticker}")


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


def main():
    parser = argparse.ArgumentParser(description="Manage watchlist items")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # add command
    add_parser = subparsers.add_parser("add", help="Add item to watchlist")
    add_parser.add_argument("ticker", type=str, help="Stock ticker (e.g., AAPL)")
    add_parser.add_argument("target_price", type=float, help="Target price for breakout")
    add_parser.add_argument("--max-units", type=int, default=1, help="Max units to buy (default: 1)")
    add_parser.add_argument("--stop-loss", type=float, help="Custom stop loss %")

    # remove command
    remove_parser = subparsers.add_parser("remove", help="Remove item from watchlist")
    remove_parser.add_argument("ticker", type=str, help="Stock ticker")

    # update command
    update_parser = subparsers.add_parser("update", help="Update item in watchlist")
    update_parser.add_argument("ticker", type=str, help="Stock ticker")
    update_parser.add_argument("--target", type=float, help="New target price")
    update_parser.add_argument("--max-units", type=int, help="New max units")
    update_parser.add_argument("--stop-loss", type=float, help="New stop loss %")

    # list command
    subparsers.add_parser("list", help="List all items in watchlist")

    args = parser.parse_args()

    if args.command == "add":
        add_item(args.ticker, args.target_price, args.max_units, args.stop_loss)
    elif args.command == "remove":
        remove_item(args.ticker)
    elif args.command == "update":
        update_item(args.ticker, args.target, args.max_units, args.stop_loss)
    elif args.command == "list":
        list_items()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
