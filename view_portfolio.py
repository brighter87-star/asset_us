"""
Portfolio Viewer CLI for US Stocks
View portfolio snapshots and position details.
"""

import argparse
import sys
from datetime import date, datetime
from typing import Optional

from db.connection import get_connection
from services.lot_service import get_open_lots, construct_daily_lots, update_lot_metrics


def format_number(value, decimals=0):
    """Format number with thousand separators."""
    if value is None:
        return "N/A"
    if decimals == 0:
        return f"{int(value):,}"
    return f"{float(value):,.{decimals}f}"


def format_currency(value, decimals=2):
    """Format currency in USD."""
    if value is None:
        return "N/A"
    return f"${float(value):,.{decimals}f}"


def format_percentage(value, decimals=2):
    """Format percentage."""
    if value is None:
        return "N/A"
    sign = "+" if value > 0 else ""
    return f"{sign}{float(value):.{decimals}f}%"


def view_portfolio():
    """
    View current portfolio from daily_lots.
    """
    conn = get_connection()

    try:
        # Get open lots
        lots = get_open_lots(conn)

        if not lots:
            print("No open positions found.")
            print("\nTip: Run 'python main.py' to sync data and construct lots.")
            return

        # Header
        print("=" * 110)
        print(f"Portfolio - {date.today()} (US Stocks)")
        print("=" * 110)

        # Table header
        print(f"\n{'Stock':<25} {'Qty':>6} {'Avg Cost':>12} {'Current':>12} {'Value':>14} {'P&L':>12} {'Return':>10}")
        print("-" * 110)

        total_cost = 0
        total_market_value = 0
        total_pnl = 0

        for lot in lots:
            stock_name = lot['stock_name'] or 'Unknown'
            stock_code = lot['stock_code']
            crd_class = lot['crd_class']

            # Truncate long stock names
            display_name = f"{stock_name[:18]}..." if len(stock_name) > 18 else stock_name
            if crd_class == 'CREDIT':
                display_name = f"{display_name}*"

            qty = lot['net_quantity']
            avg_cost = lot['avg_purchase_price'] or 0
            current = lot['current_price'] or 0
            cost = lot['total_cost'] or 0
            pnl = lot['unrealized_pnl'] or 0
            return_pct = lot['unrealized_return_pct'] or 0

            market_value = current * qty if current else 0

            total_cost += cost
            total_market_value += market_value
            total_pnl += pnl

            print(f"{display_name:<25} {format_number(qty):>6} "
                  f"{format_currency(avg_cost):>12} {format_currency(current):>12} "
                  f"{format_currency(market_value):>14} {format_currency(pnl):>12} "
                  f"{format_percentage(return_pct):>10}")

        # Summary
        print("-" * 110)
        total_return_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        print(f"{'TOTAL':<25} {'':<6} {'':<12} {'':<12} "
              f"{format_currency(total_market_value):>14} {format_currency(total_pnl):>12} "
              f"{format_percentage(total_return_pct):>10}")

        print("\n" + "=" * 110)
        print(f"Total Stock Value:  {format_currency(total_market_value)}")
        print(f"Total Invested:     {format_currency(total_cost)}")
        print(f"Total P&L:          {format_currency(total_pnl)} ({format_percentage(total_return_pct)})")
        print("=" * 110)
        print("\n* Credit position")

    finally:
        conn.close()


def view_position_detail(stock_code: str):
    """
    View detailed lot breakdown for a specific stock.

    Args:
        stock_code: Stock code to view
    """
    conn = get_connection()

    try:
        # Get all open lots for this stock
        lots = get_open_lots(conn, stock_code)

        if not lots:
            print(f"No open positions found for {stock_code}")
            return

        # Extract common info
        stock_name = lots[0]['stock_name']
        currency = lots[0].get('currency', 'USD')
        exchange = lots[0].get('exchange_code', 'NASD')

        # Header
        print("=" * 100)
        print(f"{stock_name} ({stock_code}) - {exchange} - {len(lots)} lot(s)")
        print("=" * 100)

        total_qty = 0
        total_cost = 0
        total_pnl = 0

        for i, lot in enumerate(lots, 1):
            crd_class = lot['crd_class']
            trade_date = lot['trade_date']
            net_quantity = lot['net_quantity']
            avg_price = lot['avg_purchase_price'] or 0
            cost = lot['total_cost'] or 0
            current_price = lot['current_price'] or 0
            pnl = lot['unrealized_pnl'] or 0
            return_pct = lot['unrealized_return_pct'] or 0
            holding_days = lot.get('holding_days', 0)

            total_qty += net_quantity or 0
            total_cost += cost or 0
            total_pnl += pnl or 0

            credit_mark = " [CREDIT]" if crd_class == 'CREDIT' else ""

            print(f"\nLot #{i} - {trade_date}{credit_mark}")
            print("-" * 100)
            print(f"  Quantity:       {format_number(net_quantity):>10} shares")
            print(f"  Purchase Price: {format_currency(avg_price):>15}")
            print(f"  Current Price:  {format_currency(current_price):>15}")
            print(f"  Total Cost:     {format_currency(cost):>15}")
            print(f"  Market Value:   {format_currency(current_price * net_quantity if current_price else 0):>15}")
            print(f"  P&L:            {format_currency(pnl):>15} ({format_percentage(return_pct)})")
            print(f"  Holding Days:   {format_number(holding_days):>10} days")

        # Summary
        print("\n" + "=" * 100)
        print("SUMMARY")
        print("=" * 100)
        print(f"Total Lots:     {len(lots)}")
        print(f"Total Quantity: {format_number(total_qty)} shares")
        print(f"Total Cost:     {format_currency(total_cost)}")

        avg_price_overall = total_cost / total_qty if total_qty > 0 else 0
        print(f"Average Price:  {format_currency(avg_price_overall)}")

        current_price = lots[0]['current_price'] if lots else 0
        market_value = current_price * total_qty if current_price else 0
        print(f"Current Price:  {format_currency(current_price)}")
        print(f"Market Value:   {format_currency(market_value)}")

        total_return_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
        print(f"Total P&L:      {format_currency(total_pnl)} ({format_percentage(total_return_pct)})")
        print("=" * 100)

    finally:
        conn.close()


def rebuild_lots():
    """Rebuild daily_lots from trade history."""
    print("Rebuilding daily lots from trade history...")

    conn = get_connection()
    try:
        # Clear existing lots
        with conn.cursor() as cur:
            cur.execute("DELETE FROM daily_lots")
        conn.commit()

        # Reconstruct lots
        construct_daily_lots(conn)
        print("  Lots constructed from trade history")

        # Update metrics
        updated = update_lot_metrics(conn)
        print(f"  Updated {updated} lots with current prices")

        conn.commit()
        print("Done!")
    finally:
        conn.close()


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="View portfolio snapshots and position details (US Stocks)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python view_portfolio.py                # View current portfolio
  python view_portfolio.py --stock AAPL   # View AAPL lots detail
  python view_portfolio.py --rebuild      # Rebuild lots from trade history
        """
    )

    parser.add_argument(
        '--stock',
        type=str,
        help='Stock code for detailed lot view (e.g., AAPL)'
    )

    parser.add_argument(
        '--rebuild',
        action='store_true',
        help='Rebuild daily_lots from trade history'
    )

    args = parser.parse_args()

    if args.rebuild:
        rebuild_lots()
    elif args.stock:
        view_position_detail(args.stock)
    else:
        view_portfolio()


if __name__ == "__main__":
    main()
