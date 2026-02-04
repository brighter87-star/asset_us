"""
Automated Trading System for US Stocks (Korea Investment Securities)
Trend-following breakout strategy with pyramiding.

Usage:
    python auto_trade.py              # Run trading loop
    python auto_trade.py --status     # Show current status with live prices
    python auto_trade.py --test       # Test API connection
"""

import sys
import time
from datetime import datetime

from db.connection import get_connection
from services.kis_service import KISAPIClient
from services.data_sync_service import sync_holdings_from_kis
from services.monitor_service import MonitorService
from services.trade_logger import trade_logger
from services.price_service import RestPricePoller


def load_holdings_prices_from_db() -> dict:
    """Load current prices from holdings table for initial price cache."""
    from datetime import date
    prices = {}

    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT stk_cd, MAX(cur_prc) as cur_prc
                FROM holdings
                WHERE snapshot_date = %s AND cur_prc > 0
                GROUP BY stk_cd
            """, (date.today(),))

            rows = cur.fetchall()

        conn.close()

        for row in rows:
            stock_code, cur_prc = row
            if stock_code and cur_prc:
                prices[stock_code] = {"last": float(cur_prc)}

    except Exception as e:
        print(f"[WARN] Failed to load holdings prices: {e}")

    return prices


def get_market_session() -> str:
    """
    Determine current US market session.
    Returns: 'PRE', 'REGULAR', 'AFTER', or 'CLOSED'

    US Market Hours (ET):
    - Pre-market: 4:00 AM - 9:30 AM ET
    - Regular: 9:30 AM - 4:00 PM ET
    - After-market: 4:00 PM - 8:00 PM ET
    """
    from datetime import time
    from zoneinfo import ZoneInfo

    ET = ZoneInfo("America/New_York")
    now_et = datetime.now(ET)
    current_time = now_et.time()

    # Weekend = closed
    if now_et.weekday() >= 5:
        return "CLOSED"

    pre_start = time(4, 0)
    regular_start = time(9, 30)
    regular_end = time(16, 0)
    after_end = time(20, 0)

    if pre_start <= current_time < regular_start:
        return "PRE"
    elif regular_start <= current_time < regular_end:
        return "REGULAR"
    elif regular_end <= current_time < after_end:
        return "AFTER"
    else:
        return "CLOSED"


def print_banner():
    """Print startup banner."""
    print("=" * 70)
    print("  US Stock Auto Trading System (Korea Investment Securities)")
    print("=" * 70)


def print_settings(monitor: MonitorService = None):
    """Print current settings."""
    if monitor:
        s = monitor.trading_settings
        unit_pct = s.get_unit_percent()
    else:
        s = type('S', (), {'UNIT': 1.0, 'STOP_LOSS_PCT': 7.0, 'PRICE_BUFFER_PCT': 0.5})()
        unit_pct = 5.0

    print("\n[Settings]")
    print(f"  UNIT: {s.UNIT} ({unit_pct}% of assets)")
    print(f"  STOP_LOSS_PCT: {s.STOP_LOSS_PCT}%")
    print(f"  PRICE_BUFFER_PCT: {getattr(s, 'PRICE_BUFFER_PCT', 0.5)}%")


def test_connection():
    """Test API connection."""
    print("\n[Testing API Connection]")

    client = KISAPIClient()

    try:
        token = client.get_access_token()
        print(f"  Token: OK ({token[:20]}...)")
    except Exception as e:
        print(f"  Token: FAILED - {e}")
        return False

    try:
        power = client.get_buying_power("NASD")
        print(f"  Buying Power: ${power['available_amt']:,.2f} USD")
    except Exception as e:
        print(f"  Buying Power: FAILED - {e}")
        return False

    try:
        price = client.get_current_price("AAPL", "NAS")
        print(f"  Price API: AAPL @ ${price['last']:.2f}")
    except Exception as e:
        print(f"  Price API: FAILED - {e}")
        return False

    print("\n  All tests passed!")
    return True


def show_status():
    """Show current monitoring status."""
    monitor = MonitorService()
    monitor.load_watchlist()

    # DB에서 positions 동기화
    monitor.order_service.sync_positions_from_db(
        stop_loss_pct=monitor.trading_settings.STOP_LOSS_PCT
    )

    status = monitor.get_status()

    print("\n" + "=" * 70)
    print("  AUTO TRADING STATUS")
    print("=" * 70)

    print(f"\n[System]")
    print(f"  Time (ET): {status['current_time_et']}")
    print(f"  Time (KST): {status['current_time_kst']}")
    print(f"  Market Open: {'Yes' if status['market_open'] else 'No'}")
    print(f"  Near Close: {'Yes' if status['near_close'] else 'No'}")

    print_settings(monitor)

    # Market session
    session = get_market_session()
    session_labels = {"PRE": "Pre-Market", "REGULAR": "Regular", "AFTER": "After-Hours", "CLOSED": "Closed"}
    print(f"  Session: {session_labels[session]}")

    # Watchlist
    print(f"\n[Watchlist] ({status['watchlist_count']} items)")
    print("-" * 60)
    print(f"{'Symbol':<8} {'Target($)':>12} {'SL%':>8}")
    print("-" * 60)

    for item in monitor.watchlist:
        symbol = item['ticker']
        target = item['target_price']
        sl = item.get('stop_loss_pct') or monitor.trading_settings.STOP_LOSS_PCT
        print(f"{symbol:<8} {target:>12,.2f} {sl:>7.1f}%")

    print("-" * 60)

    # Open positions
    print(f"\n[Open Positions] ({status['open_positions']})")
    positions = monitor.order_service.get_open_positions()

    if positions:
        print("-" * 80)
        print(f"{'Symbol':<8} {'Qty':>6} {'Entry($)':>12} {'Current($)':>12} {'P/L%':>10} {'Stop($)':>12}")
        print("-" * 80)

        holdings_prices = load_holdings_prices_from_db()

        for pos in positions:
            symbol = pos['symbol']
            qty = pos['quantity']
            entry = pos['entry_price']
            stop_loss = pos.get('stop_loss_price', 0)
            current = pos.get('current_price', 0) or holdings_prices.get(symbol, {}).get('last', 0)

            if current > 0 and entry > 0:
                pnl_pct = ((current - entry) / entry) * 100
                pnl_str = f"{pnl_pct:+.2f}%"
                print(f"{symbol:<8} {qty:>6} {entry:>12,.2f} {current:>12,.2f} {pnl_str:>10} {stop_loss:>12,.2f}")
            else:
                print(f"{symbol:<8} {qty:>6} {entry:>12,.2f} {'---':>12} {'---':>10} {stop_loss:>12,.2f}")

        print("-" * 80)
    else:
        print("  No open positions")

    print()


def show_live_status(monitor: MonitorService, prices: dict, holdings_prices: dict = None, clear: bool = True):
    """Display live status with real-time prices."""
    import os
    now = datetime.now()

    if holdings_prices is None:
        holdings_prices = {}

    # Clear screen
    if clear:
        os.system('cls' if os.name == 'nt' else 'clear')

    # Get market session
    session = get_market_session()
    session_labels = {
        "PRE": "Pre-Market",
        "REGULAR": "Regular Hours",
        "AFTER": "After-Hours",
        "CLOSED": "Market Closed"
    }

    # 보유 종목 정보
    positions = {pos['symbol']: pos for pos in monitor.order_service.get_open_positions()}

    # Header
    print(f"[{now.strftime('%H:%M:%S.%f')[:12]}] Live Monitoring (US Stocks) [{session_labels[session]}]")
    print("=" * 90)

    # Watchlist section ($ in label only)
    print("[Watchlist]")
    print(f"{'Symbol':<8} {'Target($)':>12} {'Current($)':>12} {'Diff':>10} {'Status':>10}")
    print("-" * 65)

    for item in monitor.watchlist:
        ticker = item['ticker']
        target = item['target_price']

        price_data = prices.get(ticker, {})
        current = price_data.get('last', 0)
        if current <= 0:
            current = holdings_prices.get(ticker, {}).get('last', 0)

        if current > 0:
            if ticker in positions:
                pos = positions[ticker]
                entry = pos.get('entry_price', 0)
                stop_loss = pos.get('stop_loss_price', 0)
                if entry > 0:
                    pnl_pct = ((current - entry) / entry) * 100
                    diff_str = f"{pnl_pct:+.2f}%"
                    if current <= stop_loss:
                        status_str = "<<STOP!"
                    elif pnl_pct <= -5:
                        status_str = "WARNING"
                    elif pnl_pct > 0:
                        status_str = "HOLD ▲"
                    else:
                        status_str = "HOLD ▼"
                else:
                    diff_str = "---"
                    status_str = "HOLD"
            else:
                diff_pct = ((target - current) / current) * 100
                diff_str = f"{diff_pct:+.2f}%"
                if diff_pct <= 0:
                    status_str = "BREAKOUT!"
                elif diff_pct <= 1:
                    status_str = "NEAR"
                else:
                    status_str = "WAIT"

            print(f"{ticker:<8} {target:>12,.2f} {current:>12,.2f} {diff_str:>10} {status_str:>10}")
        else:
            print(f"{ticker:<8} {target:>12,.2f} {'---':>12} {'---':>10} {'LOADING':>10}")

    print("-" * 65)

    # Holdings Stop Loss Monitor section
    if positions:
        stop_loss_pct = monitor.trading_settings.STOP_LOSS_PCT
        print(f"\n[Holdings Stop Loss Monitor]")
        print(f"{'CODE':<8} {'NAME':<12} {'ENTRY($)':>12} {'CURRENT($)':>12} {'P/L%':>10} {'STOP($)':>12} {'STATUS':>8}")
        print("-" * 90)

        for symbol, pos in positions.items():
            entry = pos.get('entry_price', 0)
            stop_loss = pos.get('stop_loss_price', 0)
            qty = pos.get('quantity', 0)

            price_data = prices.get(symbol, {})
            current = price_data.get('last', 0)
            if current <= 0:
                current = holdings_prices.get(symbol, {}).get('last', 0)

            # 종목명 (watchlist에서 찾기)
            watchlist_item = next((w for w in monitor.watchlist if w['ticker'] == symbol), None)
            name = symbol  # 미국주식은 티커로 표시

            if current > 0 and entry > 0:
                pnl_pct = ((current - entry) / entry) * 100
                pnl_str = f"{pnl_pct:+.1f}%"

                if current <= stop_loss:
                    status = "STOP!"
                elif pnl_pct <= -stop_loss_pct * 0.7:
                    status = "WARN"
                else:
                    status = "OK"

                print(f"{symbol:<8} {name:<12} {entry:>12,.2f} {current:>12,.2f} {pnl_str:>10} {stop_loss:>12,.2f} {status:>8}")
            else:
                print(f"{symbol:<8} {name:<12} {entry:>12,.2f} {'---':>12} {'---':>10} {stop_loss:>12,.2f} {'---':>8}")

        print("-" * 90)

    # Bot Trades Today section
    bot_triggers = monitor.daily_triggers
    if bot_triggers:
        print(f"\n[Bot Trades Today] (auto-executed only)")
        print(f"{'Symbol':<8} {'Target($)':>10} {'Entry($)':>10} {'Current($)':>12} {'Return':>10} {'Status':>12}")
        print("-" * 75)

        stop_loss_pct = monitor.trading_settings.STOP_LOSS_PCT

        for symbol, trigger_info in bot_triggers.items():
            entry_price = trigger_info.get('entry_price', 0)

            watchlist_item = next((w for w in monitor.watchlist if w['ticker'] == symbol), None)
            target = watchlist_item['target_price'] if watchlist_item else entry_price

            price_data = prices.get(symbol, {})
            current = price_data.get('last', 0)
            if current <= 0:
                current = holdings_prices.get(symbol, {}).get('last', 0)

            if current > 0 and entry_price > 0:
                return_pct = ((current - entry_price) / entry_price) * 100
                return_str = f"{return_pct:+.2f}%"

                if return_pct <= -stop_loss_pct:
                    status = "STOP LOSS"
                elif return_pct < 0:
                    status = "HOLDING -"
                else:
                    status = "HOLDING +"

                print(f"{symbol:<8} {target:>10,.2f} {entry_price:>10,.2f} {current:>12,.2f} {return_str:>10} {status:>12}")
            else:
                print(f"{symbol:<8} {target:>10,.2f} {entry_price:>10,.2f} {'---':>12} {'---':>10} {'LOADING':>12}")

        print("-" * 75)
    else:
        print("\n[Bot Trades Today] No auto-executed trades yet")

    print("=" * 90)


def run_trading_loop():
    """Main trading loop with live price monitoring."""
    print_banner()

    if not test_connection():
        print("\nAPI connection failed. Exiting.")
        return

    monitor = MonitorService()
    monitor.load_watchlist()

    print_settings(monitor)

    # 보유종목 동기화: API → holdings 테이블 → positions
    print("\n[Holdings Sync] Syncing holdings from KIS API...")
    try:
        conn = get_connection()
        holdings_count = sync_holdings_from_kis(conn)
        conn.close()
        print(f"  Synced {holdings_count} holdings from API")
    except Exception as e:
        print(f"  [WARN] Holdings sync failed: {e}")

    print("[Holdings Sync] Loading positions from holdings DB...")
    synced = monitor.order_service.sync_positions_from_db(
        stop_loss_pct=monitor.trading_settings.STOP_LOSS_PCT
    )
    existing_positions = monitor.order_service.get_open_positions()
    if existing_positions:
        print(f"  Monitoring {len(existing_positions)} positions for stop loss")
        for pos in existing_positions:
            print(f"    - {pos['symbol']}: {pos['quantity']} @ ${pos['entry_price']:.2f} (SL: ${pos['stop_loss_price']:.2f})")

    # holdings 현재가 캐시 (초기 표시용)
    print("\n[Price Cache] Loading current prices from holdings DB...")
    holdings_prices = load_holdings_prices_from_db()
    print(f"  Loaded {len(holdings_prices)} prices from holdings")

    if not monitor.watchlist:
        print("\nNo items in watchlist. Please add stocks to watchlist.csv")
        return

    print(f"\n[Watchlist] {len(monitor.watchlist)} items loaded")
    for item in monitor.watchlist:
        print(f"  - {item['ticker']}: ${item['target_price']:.2f}")

    # Initialize price streaming (REST API polling)
    tickers = [item['ticker'] for item in monitor.watchlist]
    for pos in existing_positions:
        if pos['symbol'] not in tickers:
            tickers.append(pos['symbol'])

    print("\n[Price Streaming] Initializing REST API polling...")

    poller = RestPricePoller(interval=2.0)
    poller.subscribe(tickers)
    poller.start()

    print(f"  Polling {len(tickers)} stocks every 2 seconds")

    print("\n" + "=" * 70)
    print("Starting trading loop... (Ctrl+C to stop)")
    print("=" * 70)

    trade_logger.log_system_event("START", f"watchlist={len(monitor.watchlist)} items")

    # Monitoring intervals
    STATUS_INTERVAL = 3  # Show status every 3 seconds
    CHECK_INTERVAL = 1   # Check prices every 1 second

    last_date = None
    last_status_time = 0

    try:
        while True:
            now = datetime.now()
            today = now.date()
            current_time = time.time()

            # Reset daily triggers on new day
            if last_date != today:
                monitor.reset_daily_triggers()
                monitor.load_watchlist()
                last_date = today

            # Get current prices and update monitor's cache
            prices = poller.get_prices()
            monitor.update_price_cache(prices)

            # Show live status periodically
            if current_time - last_status_time >= STATUS_INTERVAL:
                show_live_status(monitor, prices, holdings_prices)
                last_status_time = current_time

            # Check market status
            status = monitor.get_status()

            if status["market_open"]:
                # Run monitoring cycle
                result = monitor.run_monitoring_cycle()

                if result.get("reloaded"):
                    print(f"[{now.strftime('%H:%M:%S')}] RELOADED: watchlist & settings")
                    print_settings(monitor)
                    # Subscribe new symbols to poller
                    new_tickers = [item['ticker'] for item in monitor.watchlist]
                    poller.subscribe(new_tickers)

                if result["entries"]:
                    for entry in result["entries"]:
                        print(f"[{now.strftime('%H:%M:%S')}] ENTRY: {entry['symbol']} ({entry['type']})")

                if result["stop_losses"]:
                    for symbol in result["stop_losses"]:
                        print(f"[{now.strftime('%H:%M:%S')}] STOP LOSS: {symbol}")

                if result["close_actions"]:
                    for symbol, action in result["close_actions"].items():
                        print(f"[{now.strftime('%H:%M:%S')}] CLOSE: {symbol} -> {action}")

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n\nTrading loop stopped by user.")
        trade_logger.log_system_event("STOP", "user interrupt")

    # Cleanup
    poller.stop()

    print("\n[Final Status]")
    show_status()


def main():
    """Main entry point."""
    if "--test" in sys.argv:
        test_connection()
    elif "--status" in sys.argv:
        show_status()
    else:
        run_trading_loop()


if __name__ == "__main__":
    main()
