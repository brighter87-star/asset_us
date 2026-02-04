"""
Monitor service for price monitoring and trading strategy execution.
"""

import json
import os
import pandas as pd
from datetime import datetime, date, time
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

from services.kis_service import KISAPIClient
from services.order_service import OrderService
from services.trade_logger import trade_logger

# Watchlist file paths (CSV takes priority over xlsx)
WATCHLIST_DIR = Path(__file__).resolve().parent.parent
WATCHLIST_CSV = WATCHLIST_DIR / "watchlist.csv"
WATCHLIST_XLSX = WATCHLIST_DIR / "watchlist.xlsx"
SETTINGS_CSV = WATCHLIST_DIR / "settings.csv"
DAILY_TRIGGERS_FILE = WATCHLIST_DIR / ".daily_triggers.json"
POSITION_TRACKING_FILE = WATCHLIST_DIR / ".position_tracking.json"  # Tracks bought units per symbol

# US Eastern timezone
ET = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")


class TradingSettings:
    """Trading settings loaded from Excel."""

    # 1 unit = 항상 자산의 5% (고정)
    UNIT_BASE_PERCENT: float = 5.0

    def __init__(self):
        self.UNIT: float = 1.0          # 총 몇 unit (0.5, 1, 2...)
        self.STOP_LOSS_PCT: float = 7.0 # 손절 %
        self.PRICE_BUFFER_PCT: float = 0.5  # 주문가 버퍼 % (현재가 * 1.005)

    def update(self, key: str, value):
        """Update setting value."""
        if hasattr(self, key):
            expected_type = type(getattr(self, key))
            setattr(self, key, expected_type(value))

    def get_unit_percent(self) -> float:
        """Get total percentage for position (UNIT * 5%)."""
        return self.UNIT * self.UNIT_BASE_PERCENT

    def get_half_unit_percent(self) -> float:
        """Get half unit percentage for each buy (UNIT/2 * 5%)."""
        return (self.UNIT / 2) * self.UNIT_BASE_PERCENT


class MonitorService:
    """
    Monitors prices and executes trading strategy.
    """

    def __init__(self):
        self.trading_settings = TradingSettings()
        self.client = KISAPIClient()
        self.order_service = OrderService(settings=self.trading_settings)
        self.watchlist: List[dict] = []
        self.daily_triggers: Dict[str, dict] = {}  # Track triggered entries today
        self._file_mtime: float = 0  # File modification time
        self._pre_market_reloaded: bool = False  # Track pre-market reload
        self._price_cache: Dict[str, dict] = {}  # Cache from poller
        self._total_assets: float = 0  # Cached total assets for unit calculation
        self._load_daily_triggers()  # Load persisted bot trades

    def _load_daily_triggers(self):
        """Load daily triggers from file (only if same day)."""
        try:
            if DAILY_TRIGGERS_FILE.exists():
                with open(DAILY_TRIGGERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Only load if same date
                saved_date = data.get("date")
                if saved_date == str(date.today()):
                    self.daily_triggers = data.get("triggers", {})
                    print(f"[TRIGGERS] Loaded {len(self.daily_triggers)} bot trades from today")
                else:
                    # Different day, start fresh
                    self.daily_triggers = {}
                    print(f"[TRIGGERS] New day, cleared previous triggers")
        except Exception as e:
            print(f"[WARN] Failed to load daily triggers: {e}")
            self.daily_triggers = {}

    def _save_daily_triggers(self):
        """Save daily triggers to file."""
        try:
            data = {
                "date": str(date.today()),
                "triggers": self.daily_triggers,
            }
            with open(DAILY_TRIGGERS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            print(f"[WARN] Failed to save daily triggers: {e}")

    def update_price_cache(self, prices: Dict[str, dict]):
        """Update price cache from external source (e.g., poller)."""
        self._price_cache = prices

    def _get_watchlist_file(self) -> Optional[Path]:
        """Get watchlist file path (CSV priority)."""
        if WATCHLIST_CSV.exists():
            return WATCHLIST_CSV
        if WATCHLIST_XLSX.exists():
            return WATCHLIST_XLSX
        return None

    def _get_file_mtime(self) -> float:
        """Get file modification time."""
        try:
            watchlist_file = self._get_watchlist_file()
            if watchlist_file:
                mtime = os.path.getmtime(watchlist_file)
                if SETTINGS_CSV.exists():
                    mtime = max(mtime, os.path.getmtime(SETTINGS_CSV))
                return mtime
            return 0
        except Exception:
            return 0

    def _check_file_changed(self) -> bool:
        """Check if file has been modified since last load."""
        current_mtime = self._get_file_mtime()
        if current_mtime > self._file_mtime:
            return True
        return False

    def load_settings(self) -> bool:
        """
        Load settings from CSV or Excel 'settings' sheet.

        Expected columns:
        - key: Setting name (UNIT, TICK_BUFFER, STOP_LOSS_PCT)
        - value: Setting value
        """
        try:
            df = None

            # CSV takes priority
            if SETTINGS_CSV.exists():
                df = pd.read_csv(SETTINGS_CSV)
                print(f"[SETTINGS] Loading from {SETTINGS_CSV.name}")
            elif WATCHLIST_XLSX.exists():
                try:
                    df = pd.read_excel(WATCHLIST_XLSX, sheet_name="settings")
                except Exception:
                    pass

            if df is None or df.empty:
                return False

            df.columns = df.columns.str.lower().str.strip()

            for _, row in df.iterrows():
                key = str(row.get("key", "")).strip().upper()
                value = row.get("value")

                if key and not pd.isna(value):
                    self.trading_settings.update(key, value)

            print(f"[SETTINGS] UNIT={self.trading_settings.UNIT} "
                  f"({self.trading_settings.get_unit_percent()}%), "
                  f"SL={self.trading_settings.STOP_LOSS_PCT}%, "
                  f"BUFFER={self.trading_settings.PRICE_BUFFER_PCT}%")

            trade_logger.log_settings_change({
                "UNIT": self.trading_settings.UNIT,
                "STOP_LOSS_PCT": self.trading_settings.STOP_LOSS_PCT,
                "PRICE_BUFFER_PCT": self.trading_settings.PRICE_BUFFER_PCT,
            })
            return True

        except Exception as e:
            print(f"[WARNING] Failed to load settings: {e}")
            return False

    def load_watchlist(self) -> List[dict]:
        """
        Load watchlist from CSV or Excel 'watchlist' sheet.
        CSV takes priority over xlsx.

        Expected columns:
        - ticker: Stock symbol (e.g., AAPL)
        - target_price: Target price for breakout
        - stop_loss_pct: (Optional) Custom stop loss %
        """
        watchlist_file = self._get_watchlist_file()
        if not watchlist_file:
            print(f"[WARNING] Watchlist not found (watchlist.csv or watchlist.xlsx)")
            return []

        try:
            # Load settings first
            self.load_settings()

            # Load watchlist (CSV or xlsx)
            if watchlist_file.suffix == '.csv':
                df = pd.read_csv(watchlist_file)
                print(f"[INFO] Loading watchlist from {watchlist_file.name}")
            else:
                df = pd.read_excel(watchlist_file, sheet_name="watchlist")

            # Normalize column names
            df.columns = df.columns.str.lower().str.strip()

            watchlist = []
            for _, row in df.iterrows():
                ticker = str(row.get("ticker", "")).strip().upper()
                target_price = row.get("target_price")

                if not ticker or pd.isna(target_price):
                    continue

                item = {
                    "ticker": ticker,
                    "target_price": float(target_price),
                    "stop_loss_pct": None,
                    "max_units": 1,  # Default: 1 unit (single entry)
                    "added_date": None,
                }

                # Optional custom stop loss
                if "stop_loss_pct" in row and not pd.isna(row["stop_loss_pct"]):
                    item["stop_loss_pct"] = float(row["stop_loss_pct"])

                # max_units: how many units to buy (allows pyramiding)
                if "max_units" in row and not pd.isna(row["max_units"]):
                    item["max_units"] = int(row["max_units"])

                # added_date: when this item was added to watchlist
                if "added_date" in row and not pd.isna(row["added_date"]):
                    item["added_date"] = str(row["added_date"])

                watchlist.append(item)

            self.watchlist = watchlist
            self._file_mtime = self._get_file_mtime()
            print(f"[INFO] Loaded {len(watchlist)} items from watchlist")
            return watchlist

        except Exception as e:
            print(f"[ERROR] Failed to load watchlist: {e}")
            return []

    def reload_if_changed(self) -> bool:
        """Reload watchlist if file has changed."""
        if self._check_file_changed():
            print(f"[INFO] File changed, reloading...")
            self.load_watchlist()
            return True
        return False

    def get_current_time_et(self) -> datetime:
        """Get current time in US Eastern."""
        return datetime.now(ET)

    def get_current_time_kst(self) -> datetime:
        """Get current time in Korea."""
        return datetime.now(KST)

    def is_market_open(self) -> bool:
        """Check if US market is open (9:30 AM - 4:00 PM ET)."""
        now_et = self.get_current_time_et()
        market_open = time(9, 30)
        market_close = time(16, 0)

        # Check weekday (Mon=0, Sun=6)
        if now_et.weekday() >= 5:
            return False

        current_time = now_et.time()
        return market_open <= current_time < market_close

    def is_near_market_close(self, minutes: int = 5) -> bool:
        """Check if we're within N minutes of market close."""
        now_et = self.get_current_time_et()
        market_close = time(16, 0)

        if now_et.weekday() >= 5:
            return False

        current_time = now_et.time()

        # Calculate minutes until close
        close_minutes = market_close.hour * 60 + market_close.minute
        current_minutes = current_time.hour * 60 + current_time.minute

        minutes_until_close = close_minutes - current_minutes

        return 0 < minutes_until_close <= minutes

    def is_market_open_time(self) -> bool:
        """Check if it's exactly market open time (within first minute)."""
        now_et = self.get_current_time_et()
        current_time = now_et.time()

        market_open_start = time(9, 30)
        market_open_end = time(9, 31)

        return market_open_start <= current_time < market_open_end

    def is_pre_market_time(self) -> bool:
        """Check if it's 5 minutes before market open (9:25 AM ET)."""
        now_et = self.get_current_time_et()

        if now_et.weekday() >= 5:
            return False

        current_time = now_et.time()
        pre_market_start = time(9, 25)
        pre_market_end = time(9, 26)

        return pre_market_start <= current_time < pre_market_end

    def check_pre_market_reload(self) -> bool:
        """
        Check and perform pre-market reload (5 min before open).
        Returns True if reload was performed.
        """
        if not self.is_pre_market_time():
            self._pre_market_reloaded = False
            return False

        if self._pre_market_reloaded:
            return False

        print(f"[PRE-MARKET] Reloading settings and watchlist...")
        self.load_watchlist()
        self._pre_market_reloaded = True
        return True

    def get_price(self, symbol: str) -> Optional[dict]:
        """Get current price for symbol (uses cache first, then API)."""
        # Check cache first (from poller)
        if symbol in self._price_cache:
            cached = self._price_cache[symbol]
            if cached and cached.get("last", 0) > 0:
                return cached

        # Fallback to API (try multiple exchanges)
        exchanges = ["NAS", "NYS", "AMS"]
        for exchange in exchanges:
            try:
                result = self.client.get_current_price(symbol, exchange)
                if result and result.get("last", 0) > 0:
                    return result
            except Exception:
                continue

        return None

    def get_total_assets(self, force_refresh: bool = False) -> float:
        """
        Get total portfolio value (외화잔고).

        외화잔고 = 외화주문가능금액 + 전체 주식평가액

        This matches the "외화잔고" shown in HTS/MTS.
        """
        if self._total_assets > 0 and not force_refresh:
            return self._total_assets

        total = 0.0

        # 1. Get 외화주문가능금액 from buying power API (ord_psbl_frcr_amt)
        try:
            import requests
            url = f"{self.client.base_url}/uapi/overseas-stock/v1/trading/inquire-psamount"
            headers = self.client._get_headers("TTTS3007R")
            params = {
                "CANO": self.client.cano,
                "ACNT_PRDT_CD": self.client.acnt_prdt_cd,
                "OVRS_EXCG_CD": "NASD",
                "OVRS_ORD_UNPR": "100",
                "ITEM_CD": "AAPL",
            }
            self.client._wait_for_rate_limit()
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            output = data.get("output", {})
            # ord_psbl_frcr_amt = 외화주문가능금액 (NOT ovrs_ord_psbl_amt)
            cash = float(output.get("ord_psbl_frcr_amt", 0) or 0)
            total += cash
        except Exception as e:
            print(f"[WARN] Failed to get buying power: {e}")

        # 2. Get stock evaluation from holdings (all exchanges)
        try:
            from db.connection import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                # Sum of all holdings evaluation amounts (USD)
                cur.execute("""
                    SELECT SUM(evlt_amt) FROM holdings
                    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM holdings)
                """)
                row = cur.fetchone()
                if row and row[0]:
                    total += float(row[0])
            conn.close()
        except Exception as e:
            print(f"[WARN] Failed to get holdings sum: {e}")
            # Fallback: get directly from API
            try:
                for exc in ["NASD", "NYSE", "AMEX"]:
                    try:
                        holdings = self.client.get_holdings(exchange_code=exc, currency="USD")
                        for h in holdings:
                            total += float(h.get("ovrs_stck_evlu_amt", 0) or 0)
                    except Exception:
                        pass
            except Exception:
                pass

        if total > 0:
            self._total_assets = total

        return self._total_assets

    def get_current_units_held(self, symbol: str) -> float:
        """
        Calculate how many units are currently held for a symbol.
        Uses PURCHASE COST (not current value) to avoid the problem where
        a stock that doubled would show 2x units.

        1 unit = 5% of total portfolio value

        Example:
        - Total assets: $100,000, 1 unit = $5,000
        - Bought $4,500 of AAPL (0.9 units)
        - AAPL doubles to $9,000 market value
        - current_units = $4,500 / $5,000 = 0.9 (not 1.8!)
        """
        total_assets = self.get_total_assets()
        if total_assets <= 0:
            return 0

        unit_value = total_assets * 0.05  # 1 unit = 5%

        # Get PURCHASE COST from holdings (not market value)
        try:
            from db.connection import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                # Use pur_amt (purchase amount) instead of evlt_amt (evaluation amount)
                cur.execute("""
                    SELECT pur_amt FROM holdings
                    WHERE stk_cd = %s
                    ORDER BY snapshot_date DESC LIMIT 1
                """, (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    purchase_cost = float(row[0])
                    current_units = purchase_cost / unit_value
                    return current_units
            conn.close()
        except Exception as e:
            print(f"[WARN] Failed to get holdings for {symbol}: {e}")

        # Fallback: check daily_lots for total_cost
        try:
            from db.connection import get_connection
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT SUM(total_cost) FROM daily_lots
                    WHERE stock_code = %s AND net_quantity > 0
                """, (symbol,))
                row = cur.fetchone()
                if row and row[0]:
                    total_cost = float(row[0])
                    return total_cost / unit_value
            conn.close()
        except Exception:
            pass

        # Fallback: check order_service positions
        for pos in self.order_service.get_open_positions():
            if pos["symbol"] == symbol:
                qty = pos.get("quantity", 0)
                entry_price = pos.get("entry_price", 0)
                purchase_cost = qty * entry_price
                return purchase_cost / unit_value if unit_value > 0 else 0

        return 0

    def check_breakout_entry(self, item: dict) -> bool:
        """
        Check if breakout entry condition is met.

        Returns True if:
        - Current price >= target price (+ tick buffer applied in order)
        - Not already triggered today (for this unit)
        - current_units < max_units (allows pyramiding based on actual holdings)
        """
        symbol = item["ticker"]
        target_price = item["target_price"]
        max_units = item.get("max_units", 1)

        # Check how many units already held (from actual holdings, includes manual buys)
        current_units = self.get_current_units_held(symbol)

        # Already at max units?
        if current_units >= max_units:
            return False

        # Count today's bot triggers for this symbol
        today_triggers = self.daily_triggers.get(symbol, {}).get("trigger_count", 0)

        # Max 1 trigger per day per symbol (to avoid rapid-fire buying on volatile days)
        if today_triggers >= 1:
            return False

        # Get current price
        price_data = self.get_price(symbol)
        if not price_data:
            return False

        current_price = price_data["last"]

        # Check breakout (current >= target)
        if current_price >= target_price:
            remaining_units = max_units - current_units
            print(f"[{symbol}] BREAKOUT: ${current_price:.2f} >= ${target_price:.2f}")
            print(f"[{symbol}] Units: {current_units:.2f}/{max_units} held, {remaining_units:.2f} remaining")
            return True

        return False

    def check_gap_up_entry(self, item: dict) -> bool:
        """
        Check if gap-up entry condition is met at market open.

        Returns True if:
        - It's market open time
        - Open price > target price
        - current_units < max_units (allows pyramiding)
        - Not already triggered today
        """
        symbol = item["ticker"]
        target_price = item["target_price"]
        max_units = item.get("max_units", 1)

        # Check how many units already held (from actual holdings, includes manual buys)
        current_units = self.get_current_units_held(symbol)

        # Already at max units?
        if current_units >= max_units:
            return False

        # Count today's bot triggers for this symbol
        today_triggers = self.daily_triggers.get(symbol, {}).get("trigger_count", 0)
        if today_triggers >= 1:
            return False

        price_data = self.get_price(symbol)
        if not price_data:
            return False

        open_price = price_data["open"]

        # Gap up: open >= target
        if open_price >= target_price:
            remaining_units = max_units - current_units
            print(f"[{symbol}] GAP UP: Open ${open_price:.2f} >= ${target_price:.2f}")
            print(f"[{symbol}] Units: {current_units:.2f}/{max_units} held, {remaining_units:.2f} remaining")
            return True

        return False

    def execute_entry(self, item: dict, is_gap_up: bool = False) -> bool:
        """Execute entry order."""
        symbol = item["ticker"]
        target_price = item["target_price"]
        stop_loss_pct = item.get("stop_loss_pct")

        price_data = self.get_price(symbol)
        if not price_data:
            return False

        # Always use current price for entry (ensures order fills)
        # Target price is just the trigger, actual buy is at market price
        entry_price = price_data["last"]

        result = self.order_service.execute_buy(
            symbol=symbol,
            target_price=entry_price,
            is_initial=True,
            stop_loss_pct=stop_loss_pct,
        )

        if result:
            # Track trigger count for the day (allows checking max 1 trigger per day)
            existing = self.daily_triggers.get(symbol, {})
            trigger_count = existing.get("trigger_count", 0) + 1

            self.daily_triggers[symbol] = {
                "entry_type": "gap_up" if is_gap_up else "breakout",
                "entry_time": datetime.now().isoformat(),
                "entry_price": entry_price,
                "trigger_count": trigger_count,
            }
            self._save_daily_triggers()  # Persist to file
            return True

        return False

    def check_and_execute_stop_loss(self) -> List[str]:
        """
        Check stop loss for TODAY's bought positions only (real-time).
        Previous days' positions are checked at market close.

        Returns list of symbols that were stopped out.
        """
        stopped = []

        # Only check today's bought stocks for real-time stop loss
        today_bought = self._get_today_bought_symbols()
        today_bought.update(self.daily_triggers.keys())

        if not today_bought:
            return stopped

        for pos in self.order_service.get_open_positions():
            symbol = pos["symbol"]

            # Skip if not bought today
            if symbol not in today_bought:
                continue

            price_data = self.get_price(symbol)
            if not price_data:
                continue

            current_price = price_data["last"]

            if self.order_service.check_stop_loss(symbol, current_price):
                result = self.order_service.execute_sell(
                    symbol=symbol,
                    price=current_price,
                    reason="stop_loss_intraday",
                )
                if result:
                    stopped.append(symbol)

        return stopped

    def check_previous_holdings_stop_loss(self) -> List[str]:
        """
        Check stop loss for PREVIOUS days' holdings at market close.
        Uses close price to determine if -7% threshold is breached.

        Returns list of symbols that were stopped out.
        """
        stopped = []

        today_bought = self._get_today_bought_symbols()
        today_bought.update(self.daily_triggers.keys())

        for pos in self.order_service.get_open_positions():
            symbol = pos["symbol"]

            # Skip if bought today (already handled by real-time stop loss)
            if symbol in today_bought:
                continue

            price_data = self.get_price(symbol)
            if not price_data:
                continue

            current_price = price_data["last"]

            if self.order_service.check_stop_loss(symbol, current_price):
                print(f"[{symbol}] Previous holding close below stop loss: ${current_price:.2f}")
                result = self.order_service.execute_sell(
                    symbol=symbol,
                    price=current_price,
                    reason="stop_loss_close",
                )
                if result:
                    stopped.append(symbol)

        return stopped

    def _get_today_bought_symbols(self) -> set:
        """Get symbols bought today from trade history (includes manual buys)."""
        from db.connection import get_connection
        from datetime import date

        try:
            conn = get_connection()
            today = date.today()

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT stk_cd
                    FROM account_trade_history
                    WHERE trade_date = %s AND io_tp_nm LIKE '%%매수%%'
                """, (today,))
                rows = cur.fetchall()

            conn.close()
            return {row[0] for row in rows if row[0]}
        except Exception as e:
            print(f"[WARN] Failed to get today's buys: {e}")
            return set()

    def _sync_trade_history_before_close(self):
        """Sync trade history and holdings from API before close logic (catch manual buys)."""
        if hasattr(self, '_close_synced') and self._close_synced:
            return  # Already synced today

        try:
            from db.connection import get_connection
            from services.data_sync_service import sync_trade_history_from_kis, sync_holdings_from_kis

            conn = get_connection()

            # 1. Sync trade history (for today_bought detection)
            print("[CLOSE] Syncing trade history...")
            trade_count = sync_trade_history_from_kis(conn)
            print(f"[CLOSE] Synced {trade_count} trades")

            # 2. Sync holdings (for current positions)
            print("[CLOSE] Syncing holdings...")
            holdings_count = sync_holdings_from_kis(conn)
            print(f"[CLOSE] Synced {holdings_count} holdings")

            conn.close()

            # 3. Refresh positions from holdings DB (includes manual buys)
            print("[CLOSE] Refreshing positions from holdings...")
            self.order_service.sync_positions_from_db(
                stop_loss_pct=self.trading_settings.STOP_LOSS_PCT
            )

            self._close_synced = True
        except Exception as e:
            print(f"[WARN] Failed to sync before close: {e}")

    def execute_close_logic(self) -> Dict[str, str]:
        """
        Execute end-of-day logic for TODAY's entries only.

        Logic (stocks entered today - both auto and manual):
        - If close > entry: Add 0.5 unit (pyramid)
        - If close < entry: Sell all (cut loss)

        Pre-existing positions (bought before today) are NOT affected.

        Returns dict of {symbol: action_taken}
        """
        actions = {}

        # Get today's bought symbols (auto + manual)
        today_bought = self._get_today_bought_symbols()
        today_bought.update(self.daily_triggers.keys())

        if not today_bought:
            print("[CLOSE] No stocks bought today - skipping close logic")
            return actions

        print(f"[CLOSE] Checking {len(today_bought)} stocks bought today: {today_bought}")

        for pos in self.order_service.get_open_positions():
            symbol = pos["symbol"]

            # Skip if not bought today
            if symbol not in today_bought:
                continue

            entry_price = pos.get("entry_price", 0)

            price_data = self.get_price(symbol)
            if not price_data:
                continue

            current_price = price_data["last"]

            if current_price > entry_price:
                # Profitable - pyramid
                print(f"[{symbol}] Close ${current_price:.2f} > Entry ${entry_price:.2f} - PYRAMID")

                # Find watchlist item for stop_loss_pct
                watchlist_item = next(
                    (w for w in self.watchlist if w["ticker"] == symbol),
                    None
                )
                stop_loss_pct = watchlist_item.get("stop_loss_pct") if watchlist_item else None

                result = self.order_service.execute_buy(
                    symbol=symbol,
                    target_price=current_price,
                    is_initial=False,
                    stop_loss_pct=stop_loss_pct,
                )

                if result:
                    actions[symbol] = "pyramid"
                else:
                    actions[symbol] = "pyramid_failed"

            else:
                # Loss - sell all
                print(f"[{symbol}] Close ${current_price:.2f} <= Entry ${entry_price:.2f} - SELL")

                result = self.order_service.execute_sell(
                    symbol=symbol,
                    price=current_price,
                    reason="close_below_entry",
                )

                if result:
                    actions[symbol] = "sold"
                else:
                    actions[symbol] = "sell_failed"

        return actions

    def reset_daily_triggers(self):
        """Reset daily triggers (call at start of new trading day)."""
        self.daily_triggers = {}
        self._save_daily_triggers()  # Persist reset
        print("[INFO] Daily triggers reset")

    def run_monitoring_cycle(self) -> dict:
        """
        Run one monitoring cycle.

        Returns dict with actions taken.
        """
        result = {
            "timestamp": datetime.now().isoformat(),
            "market_open": self.is_market_open(),
            "entries": [],
            "stop_losses": [],
            "close_actions": {},
            "reloaded": False,
        }

        # Check for file changes
        if self.reload_if_changed():
            result["reloaded"] = True

        # Pre-market reload (5 min before open)
        if self.check_pre_market_reload():
            result["reloaded"] = True

        if not self.is_market_open():
            return result

        # Check market open gap-up entries
        if self.is_market_open_time():
            for item in self.watchlist:
                if self.check_gap_up_entry(item):
                    if self.execute_entry(item, is_gap_up=True):
                        result["entries"].append({
                            "symbol": item["ticker"],
                            "type": "gap_up",
                        })

        # Check breakout entries
        for item in self.watchlist:
            if self.check_breakout_entry(item):
                if self.execute_entry(item, is_gap_up=False):
                    result["entries"].append({
                        "symbol": item["ticker"],
                        "type": "breakout",
                    })

        # Check stop losses
        stopped = self.check_and_execute_stop_loss()
        result["stop_losses"] = stopped

        # Execute close logic near market close
        if self.is_near_market_close(5):
            # Sync trade history first (to catch manual buys)
            self._sync_trade_history_before_close()

            # Check previous holdings stop loss (close price based)
            prev_stopped = self.check_previous_holdings_stop_loss()
            result["stop_losses"].extend(prev_stopped)

            # Execute close logic for today's entries
            result["close_actions"] = self.execute_close_logic()

        return result

    def get_status(self) -> dict:
        """Get current monitoring status."""
        return {
            "current_time_et": self.get_current_time_et().isoformat(),
            "current_time_kst": self.get_current_time_kst().isoformat(),
            "market_open": self.is_market_open(),
            "near_close": self.is_near_market_close(5),
            "watchlist_count": len(self.watchlist),
            "open_positions": len(self.order_service.get_open_positions()),
            "daily_triggers": len(self.daily_triggers),
        }
