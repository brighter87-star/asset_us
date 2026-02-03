"""
Monitor service for price monitoring and trading strategy execution.
"""

import os
import pandas as pd
from datetime import datetime, time
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

# US Eastern timezone
ET = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")


class TradingSettings:
    """Trading settings loaded from Excel."""

    # 1 unit = 항상 자산의 5% (고정)
    UNIT_BASE_PERCENT: float = 5.0

    def __init__(self):
        self.UNIT: int = 1              # 총 몇 unit (1, 2, 3...)
        self.TICK_BUFFER: int = 3       # 목표가 + N틱
        self.STOP_LOSS_PCT: float = 7.0 # 손절 %

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
                  f"TICK={self.trading_settings.TICK_BUFFER}, "
                  f"SL={self.trading_settings.STOP_LOSS_PCT}%")

            trade_logger.log_settings_change({
                "UNIT": self.trading_settings.UNIT,
                "TICK_BUFFER": self.trading_settings.TICK_BUFFER,
                "STOP_LOSS_PCT": self.trading_settings.STOP_LOSS_PCT,
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
                }

                # Optional custom stop loss
                if "stop_loss_pct" in row and not pd.isna(row["stop_loss_pct"]):
                    item["stop_loss_pct"] = float(row["stop_loss_pct"])

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
        """Get current price for symbol."""
        try:
            return self.client.get_current_price(symbol, "NAS")
        except Exception as e:
            print(f"[{symbol}] Failed to get price: {e}")
            return None

    def check_breakout_entry(self, item: dict) -> bool:
        """
        Check if breakout entry condition is met.

        Returns True if:
        - Current price >= target price (+ tick buffer applied in order)
        - Not already triggered today
        - Not already have position
        """
        symbol = item["ticker"]
        target_price = item["target_price"]

        # Already triggered today?
        if symbol in self.daily_triggers:
            return False

        # Already have position?
        if self.order_service.has_position(symbol):
            return False

        # Get current price
        price_data = self.get_price(symbol)
        if not price_data:
            return False

        current_price = price_data["last"]

        # Check breakout
        trigger_price = target_price + (self.trading_settings.TICK_BUFFER * 0.01)

        if current_price >= trigger_price:
            print(f"[{symbol}] BREAKOUT: ${current_price:.2f} >= ${trigger_price:.2f}")
            return True

        return False

    def check_gap_up_entry(self, item: dict) -> bool:
        """
        Check if gap-up entry condition is met at market open.

        Returns True if:
        - It's market open time
        - Open price > target price
        - Not already triggered today
        - Not already have position
        """
        symbol = item["ticker"]
        target_price = item["target_price"]

        if symbol in self.daily_triggers:
            return False

        if self.order_service.has_position(symbol):
            return False

        price_data = self.get_price(symbol)
        if not price_data:
            return False

        open_price = price_data["open"]
        trigger_price = target_price + (self.trading_settings.TICK_BUFFER * 0.01)

        if open_price >= trigger_price:
            print(f"[{symbol}] GAP UP: Open ${open_price:.2f} >= ${trigger_price:.2f}")
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

        # Use current price for gap up, target price for breakout
        if is_gap_up:
            entry_price = price_data["last"]
        else:
            entry_price = target_price

        result = self.order_service.execute_buy(
            symbol=symbol,
            target_price=entry_price,
            is_initial=True,
            stop_loss_pct=stop_loss_pct,
        )

        if result:
            self.daily_triggers[symbol] = {
                "entry_type": "gap_up" if is_gap_up else "breakout",
                "entry_time": datetime.now().isoformat(),
                "entry_price": entry_price,
            }
            return True

        return False

    def check_and_execute_stop_loss(self) -> List[str]:
        """
        Check stop loss for all open positions.

        Returns list of symbols that were stopped out.
        """
        stopped = []

        for pos in self.order_service.get_open_positions():
            symbol = pos["symbol"]

            price_data = self.get_price(symbol)
            if not price_data:
                continue

            current_price = price_data["last"]

            if self.order_service.check_stop_loss(symbol, current_price):
                result = self.order_service.execute_sell(
                    symbol=symbol,
                    price=current_price,
                    reason="stop_loss",
                )
                if result:
                    stopped.append(symbol)

        return stopped

    def execute_close_logic(self) -> Dict[str, str]:
        """
        Execute end-of-day logic for TODAY's entries only.

        Logic (only for stocks entered today via daily_triggers):
        - If close > entry: Add 0.5 unit (pyramid)
        - If close < entry: Sell all (cut loss)

        Pre-existing positions (not in daily_triggers) are NOT affected.

        Returns dict of {symbol: action_taken}
        """
        actions = {}

        # Only process stocks that were entered TODAY
        if not self.daily_triggers:
            print("[CLOSE] No daily triggers - skipping close logic")
            return actions

        for pos in self.order_service.get_open_positions():
            symbol = pos["symbol"]

            # Skip if not entered today
            if symbol not in self.daily_triggers:
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
