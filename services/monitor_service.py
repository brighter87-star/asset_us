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
        self._last_triggers_hash: str = ""  # Track changes to avoid repeated logs
        self._skipped_symbols: Dict[str, float] = {}  # Track skipped symbols with timestamp (for one-time logging)
        self._today_api_buys: Optional[set] = None  # Cache today's API buy symbols
        self._load_daily_triggers(verbose=True)  # Load persisted bot trades
        self._merge_api_buys_to_triggers()  # Also load from KIS API directly

    def _get_today_et(self) -> date:
        """Get today's date in US Eastern time."""
        return datetime.now(ET).date()

    def _load_daily_triggers(self, verbose: bool = False):
        """
        Load daily triggers from both DB and JSON file.
        - DB: source of truth for completed/synced trades
        - JSON: backup for very recent trades (API sync delay)
        """
        today_et = self._get_today_et()
        self.daily_triggers = {}

        # 1. Load from JSON file first (catches recent trades not yet in DB)
        self._load_triggers_from_json(today_et)

        # 2. Load from DB (source of truth, will add any missing)
        self._load_today_trades_from_db(today_et)

        # 3. Only log if changed or verbose
        self._log_triggers_if_changed(verbose)

    def _load_triggers_from_json(self, today_et: date):
        """Load triggers from JSON file (backup for API sync delay)."""
        try:
            if DAILY_TRIGGERS_FILE.exists():
                with open(DAILY_TRIGGERS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Only load if same date (US ET)
                saved_date = data.get("date")
                if saved_date == str(today_et):
                    json_triggers = data.get("triggers", {})
                    for symbol, info in json_triggers.items():
                        if symbol not in self.daily_triggers:
                            self.daily_triggers[symbol] = info
        except Exception as e:
            print(f"[WARN] Failed to load from JSON: {e}")

    def _load_today_trades_from_db(self, today_et: date):
        """Load today's trades from database and merge with existing triggers.

        Note: Queries last 2 days to catch overnight trades
        (e.g., if it's Feb 5 01:00 ET, Feb 4 market trades have trade_date=2026-02-04)
        """
        try:
            from datetime import timedelta
            from db.connection import get_connection

            yesterday_et = today_et - timedelta(days=1)

            conn = get_connection()
            with conn.cursor() as cur:
                # Get last 2 days buy trades (매수)
                cur.execute("""
                    SELECT stk_cd, cntr_uv, ord_tm
                    FROM account_trade_history
                    WHERE trade_date >= %s AND io_tp_nm LIKE '%%매수%%'
                    ORDER BY ord_tm
                """, (yesterday_et,))
                buys = cur.fetchall()

                # Get last 2 days sell trades (매도) - for stop loss detection
                cur.execute("""
                    SELECT stk_cd, cntr_uv, ord_tm
                    FROM account_trade_history
                    WHERE trade_date >= %s AND io_tp_nm LIKE '%%매도%%'
                    ORDER BY ord_tm
                """, (yesterday_et,))
                sells = cur.fetchall()
            conn.close()

            # Track sold symbols
            sold_symbols = set(row[0] for row in sells)

            # Count DB trades per symbol
            db_trade_counts = {}
            for stk_cd, cntr_uv, ord_tm in buys:
                if stk_cd not in db_trade_counts:
                    db_trade_counts[stk_cd] = {
                        "count": 1,
                        "entry_price": float(cntr_uv) if cntr_uv else 0,
                        "entry_time": str(ord_tm) if ord_tm else "",
                    }
                else:
                    db_trade_counts[stk_cd]["count"] += 1

            # Merge DB trades with existing triggers (from JSON)
            db_added = 0
            for stk_cd, info in db_trade_counts.items():
                if stk_cd not in self.daily_triggers:
                    # New from DB
                    self.daily_triggers[stk_cd] = {
                        "entry_type": "db",
                        "entry_time": info["entry_time"],
                        "entry_price": info["entry_price"],
                        "trigger_count": info["count"],
                        "sold": stk_cd in sold_symbols,
                    }
                    db_added += 1
                else:
                    # Already exists (from JSON), update trigger_count to max
                    existing_count = self.daily_triggers[stk_cd].get("trigger_count", 1)
                    self.daily_triggers[stk_cd]["trigger_count"] = max(existing_count, info["count"])

            # Mark sold symbols
            for symbol in sold_symbols:
                if symbol in self.daily_triggers:
                    self.daily_triggers[symbol]["sold"] = True

        except Exception as e:
            print(f"[WARN] Failed to load trades from database: {e}")
            import traceback
            traceback.print_exc()

    def _log_triggers_if_changed(self, force: bool = False):
        """Log triggers only if changed since last log."""
        # Create hash of current state
        trigger_items = []
        for symbol, info in sorted(self.daily_triggers.items()):
            sold = "SOLD" if info.get("sold") else ""
            trigger_items.append(f"{symbol}{sold}")
        current_hash = ",".join(trigger_items)

        # Only log if changed or forced
        if force or current_hash != self._last_triggers_hash:
            self._last_triggers_hash = current_hash
            if self.daily_triggers:
                parts = []
                for symbol, info in sorted(self.daily_triggers.items()):
                    if info.get("sold"):
                        parts.append(f"{symbol}(SOLD)")
                    else:
                        parts.append(symbol)
                print(f"[TRIGGERS] Today's trades: {', '.join(parts)}")
            else:
                print("[TRIGGERS] No trades today")

    def _merge_api_buys_to_triggers(self):
        """
        Load today's buys from KIS API and merge into daily_triggers.
        Called on startup to ensure all trades are captured even if DB sync is delayed.
        """
        try:
            api_buys = self._load_today_api_buys()
            added = 0
            for symbol in api_buys:
                if symbol not in self.daily_triggers:
                    self.daily_triggers[symbol] = {
                        "entry_type": "api",
                        "entry_time": "",
                        "entry_price": 0,
                        "trigger_count": 1,
                    }
                    added += 1
            if added > 0:
                print(f"[API] Added {added} symbols from KIS API to triggers")
                self._log_triggers_if_changed(force=True)
        except Exception as e:
            print(f"[WARN] Failed to merge API buys: {e}")

    def _load_today_api_buys(self) -> set:
        """
        Load today's buy orders directly from KIS API.
        Most reliable source - doesn't depend on DB sync.
        Cached to avoid repeated API calls within same session.

        Note: Queries last 2 days to catch trades from overnight session
        (e.g., if it's Feb 5 01:00 ET, Feb 4 market trades have ord_dt=20260204)
        """
        if self._today_api_buys is not None:
            return self._today_api_buys

        from datetime import timedelta
        today_et = self._get_today_et()
        yesterday_et = today_et - timedelta(days=1)

        # Query last 2 days to catch overnight trades
        start_str = yesterday_et.strftime("%Y%m%d")
        end_str = today_et.strftime("%Y%m%d")
        bought_symbols = set()

        try:
            # sll_buy_dvsn: 02 = 매수만
            trades = self.client.get_trade_history(
                start_date=start_str,
                end_date=end_str,
                exchange_code="%",
                sll_buy_dvsn="02",  # 매수만
            )
            for trade in trades:
                symbol = trade.get("pdno", "").strip()
                if symbol:
                    bought_symbols.add(symbol)

            if bought_symbols:
                print(f"[API] Recent buys from KIS API ({start_str}~{end_str}): {sorted(bought_symbols)}")

            self._today_api_buys = bought_symbols
        except Exception as e:
            print(f"[WARN] Failed to get buys from API: {e}")
            self._today_api_buys = set()

        return self._today_api_buys

    def has_today_api_buy(self, symbol: str) -> bool:
        """
        Safety check: query KIS API directly for today's buy orders.
        Most reliable - doesn't depend on DB sync.
        """
        api_buys = self._load_today_api_buys()
        return symbol in api_buys

    def has_today_db_buy(self, symbol: str) -> bool:
        """
        Safety check: directly query DB to see if we already bought this symbol today.
        This prevents double-buying even if daily_triggers fails to load.
        """
        try:
            from db.connection import get_connection
            today_et = self._get_today_et()
            conn = get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM account_trade_history
                    WHERE stk_cd = %s AND trade_date = %s AND io_tp_nm LIKE '%%매수%%'
                """, (symbol, today_et))
                count = cur.fetchone()[0]
            conn.close()
            if count > 0:
                print(f"[SAFETY] {symbol} already bought today (DB: {count} trades on {today_et})")
            return count > 0
        except Exception as e:
            print(f"[SAFETY] DB check failed for {symbol}: {e}, checking daily_triggers")
            # On error, check daily_triggers as fallback
            has_trigger = symbol in self.daily_triggers
            if has_trigger:
                print(f"[SAFETY] {symbol} found in daily_triggers")
            return has_trigger

    def _save_daily_triggers(self):
        """Save daily triggers to file (US ET date)."""
        try:
            today_et = self._get_today_et()
            data = {
                "date": str(today_et),
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

    def _should_log_skip(self, symbol: str) -> bool:
        """
        Check if we should log skip message for this symbol.
        Returns True only once per symbol, then suppresses for 10 seconds.
        """
        import time
        now = time.time()

        # Clean up old entries (> 10 seconds)
        expired = [s for s, t in self._skipped_symbols.items() if now - t > 10]
        for s in expired:
            del self._skipped_symbols[s]

        # Check if already logged recently
        if symbol in self._skipped_symbols:
            return False

        # Mark as logged
        self._skipped_symbols[symbol] = now
        return True

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

        # === SAFETY CHECKS FIRST (before price check) ===

        # 1. Check daily_triggers (in-memory, fastest)
        today_triggers = self.daily_triggers.get(symbol, {}).get("trigger_count", 0)
        if today_triggers >= 1:
            if self._should_log_skip(symbol):
                print(f"[{symbol}] Skipping: already triggered today (trigger_count={today_triggers})")
            return False

        # 2. Check KIS API directly (most reliable, doesn't depend on DB sync)
        if self.has_today_api_buy(symbol):
            if self._should_log_skip(symbol):
                print(f"[{symbol}] Skipping: KIS API shows today's buy")
            return False

        # 3. Check DB as additional safety (might have trades from manual buys)
        if self.has_today_db_buy(symbol):
            if self._should_log_skip(symbol):
                print(f"[{symbol}] Skipping: DB shows today's buy")
            return False

        # === Now check price ===
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

        # === SAFETY CHECKS FIRST ===

        # 1. Check daily_triggers (in-memory)
        today_triggers = self.daily_triggers.get(symbol, {}).get("trigger_count", 0)
        if today_triggers >= 1:
            return False

        # 2. Check KIS API directly (most reliable)
        if self.has_today_api_buy(symbol):
            return False

        # 3. Check DB as additional safety
        if self.has_today_db_buy(symbol):
            return False

        # === Now check price ===
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

            # Clear API cache so next check sees the new trade
            self._today_api_buys = None

            # Quick sync to update holdings and lots after trade
            self._quick_sync_after_trade()
            return True

        return False

    def _quick_sync_after_trade(self):
        """
        Quick sync after a trade - runs in background thread to avoid blocking.
        Only syncs trade history (lightweight). Full sync happens via cron.
        """
        import threading

        def _sync_worker():
            try:
                from db.connection import get_connection
                from services.data_sync_service import sync_trade_history_from_kis

                conn = get_connection()
                try:
                    # Only sync trade history (lightweight, ~2-3 API calls)
                    trade_count = sync_trade_history_from_kis(conn)
                    print(f"[SYNC] Trade history synced: {trade_count}")
                finally:
                    conn.close()

                # Invalidate cache
                self._total_assets = 0

            except Exception as e:
                print(f"[WARN] Quick sync failed: {e}")

        # Run in background thread
        thread = threading.Thread(target=_sync_worker, daemon=True)
        thread.start()
        print("[SYNC] Started background sync...")

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

    def refresh_daily_triggers(self):
        """Refresh daily triggers from database. Call this periodically."""
        self._load_daily_triggers()

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

        # Refresh daily triggers from DB every cycle
        self._load_daily_triggers()

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
