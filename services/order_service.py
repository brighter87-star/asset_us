"""
Order service for automated trading.
Handles position sizing, order execution, and position tracking.
"""

import json
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

from services.kis_service import KISAPIClient
from services.trade_logger import trade_logger

# US Eastern timezone
ET = ZoneInfo("America/New_York")


def get_trading_date_et() -> date:
    """
    Get the current US trading date (ET timezone).
    - Before 8 PM ET: current date
    - After 8 PM ET: next date (today's session ended)
    """
    now_et = datetime.now(ET)
    if now_et.hour >= 20:
        return (now_et + timedelta(days=1)).date()
    return now_et.date()

# 포지션 상태 파일
POSITIONS_FILE = Path(__file__).resolve().parent.parent / ".positions.json"


class DefaultSettings:
    """Default trading settings."""
    UNIT: float = 1.0
    STOP_LOSS_PCT: float = 7.0
    PRICE_BUFFER_PCT: float = 0.5  # 주문가 버퍼 %
    UNIT_BASE_PERCENT: float = 5.0  # 1 unit = 5%

    def get_unit_percent(self) -> float:
        return self.UNIT * self.UNIT_BASE_PERCENT

    def get_half_unit_percent(self) -> float:
        return (self.UNIT / 2) * self.UNIT_BASE_PERCENT


class OrderService:
    """
    Manages order execution and position tracking.
    """

    # Price API -> Order API exchange code mapping
    EXCHANGE_MAP = {
        "NAS": "NASD",
        "NYS": "NYSE",
        "AMS": "AMEX",
    }

    def __init__(self, settings: Any = None):
        self.settings = settings or DefaultSettings()
        self.client = KISAPIClient()
        self.positions: Dict[str, dict] = {}
        self._exchange_cache: Dict[str, str] = {}  # symbol -> order exchange code
        self._load_positions()

    def _detect_exchange(self, symbol: str) -> str:
        """Detect the correct exchange for a symbol."""
        # Check cache first
        if symbol in self._exchange_cache:
            cached = self._exchange_cache[symbol]
            print(f"[{symbol}] Using cached exchange: {cached}")
            return cached

        # Try to get price from each exchange to detect correct one
        exchanges_to_try = ["NAS", "NYS", "AMS"]
        for price_exchange in exchanges_to_try:
            try:
                result = self.client.get_current_price(symbol, price_exchange)
                if result and result.get("last", 0) > 0:
                    order_exchange = self.EXCHANGE_MAP.get(price_exchange, "NASD")
                    self._exchange_cache[symbol] = order_exchange
                    print(f"[{symbol}] Detected exchange: {price_exchange} -> {order_exchange}")
                    return order_exchange
            except Exception:
                continue

        # Default to NASD if detection fails
        print(f"[{symbol}] Exchange detection failed, defaulting to NASD")
        return "NASD"

    def _load_positions(self):
        """Load positions from file."""
        try:
            if POSITIONS_FILE.exists():
                with open(POSITIONS_FILE, "r") as f:
                    self.positions = json.load(f)
        except Exception:
            self.positions = {}

    def _save_positions(self):
        """Save positions to file."""
        try:
            with open(POSITIONS_FILE, "w") as f:
                json.dump(self.positions, f, indent=2, default=str)
        except Exception as e:
            print(f"[ERROR] Failed to save positions: {e}")

    def sync_positions_from_db(self, stop_loss_pct: float = 7.0):
        """
        holdings 테이블에서 보유종목을 로드하여 positions에 동기화.

        Args:
            stop_loss_pct: 기본 손절률 (%)

        Returns:
            synced count
        """
        from db.connection import get_connection

        try:
            conn = get_connection()

            # Use US ET date for consistency with trading schedule
            trading_date = get_trading_date_et()

            with conn.cursor() as cur:
                # First try today's ET date, then fall back to most recent date
                cur.execute("""
                    SELECT
                        stk_cd as stock_code,
                        stk_nm as stock_name,
                        crd_class,
                        SUM(rmnd_qty) as total_qty,
                        SUM(rmnd_qty * avg_prc) / SUM(rmnd_qty) as avg_price,
                        SUM(rmnd_qty * avg_prc) as total_cost,
                        MAX(cur_prc) as current_price,
                        MAX(exchange_code) as exchange_code,
                        MAX(currency) as currency
                    FROM holdings
                    WHERE snapshot_date = %s AND rmnd_qty > 0
                    GROUP BY stk_cd, crd_class
                """, (trading_date,))
                rows = cur.fetchall()

                # If no data for today's ET date, try most recent snapshot
                if not rows:
                    cur.execute("SELECT MAX(snapshot_date) FROM holdings WHERE rmnd_qty > 0")
                    result = cur.fetchone()
                    fallback_date = result[0] if result and result[0] else None

                    if fallback_date:
                        print(f"[WARN] No holdings for {trading_date}, using {fallback_date}")
                        cur.execute("""
                            SELECT
                                stk_cd as stock_code,
                                stk_nm as stock_name,
                                crd_class,
                                SUM(rmnd_qty) as total_qty,
                                SUM(rmnd_qty * avg_prc) / SUM(rmnd_qty) as avg_price,
                                SUM(rmnd_qty * avg_prc) as total_cost,
                                MAX(cur_prc) as current_price,
                                MAX(exchange_code) as exchange_code,
                                MAX(currency) as currency
                            FROM holdings
                            WHERE snapshot_date = %s AND rmnd_qty > 0
                            GROUP BY stk_cd, crd_class
                        """, (fallback_date,))
                        rows = cur.fetchall()

            conn.close()

            # 기존 positions 초기화 (DB 기준으로 새로 로드)
            self.positions = {}

            synced = 0
            for row in rows:
                stock_code, stock_name, crd_class, total_qty, avg_price, total_cost, current_price, exchange_code, currency = row

                if not stock_code or not total_qty or total_qty <= 0:
                    continue

                avg_price = float(avg_price or 0)
                current_price = float(current_price or 0)
                total_qty = int(total_qty)
                total_cost = float(total_cost or 0)

                if avg_price <= 0:
                    print(f"[WARN] {stock_code}: avg_price=0, skipping")
                    continue

                stop_loss_price = avg_price * (1 - stop_loss_pct / 100)

                self.positions[stock_code] = {
                    "symbol": stock_code,
                    "name": stock_name or "",
                    "quantity": total_qty,
                    "entry_price": avg_price,
                    "stop_loss_price": stop_loss_price,
                    "stop_loss_pct": stop_loss_pct,
                    "status": "open",
                    "crd_class": crd_class or "CASH",
                    "total_cost": total_cost,
                    "current_price": current_price,
                    "exchange_code": exchange_code or "NASD",
                    "currency": currency or "USD",
                    "source": "holdings",
                }
                synced += 1

            self._save_positions()
            return synced

        except Exception as e:
            print(f"[ERROR] Failed to sync from DB: {e}")
            return 0

    def get_available_capital(self) -> float:
        """Get available USD capital for trading."""
        try:
            power = self.client.get_buying_power("NASD")
            return power["available_amt"]
        except Exception as e:
            print(f"[ERROR] Failed to get buying power: {e}")
            return 0.0

    def calculate_half_unit_amount(self) -> float:
        """
        Calculate half-unit amount for each buy.
        Each buy uses (UNIT / 2) * 5% of total capital.
        """
        available = self.get_available_capital()

        # Estimate total capital (available + positions value)
        positions_value = sum(
            pos.get("quantity", 0) * pos.get("entry_price", 0)
            for pos in self.positions.values()
            if pos.get("status") == "open"
        )
        total_capital = available + positions_value

        half_unit_pct = self.settings.get_half_unit_percent() / 100
        return total_capital * half_unit_pct

    def calculate_shares(self, price: float) -> int:
        """
        Calculate number of shares to buy for one buy (half unit).

        Args:
            price: Stock price

        Returns:
            Number of shares (at least 1, rounded to nearest)
        """
        half_unit_amount = self.calculate_half_unit_amount()
        shares = round(half_unit_amount / price)  # 반올림
        return max(shares, 1)  # 최소 1주

    def add_price_buffer(self, price: float, buffer_pct: float = None) -> float:
        """
        Add percentage buffer to price for buy orders.

        Args:
            price: Current price
            buffer_pct: Buffer percentage (uses settings.PRICE_BUFFER_PCT if None)

        Returns:
            Price with buffer added, rounded to 2 decimals
        """
        if buffer_pct is None:
            buffer_pct = getattr(self.settings, 'PRICE_BUFFER_PCT', 0.5)
        buffered_price = price * (1 + buffer_pct / 100)
        return round(buffered_price, 2)

    def subtract_price_buffer(self, price: float, buffer_pct: float = None) -> float:
        """
        Subtract percentage buffer from price for sell orders.

        Args:
            price: Current price
            buffer_pct: Buffer percentage (uses settings.PRICE_BUFFER_PCT if None)

        Returns:
            Price with buffer subtracted, rounded to 2 decimals
        """
        if buffer_pct is None:
            buffer_pct = getattr(self.settings, 'PRICE_BUFFER_PCT', 0.5)
        buffered_price = price * (1 - buffer_pct / 100)
        return round(buffered_price, 2)

    def execute_buy(
        self,
        symbol: str,
        target_price: float,
        is_initial: bool = True,
        stop_loss_pct: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Execute buy order with retry logic.

        Args:
            symbol: Stock symbol
            target_price: Target price (before tick buffer)
            is_initial: True for first buy (0.5 unit), False for pyramid (0.5 unit)
            stop_loss_pct: Custom stop loss %, or use default

        Returns:
            Order result or None if failed
        """
        reason = "initial_entry" if is_initial else "pyramid"

        # Fixed 0.5% buffer on real-time price
        buffer_pct = 0.5

        # Fetch real-time current price from API (not cached price)
        try:
            exchange_code = self._detect_exchange(symbol)
            real_time_price = self.client.get_current_price(symbol, exchange_code)
            current_price = real_time_price.get("last", target_price)
            print(f"[{symbol}] Real-time price: ${current_price:.2f}")
        except Exception as e:
            print(f"[{symbol}] Failed to get real-time price, using cached: {e}")
            current_price = target_price

        # Apply buffer to real-time current price
        buy_price = self.add_price_buffer(current_price, buffer_pct)
        shares = self.calculate_shares(buy_price)

        if shares <= 0:
            print(f"[{symbol}] Insufficient capital for buy order")
            return None

        print(f"[{symbol}] BUY: {shares} shares @ ${buy_price:.2f} (+{buffer_pct}%)")
        trade_logger.log_order_attempt(symbol, "BUY", shares, buy_price, "LIMIT", f"{reason}_+{buffer_pct}%")

        try:
            result = self.client.buy_order(symbol, shares, buy_price, exchange_code=exchange_code)

            trade_logger.log_order_result(
                symbol, "BUY", shares, buy_price,
                success=True,
                order_no=result.get("order_no", ""),
                order_time=result.get("order_time", ""),
                message=result.get("message", ""),
            )

            # Update position tracking
            if symbol not in self.positions:
                self.positions[symbol] = {
                    "symbol": symbol,
                    "status": "open",
                    "entry_price": buy_price,
                    "quantity": shares,
                    "stop_loss_pct": stop_loss_pct or self.settings.STOP_LOSS_PCT,
                    "entry_time": datetime.now().isoformat(),
                    "buy_count": 1,
                }
                trade_logger.log_position_update(symbol, "OPEN", shares, buy_price, shares)
            else:
                pos = self.positions[symbol]
                old_qty = pos.get("quantity", 0)
                old_price = pos.get("entry_price", buy_price)
                new_qty = old_qty + shares
                new_avg_price = (old_price * old_qty + buy_price * shares) / new_qty

                pos["quantity"] = new_qty
                pos["entry_price"] = new_avg_price
                pos["buy_count"] = pos.get("buy_count", 1) + 1
                pos["last_buy_time"] = datetime.now().isoformat()

                trade_logger.log_position_update(symbol, "ADD", shares, buy_price, new_qty)

            self._save_positions()
            return result

        except Exception as e:
            error_msg = str(e)
            trade_logger.log_order_result(
                symbol, "BUY", shares, buy_price,
                success=False, error=error_msg
            )
            print(f"[{symbol}] BUY failed: {error_msg}")
            return None

    def execute_sell(self, symbol: str, price: float, reason: str = "") -> Optional[dict]:
        """
        Execute sell order for entire position with retry logic.

        Args:
            symbol: Stock symbol
            price: Current price (buffer will be subtracted)
            reason: Reason for selling (for logging)

        Returns:
            Order result or None if failed
        """
        if symbol not in self.positions:
            print(f"[{symbol}] No position to sell")
            return None

        pos = self.positions[symbol]
        quantity = pos.get("quantity", 0)

        if quantity <= 0:
            print(f"[{symbol}] No shares to sell")
            return None

        # Check actual sellable quantity from API
        try:
            sellable_qty = self.client.get_sellable_quantity(symbol)
            if sellable_qty <= 0:
                print(f"[{symbol}] No sellable shares in account (API returned 0)")
                return None
            if sellable_qty < quantity:
                print(f"[{symbol}] Adjusting quantity: {quantity} -> {sellable_qty} (API available)")
                quantity = sellable_qty
        except Exception as e:
            print(f"[{symbol}] Could not verify sellable qty, using position qty: {e}")

        # Fixed 0.5% buffer on real-time price
        buffer_pct = 0.5

        # Fetch real-time current price from API (not cached price)
        try:
            exchange_code = self._detect_exchange(symbol)
            real_time_price = self.client.get_current_price(symbol, exchange_code)
            current_price = real_time_price.get("last", price)
            print(f"[{symbol}] Real-time price: ${current_price:.2f}")
        except Exception as e:
            print(f"[{symbol}] Failed to get real-time price, using passed: {e}")
            current_price = price

        sell_price = self.subtract_price_buffer(current_price, buffer_pct)

        print(f"[{symbol}] SELL: {quantity} shares @ ${sell_price:.2f} (-{buffer_pct}%)")
        trade_logger.log_order_attempt(symbol, "SELL", quantity, sell_price, "LIMIT", f"{reason}_-{buffer_pct}%")

        try:
            result = self.client.sell_order(symbol, quantity, sell_price, exchange_code=exchange_code)

            # Calculate P&L
            entry_price = pos.get("entry_price", price)
            pnl = (sell_price - entry_price) * quantity
            pnl_pct = ((sell_price / entry_price) - 1) * 100

            trade_logger.log_order_result(
                symbol, "SELL", quantity, sell_price,
                success=True,
                order_no=result.get("order_no", ""),
                order_time=result.get("order_time", ""),
                message=result.get("message", ""),
            )

            # Close position
            pos["status"] = "closed"
            pos["exit_price"] = sell_price
            pos["exit_time"] = datetime.now().isoformat()
            pos["exit_reason"] = reason
            pos["realized_pnl"] = pnl
            pos["realized_pnl_pct"] = pnl_pct

            trade_logger.log_position_update(symbol, "CLOSE", quantity, sell_price, 0, pnl)

            self._save_positions()
            return result

        except Exception as e:
            error_msg = str(e)
            trade_logger.log_order_result(
                symbol, "SELL", quantity, sell_price,
                success=False, error=error_msg
            )
            print(f"[{symbol}] SELL failed: {error_msg}")
            return None

    def check_stop_loss(self, symbol: str, current_price: float) -> bool:
        """
        Check if stop loss is triggered.

        Returns:
            True if stop loss triggered
        """
        if symbol not in self.positions:
            return False

        pos = self.positions[symbol]
        if pos.get("status") != "open":
            return False

        entry_price = pos.get("entry_price", 0)
        stop_loss_pct = pos.get("stop_loss_pct", self.settings.STOP_LOSS_PCT)

        if entry_price <= 0:
            return False

        change_pct = ((current_price / entry_price) - 1) * 100

        if change_pct <= -stop_loss_pct:
            trade_logger.log_stop_loss(symbol, entry_price, current_price, stop_loss_pct, change_pct)
            return True

        return False

    def get_position(self, symbol: str) -> Optional[dict]:
        """Get position for symbol."""
        return self.positions.get(symbol)

    def get_open_positions(self) -> List[dict]:
        """Get all open positions."""
        return [
            pos for pos in self.positions.values()
            if pos.get("status") == "open"
        ]

    def has_position(self, symbol: str) -> bool:
        """Check if we have an open position."""
        pos = self.positions.get(symbol)
        return pos is not None and pos.get("status") == "open"

    def clear_closed_positions(self):
        """Remove closed positions from tracking."""
        self.positions = {
            symbol: pos
            for symbol, pos in self.positions.items()
            if pos.get("status") == "open"
        }
        self._save_positions()
