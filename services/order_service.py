"""
Order service for automated trading.
Handles position sizing, order execution, and position tracking.
"""

import json
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Dict, List, Optional, Any

from services.kis_service import KISAPIClient
from services.trade_logger import trade_logger

# 포지션 상태 파일
POSITIONS_FILE = Path(__file__).resolve().parent.parent / ".positions.json"


class DefaultSettings:
    """Default trading settings."""
    UNIT: int = 1
    TICK_BUFFER: int = 3
    STOP_LOSS_PCT: float = 7.0
    UNIT_BASE_PERCENT: float = 5.0  # 1 unit = 5%

    def get_unit_percent(self) -> float:
        return self.UNIT * self.UNIT_BASE_PERCENT

    def get_half_unit_percent(self) -> float:
        return (self.UNIT / 2) * self.UNIT_BASE_PERCENT


class OrderService:
    """
    Manages order execution and position tracking.
    """

    def __init__(self, settings: Any = None):
        self.settings = settings or DefaultSettings()
        self.client = KISAPIClient()
        self.positions: Dict[str, dict] = {}
        self._load_positions()

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
        from datetime import date
        from db.connection import get_connection

        try:
            conn = get_connection()
            today = date.today()

            with conn.cursor() as cur:
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
                """, (today,))
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
            print(f"[SYNC] Loaded {synced} positions from holdings DB")
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
            Number of shares (rounded down)
        """
        half_unit_amount = self.calculate_half_unit_amount()
        shares = int(half_unit_amount / price)
        return max(shares, 0)

    def get_tick_size(self, price: float) -> float:
        """
        Get tick size for US stocks.
        US stocks trade in $0.01 increments.
        """
        return 0.01

    def add_tick_buffer(self, price: float) -> float:
        """Add tick buffer to price."""
        tick_size = self.get_tick_size(price)
        buffer = tick_size * self.settings.TICK_BUFFER
        return round(price + buffer, 2)

    def execute_buy(
        self,
        symbol: str,
        target_price: float,
        is_initial: bool = True,
        stop_loss_pct: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Execute buy order.

        Args:
            symbol: Stock symbol
            target_price: Target price (before tick buffer)
            is_initial: True for first buy (0.5 unit), False for pyramid (0.5 unit)
            stop_loss_pct: Custom stop loss %, or use default

        Returns:
            Order result or None if failed
        """
        # Calculate buy price with tick buffer
        buy_price = self.add_tick_buffer(target_price)

        # Calculate shares (half unit each time)
        shares = self.calculate_shares(buy_price)

        if shares <= 0:
            print(f"[{symbol}] Insufficient capital for buy order")
            return None

        reason = "initial_entry" if is_initial else "pyramid"
        trade_logger.log_order_attempt(symbol, "BUY", shares, buy_price, "LIMIT", reason)

        try:
            result = self.client.buy_order(symbol, shares, buy_price)

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
                # Averaging in
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
            trade_logger.log_order_result(
                symbol, "BUY", shares, buy_price,
                success=False, error=str(e)
            )
            return None

    def execute_sell(self, symbol: str, price: float, reason: str = "") -> Optional[dict]:
        """
        Execute sell order for entire position.

        Args:
            symbol: Stock symbol
            price: Sell price
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

        trade_logger.log_order_attempt(symbol, "SELL", quantity, price, "LIMIT", reason)

        try:
            result = self.client.sell_order(symbol, quantity, price)

            # Calculate P&L
            entry_price = pos.get("entry_price", price)
            pnl = (price - entry_price) * quantity
            pnl_pct = ((price / entry_price) - 1) * 100

            trade_logger.log_order_result(
                symbol, "SELL", quantity, price,
                success=True,
                order_no=result.get("order_no", ""),
                order_time=result.get("order_time", ""),
                message=result.get("message", ""),
            )

            # Close position
            pos["status"] = "closed"
            pos["exit_price"] = price
            pos["exit_time"] = datetime.now().isoformat()
            pos["exit_reason"] = reason
            pos["realized_pnl"] = pnl
            pos["realized_pnl_pct"] = pnl_pct

            trade_logger.log_position_update(symbol, "CLOSE", quantity, price, 0, pnl)

            self._save_positions()
            return result

        except Exception as e:
            trade_logger.log_order_result(
                symbol, "SELL", quantity, price,
                success=False, error=str(e)
            )
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
