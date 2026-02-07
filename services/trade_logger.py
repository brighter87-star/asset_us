"""
Trade logging service for order tracking and debugging.
Logs all orders, executions, and trading events to files.
Sends notifications to Telegram for important events.
"""

import json
import logging
import os
import requests
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from dotenv import load_dotenv

# Load .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

# Telegram settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Log directory
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


class TradeLogger:
    """
    Logs trading activity to files for later analysis.

    Log files:
    - trades_YYYYMMDD.log: Human-readable trade log
    - trades_YYYYMMDD.json: JSON format for DB comparison
    """

    def __init__(self):
        self._setup_logger()
        self._json_log_file = None
        self._current_date = None

    def _setup_logger(self):
        """Setup file and console logger."""
        self.logger = logging.getLogger("trade_logger")
        self.logger.setLevel(logging.DEBUG)

        # Clear existing handlers
        self.logger.handlers = []

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            "[%(asctime)s] %(message)s",
            datefmt="%H:%M:%S"
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

    def _get_file_handler(self) -> logging.FileHandler:
        """Get or create file handler for today's log."""
        today = datetime.now().strftime("%Y%m%d")

        if self._current_date != today:
            # Remove old file handler
            for handler in self.logger.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    self.logger.removeHandler(handler)
                    handler.close()

            # Create new file handler
            log_file = LOG_DIR / f"trades_{today}.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)

            # Update JSON log file path
            self._json_log_file = LOG_DIR / f"trades_{today}.json"
            self._current_date = today

        return None

    def _write_json_log(self, record: Dict[str, Any]):
        """Append record to JSON log file."""
        self._get_file_handler()  # Ensure correct date

        try:
            # Read existing records
            records = []
            if self._json_log_file.exists():
                with open(self._json_log_file, "r", encoding="utf-8") as f:
                    try:
                        records = json.load(f)
                    except json.JSONDecodeError:
                        records = []

            # Append new record
            records.append(record)

            # Write back
            with open(self._json_log_file, "w", encoding="utf-8") as f:
                json.dump(records, f, indent=2, ensure_ascii=False, default=str)

        except Exception as e:
            self.logger.error(f"Failed to write JSON log: {e}")

    def _send_telegram(self, message: str, parse_mode: str = "HTML"):
        """Send message to Telegram."""
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        try:
            response = requests.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": parse_mode,
                },
                timeout=10,
            )
            if not response.json().get("ok"):
                self.logger.warning(f"Telegram send failed: {response.json()}")
        except Exception as e:
            self.logger.warning(f"Telegram error: {e}")

    def log_order_attempt(
        self,
        symbol: str,
        side: str,  # "BUY" or "SELL"
        quantity: int,
        price: float,
        order_type: str = "LIMIT",
        reason: str = "",
    ):
        """Log order attempt before sending to API."""
        self._get_file_handler()

        msg = f"ORDER_ATTEMPT | {side} {symbol} | qty={quantity} @ ${price:.2f} | type={order_type}"
        if reason:
            msg += f" | reason={reason}"

        self.logger.info(msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "ORDER_ATTEMPT",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "order_type": order_type,
            "reason": reason,
        })

    def log_order_result(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        success: bool,
        order_no: str = "",
        order_time: str = "",
        message: str = "",
        error: str = "",
    ):
        """Log order result after API response."""
        self._get_file_handler()

        if success:
            msg = f"ORDER_ACCEPTED | {side} {symbol} | qty={quantity} @ ${price:.2f} | order_no={order_no} | time={order_time}"
            self.logger.info(msg)

            # Telegram notification - 주문 접수 (not 체결)
            emoji = "\U0001F4DD"  # 메모 이모지
            tg_msg = (
                f"{emoji} <b>주문 접수</b>\n"
                f"종목: <code>{symbol}</code>\n"
                f"구분: {side}\n"
                f"수량: {quantity}주\n"
                f"가격: ${price:.2f}\n"
                f"주문번호: {order_no}"
            )
            self._send_telegram(tg_msg)
        else:
            msg = f"ORDER_FAILED | {side} {symbol} | qty={quantity} @ ${price:.2f} | error={error}"
            self.logger.error(msg)

            # Telegram notification
            tg_msg = (
                f"\u274C <b>주문실패</b>\n"
                f"종목: <code>{symbol}</code>\n"
                f"구분: {side}\n"
                f"수량: {quantity}주\n"
                f"가격: ${price:.2f}\n"
                f"오류: {error}"
            )
            self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "ORDER_RESULT",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "success": success,
            "order_no": order_no,
            "order_time": order_time,
            "message": message,
            "error": error,
        })

    def log_order_filled(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        order_no: str = "",
        fill_time: str = "",
    ):
        """Log actual order fill (execution)."""
        self._get_file_handler()

        msg = f"ORDER_FILLED | {side} {symbol} | qty={quantity} @ ${price:.2f} | order_no={order_no} | time={fill_time}"
        self.logger.info(msg)

        # Telegram notification - 체결
        emoji = "\u2705" if side == "BUY" else "\U0001F4B0"
        tg_msg = (
            f"{emoji} <b>주문 체결</b>\n"
            f"종목: <code>{symbol}</code>\n"
            f"구분: {side}\n"
            f"수량: {quantity}주\n"
            f"체결가: ${price:.2f}\n"
            f"주문번호: {order_no}"
        )
        self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "ORDER_FILLED",
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "order_no": order_no,
            "fill_time": fill_time,
        })

    def log_price_check(
        self,
        symbol: str,
        current_price: float,
        target_price: float,
        trigger_price: float,
        triggered: bool,
    ):
        """Log price check for breakout detection."""
        self._get_file_handler()

        status = "TRIGGERED" if triggered else "watching"
        msg = f"PRICE_CHECK | {symbol} | current=${current_price:.2f} | target=${target_price:.2f} | trigger=${trigger_price:.2f} | {status}"

        if triggered:
            self.logger.info(msg)
        else:
            self.logger.debug(msg)

        if triggered:
            self._write_json_log({
                "timestamp": datetime.now().isoformat(),
                "event": "PRICE_TRIGGER",
                "symbol": symbol,
                "current_price": current_price,
                "target_price": target_price,
                "trigger_price": trigger_price,
            })

    def log_stop_loss(
        self,
        symbol: str,
        entry_price: float,
        current_price: float,
        stop_loss_pct: float,
        change_pct: float,
    ):
        """Log stop loss trigger."""
        self._get_file_handler()

        msg = f"STOP_LOSS | {symbol} | entry=${entry_price:.2f} | current=${current_price:.2f} | change={change_pct:+.2f}% | threshold=-{stop_loss_pct}%"
        self.logger.warning(msg)

        # Telegram notification
        tg_msg = (
            f"\U0001F6A8 <b>손절 발동</b>\n"
            f"종목: <code>{symbol}</code>\n"
            f"진입가: ${entry_price:.2f}\n"
            f"현재가: ${current_price:.2f}\n"
            f"손실률: {change_pct:+.2f}%\n"
            f"손절기준: -{stop_loss_pct}%"
        )
        self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "STOP_LOSS_TRIGGER",
            "symbol": symbol,
            "entry_price": entry_price,
            "current_price": current_price,
            "stop_loss_pct": stop_loss_pct,
            "change_pct": change_pct,
        })

    def log_close_action(
        self,
        symbol: str,
        action: str,  # "PYRAMID" or "SELL"
        entry_price: float,
        close_price: float,
        change_pct: float,
        today_volume: int = 0,
        avg_volume: float = 0,
        volume_ratio: float = 0,
        volume_threshold: float = 0,
        reason: str = "",
    ):
        """Log end-of-day action with volume details."""
        self._get_file_handler()

        msg = f"CLOSE_ACTION | {symbol} | action={action} | entry=${entry_price:.2f} | close=${close_price:.2f} | change={change_pct:+.2f}%"
        if avg_volume > 0:
            msg += f" | vol={today_volume:,} / avg={avg_volume:,.0f} = {volume_ratio:.1f}x (need {volume_threshold}x)"
        self.logger.info(msg)

        # Telegram notification
        price_ok = close_price > entry_price
        volume_ok = volume_ratio >= volume_threshold if avg_volume > 0 else False

        price_icon = "\u2705" if price_ok else "\u274C"
        volume_icon = "\u2705" if volume_ok else "\u274C"

        if action == "PYRAMID":
            header = "\U0001F4C8 <b>장마감 추가매수</b>"
        elif reason == "close_weak_volume":
            header = "\U0001F4CA <b>장마감 매도 (거래량 부족)</b>"
        else:
            header = "\U0001F4C9 <b>장마감 매도 (손실)</b>"

        vol_line = ""
        if avg_volume > 0:
            vol_line = (
                f"\n\n<b>조건 충족 여부:</b>\n"
                f"{price_icon} 수익: ${close_price:.2f} vs ${entry_price:.2f} ({change_pct:+.2f}%)\n"
                f"{volume_icon} 거래량: {today_volume:,} / 평균 {avg_volume:,.0f} = {volume_ratio:.1f}x (기준 {volume_threshold}x)"
            )
        else:
            vol_line = (
                f"\n\n<b>조건 충족 여부:</b>\n"
                f"{price_icon} 수익: ${close_price:.2f} vs ${entry_price:.2f} ({change_pct:+.2f}%)\n"
                f"\u274C 거래량: 평균 데이터 없음"
            )

        tg_msg = (
            f"{header}\n"
            f"종목: <code>{symbol}</code>"
            f"{vol_line}"
        )
        self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "CLOSE_ACTION",
            "symbol": symbol,
            "action": action,
            "entry_price": entry_price,
            "close_price": close_price,
            "change_pct": change_pct,
            "today_volume": today_volume,
            "avg_volume": avg_volume,
            "volume_ratio": volume_ratio,
            "volume_threshold": volume_threshold,
            "reason": reason,
        })

    def log_position_update(
        self,
        symbol: str,
        action: str,  # "OPEN", "ADD", "CLOSE"
        quantity: int,
        avg_price: float,
        total_quantity: int,
        realized_pnl: Optional[float] = None,
    ):
        """Log position changes."""
        self._get_file_handler()

        msg = f"POSITION | {symbol} | {action} | qty={quantity} @ ${avg_price:.2f} | total_qty={total_quantity}"
        if realized_pnl is not None:
            msg += f" | realized_pnl=${realized_pnl:.2f}"

        self.logger.info(msg)

        # Telegram notification for OPEN and CLOSE
        if action == "OPEN":
            tg_msg = (
                f"\U0001F4C8 <b>포지션 오픈</b>\n"
                f"종목: <code>{symbol}</code>\n"
                f"수량: {quantity}주\n"
                f"평균가: ${avg_price:.2f}"
            )
            self._send_telegram(tg_msg)
        elif action == "CLOSE":
            pnl_emoji = "\U0001F4B0" if realized_pnl and realized_pnl > 0 else "\U0001F4C9"
            tg_msg = (
                f"{pnl_emoji} <b>포지션 청산</b>\n"
                f"종목: <code>{symbol}</code>\n"
                f"수량: {quantity}주\n"
                f"청산가: ${avg_price:.2f}\n"
                f"실현손익: ${realized_pnl:.2f}" if realized_pnl else ""
            )
            self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "POSITION_UPDATE",
            "symbol": symbol,
            "action": action,
            "quantity": quantity,
            "avg_price": avg_price,
            "total_quantity": total_quantity,
            "realized_pnl": realized_pnl,
        })

    def log_settings_change(self, settings: Dict[str, Any]):
        """Log settings reload."""
        self._get_file_handler()

        msg = f"SETTINGS | {settings}"
        self.logger.info(msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "SETTINGS_RELOAD",
            **settings,
        })

    def log_system_event(self, event: str, details: str = ""):
        """Log system events (start, stop, errors)."""
        self._get_file_handler()

        msg = f"SYSTEM | {event}"
        if details:
            msg += f" | {details}"

        self.logger.info(msg)

        # Telegram notification for START and STOP
        if event == "START":
            tg_msg = f"\U0001F7E2 <b>자동매매 시작</b>\n{details}"
            self._send_telegram(tg_msg)
        elif event == "STOP":
            tg_msg = f"\U0001F534 <b>자동매매 종료</b>\n{details}"
            self._send_telegram(tg_msg)

        self._write_json_log({
            "timestamp": datetime.now().isoformat(),
            "event": "SYSTEM",
            "type": event,
            "details": details,
        })


# Global logger instance
trade_logger = TradeLogger()


def test_telegram():
    """Test Telegram connection."""
    if not TELEGRAM_BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN not set in .env")
        return False

    if not TELEGRAM_CHAT_ID:
        print("[ERROR] TELEGRAM_CHAT_ID not set in .env")
        print("Run: python scripts/get_telegram_chat_id.py")
        return False

    print(f"Bot Token: {TELEGRAM_BOT_TOKEN[:20]}...")
    print(f"Chat ID: {TELEGRAM_CHAT_ID}")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": "\U0001F916 <b>테스트 메시지</b>\n자동매매 시스템 Telegram 연동 테스트입니다.",
                "parse_mode": "HTML",
            },
            timeout=10,
        )

        result = response.json()
        if result.get("ok"):
            print("[SUCCESS] Telegram message sent!")
            return True
        else:
            print(f"[ERROR] {result}")
            return False

    except Exception as e:
        print(f"[ERROR] {e}")
        return False


if __name__ == "__main__":
    test_telegram()
