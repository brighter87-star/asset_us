"""
Price service for US stocks using Korea Investment & Securities API.
REST API polling for real-time prices.
"""

import threading
import time
from datetime import datetime, time as dt_time
from typing import Dict, List, Optional, Callable
from zoneinfo import ZoneInfo

from services.kis_service import KISAPIClient

ET = ZoneInfo("America/New_York")
KST = ZoneInfo("Asia/Seoul")

# Exchange code mapping (watchlist symbol -> API exchange code)
EXCHANGE_MAP = {
    "NASD": "NAS",
    "NYSE": "NYS",
    "AMEX": "AMS",
    "NAS": "NAS",
    "NYS": "NYS",
    "AMS": "AMS",
}


class RestPricePoller:
    """
    REST API price poller for US stocks.
    Polls prices at regular intervals.
    """

    def __init__(self, interval: float = 2.0):
        """
        Args:
            interval: Polling interval in seconds (default 2s)
        """
        self.client = KISAPIClient()
        self.interval = interval

        self.subscribed_stocks: List[str] = []
        self.stock_exchanges: Dict[str, str] = {}  # symbol -> exchange_code

        self.prices: Dict[str, dict] = {}
        self.prices_lock = threading.Lock()

        self.running = False
        self.poll_thread: Optional[threading.Thread] = None

        self.on_price_update: Optional[Callable] = None

    def subscribe(self, symbols: List[str], exchange_code: str = "NAS"):
        """Subscribe to price updates for symbols."""
        for symbol in symbols:
            if symbol not in self.subscribed_stocks:
                self.subscribed_stocks.append(symbol)
                self.stock_exchanges[symbol] = exchange_code

    def set_exchange(self, symbol: str, exchange_code: str):
        """Set exchange code for a symbol."""
        api_exchange = EXCHANGE_MAP.get(exchange_code, exchange_code)
        self.stock_exchanges[symbol] = api_exchange

    def start(self):
        """Start polling."""
        if self.running:
            return

        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.poll_thread.start()
        print(f"[PRICE] Started REST poller ({self.interval}s interval)")

    def stop(self):
        """Stop polling."""
        self.running = False
        if self.poll_thread:
            self.poll_thread.join(timeout=5)
            self.poll_thread = None
        print("[PRICE] Stopped REST poller")

    def _poll_loop(self):
        """Main polling loop."""
        while self.running:
            try:
                self._poll_all_prices()
            except Exception as e:
                print(f"[PRICE] Poll error: {e}")

            time.sleep(self.interval)

    def _is_daytime_trading_hours(self) -> bool:
        """
        Check if current time is US daytime trading hours (Korean 10:00~16:00).
        During this time, we need to use BAQ/BAY/BAA exchange codes.
        """
        now_kst = datetime.now(KST)
        daytime_start = dt_time(10, 0)
        daytime_end = dt_time(16, 0)
        return daytime_start <= now_kst.time() <= daytime_end

    def _poll_all_prices(self):
        """Poll prices for all subscribed stocks."""
        # Exchange codes to try
        # During Korean daytime hours (10:00~16:00), try daytime codes first
        if self._is_daytime_trading_hours():
            # Daytime trading codes (BAQ=NASDAQ, BAY=NYSE, BAA=AMEX)
            EXCHANGE_FALLBACK = ["BAQ", "BAY", "BAA", "NAS", "NYS", "AMS"]
        else:
            # Regular/pre-market/after-market codes
            EXCHANGE_FALLBACK = ["NAS", "NYS", "AMS"]

        for symbol in self.subscribed_stocks:
            if not self.running:
                break

            # Determine which exchanges to try
            preferred = self.stock_exchanges.get(symbol, "NAS")
            exchanges_to_try = [preferred] + [e for e in EXCHANGE_FALLBACK if e != preferred]

            price_data = None
            for exchange_code in exchanges_to_try:
                try:
                    price_data = self.client.get_current_price(symbol, exchange_code)
                    if price_data and price_data.get("last", 0) > 0:
                        # Remember working exchange for next time
                        self.stock_exchanges[symbol] = exchange_code
                        break
                except Exception:
                    continue

            if price_data and price_data.get("last", 0) > 0:
                with self.prices_lock:
                    self.prices[symbol] = {
                        "last": price_data.get("last", 0),
                        "open": price_data.get("open", 0),
                        "high": price_data.get("high", 0),
                        "low": price_data.get("low", 0),
                        "base": price_data.get("base", 0),
                        "change": price_data.get("diff", 0),
                        "change_pct": price_data.get("rate", 0),
                        "volume": price_data.get("volume", 0),
                        "timestamp": datetime.now(ET).isoformat(),
                    }

                if self.on_price_update:
                    self.on_price_update(symbol, self.prices[symbol])

            # Rate limit between API calls
            time.sleep(0.3)

    def get_price(self, symbol: str) -> Optional[dict]:
        """Get cached price for symbol."""
        with self.prices_lock:
            return self.prices.get(symbol)

    def get_prices(self) -> Dict[str, dict]:
        """Get all cached prices."""
        with self.prices_lock:
            return self.prices.copy()

    def is_connected(self) -> bool:
        """Check if poller is running."""
        return self.running
