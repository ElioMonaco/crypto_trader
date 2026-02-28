"""
Crypto.com Exchange Spot Trading Bot (BTC_USD) - 15m Candle Range Strategy
========================================================================

This script demonstrates an OBJECT-ORIENTED trading bot that:

1) Subscribes to Crypto.com Exchange market WebSocket candlesticks for:
      candlestick.15m.BTC_USD

2) Runs a simple "candle range theory" strategy:
   - Find a "setup" candle when volatility contracts AND the candle is an "inside candle"
   - Enter on an "expansion breakout" candle that closes above/below the setup range

3) Places spot MARKET orders via REST:
      private/create-order

4) Logs every order attempt + response (or error) into PostgreSQL.

--------------------------------------------------------------------------------
IMPORTANT DISCLAIMERS
--------------------------------------------------------------------------------
- This is a template / educational skeleton. It is NOT production-grade.
- It uses fixed quantity by default. Real bots should size positions based on
  risk, available balances, minimum order sizes, and step sizes.
- `in_position` is a placeholder state. A real spot bot should derive position
  state from your balances (e.g., BTC balance) and open orders.
- Always test on sandbox / with small size and confirm symbol naming and
  quantity meaning for your pair and account type.

Dependencies:
    pip install websockets requests psycopg2-binary

Python version:
    3.10+ recommended

--------------------------------------------------------------------------------
HOW THE WEBSOCKET "CLOSED CANDLE" DETECTION WORKS
--------------------------------------------------------------------------------
Crypto.com WS candlestick stream often sends updates for the currently forming
candle. We need to avoid acting on partial candles.

This code uses a simple approach:
- We track the candle start timestamp `t`.
- When we see a NEW `t` different from last `t`, we treat it as a "new candle"
  message, and we accept it as the next candle in the series.
- This works as a proxy for "closed candles" because the prior timestamp candle
  won't update anymore once a new one begins.
If your WS payload contains an explicit "isFinal/confirmed" flag, that is even
better; you can switch to using that.

--------------------------------------------------------------------------------
POSTGRES LOGGING
--------------------------------------------------------------------------------
A trades table is created automatically if it does not exist. Each row stores:
- timestamp
- instrument
- side
- quantity
- signal info
- candle context (time & close)
- setup levels (high/low)
- full request JSON
- full response JSON
- ok / error text

This helps you debug and backtest behavior later.

"""

import asyncio
import json
import time
import hmac
import hashlib
from dataclasses import dataclass
from collections import deque
from typing import Deque, Optional, Dict, Any, Tuple

import requests
import websockets
import psycopg2
from psycopg2.extras import Json


# =============================================================================
# CONFIGURATION SECTION
# =============================================================================

# Crypto.com Exchange market WebSocket URL.
# This is the *market* channel endpoint used for public market data subscriptions.
MARKET_WS_URL = "wss://stream.crypto.com/exchange/v1/market"

# Crypto.com Exchange REST API base URL.
REST_ROOT = "https://api.crypto.com/exchange/v1"

# Your API key/secret from Crypto.com Exchange.
# NOTE: Keep these secure and do not commit them to source control.
API_KEY = "YOUR_KEY"
API_SECRET = "YOUR_SECRET"

# Spot instrument name you requested.
# On Crypto.com Exchange, spot instruments commonly look like "BTC_USD", "BTC_USDT", etc.
INSTRUMENT = "BTC_USD"

# Candle timeframe:
# Crypto.com supports several timeframes; here we use 15m.
TIMEFRAME = "15m"

# WebSocket subscription channel for candlesticks:
# Format: candlestick.{timeframe}.{instrument_name}
CHANNEL = f"candlestick.{TIMEFRAME}.{INSTRUMENT}"

# Postgres connection string (DSN).
# Replace with your own host/user/password/dbname.
POSTGRES_DSN = "host=localhost port=5432 dbname=trading user=postgres password=postgres"


# =============================================================================
# DATA MODELS (USING DATACLASSES)
# =============================================================================

@dataclass(frozen=True)
class Candle:
    """
    Represents one OHLCV candlestick.

    frozen=True makes this object immutable after creation:
    - prevents accidental mutation (good for market data integrity)
    """
    t: int    # Candle start timestamp in milliseconds
    o: float  # Open price
    h: float  # High price
    l: float  # Low price
    c: float  # Close price
    v: float  # Volume

    @property
    def range_(self) -> float:
        """
        The candle range is the difference between high and low.
        Used as a proxy for volatility for that period.
        """
        return self.h - self.l


@dataclass
class SetupBox:
    """
    Represents the most recent 'setup candle' range box.
    Strategy will look for breakout beyond this high/low.
    """
    setup_high: float
    setup_low: float
    avg_range: float  # average range at the time setup formed (reference volatility)
    t: int           # candle time (ms) of the setup candle


@dataclass
class StrategyConfig:
    """
    Strategy tuning parameters.
    """
    lookback_n: int = 20       # number of candles to compute average range
    compression_k: float = 0.6 # setup candle must have range < k * average_range
    expansion_m: float = 1.2   # breakout candle must have range > m * average_range


@dataclass
class BotConfig:
    """
    Bot runtime parameters.
    """
    instrument: str = INSTRUMENT
    timeframe: str = TIMEFRAME
    channel: str = CHANNEL

    # Quantity is the amount used in create-order.
    # For spot, this is commonly base-asset amount (BTC),
    # but confirm with your exchange rules / API behavior.
    quantity: str = "0.001"  # example: 0.001 BTC


# =============================================================================
# POSTGRES LOGGER
# =============================================================================

class PostgresTradeLogger:
    """
    Responsible for:
    - Opening a Postgres connection
    - Creating the trades table if it doesn't exist
    - Inserting trade attempts and responses

    Why do this?
    - auditing: know what orders were sent
    - debugging: see failures + exact API responses
    - analysis: export later for performance stats
    """

    def __init__(self, dsn: str):
        """
        Connect to Postgres and ensure schema exists.
        """
        self.dsn = dsn
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = True  # simpler: each insert commits immediately
        self._ensure_schema()

    def close(self) -> None:
        """
        Close DB connection (safe to call multiple times).
        """
        try:
            self.conn.close()
        except Exception:
            pass

    def _ensure_schema(self) -> None:
        """
        Create the `trades` table if it doesn't exist.
        Also creates a couple indexes to speed up queries.
        """
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id BIGSERIAL PRIMARY KEY,
                ts_ms BIGINT NOT NULL,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity NUMERIC NOT NULL,
                signal TEXT NOT NULL,

                candle_t BIGINT,
                candle_close NUMERIC,
                setup_high NUMERIC,
                setup_low NUMERIC,

                request_json JSONB,
                response_json JSONB,

                ok BOOLEAN NOT NULL,
                error_text TEXT
            );
            """)
            # Index by timestamp for fast chronological queries
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_ms);")
            # Index by instrument for multi-instrument setups
            cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_instr ON trades(instrument);")

    def log_trade(
        self,
        *,
        instrument: str,
        side: str,
        quantity: str,
        signal: str,
        candle: Optional[Candle],
        setup: Optional[SetupBox],
        request_json: Optional[dict],
        response_json: Optional[dict],
        ok: bool,
        error_text: Optional[str] = None
    ) -> None:
        """
        Insert one trade log record.

        We store:
        - context from the candle that caused the signal
        - setup box levels (if available)
        - request/response payloads for exact reproducibility
        - ok/error for simple filtering
        """
        ts_ms = int(time.time() * 1000)

        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO trades (
                ts_ms, instrument, side, quantity, signal,
                candle_t, candle_close, setup_high, setup_low,
                request_json, response_json, ok, error_text
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            );
            """, (
                ts_ms, instrument, side, quantity, signal,

                candle.t if candle else None,
                candle.c if candle else None,
                setup.setup_high if setup else None,
                setup.setup_low if setup else None,

                Json(request_json) if request_json else None,
                Json(response_json) if response_json else None,

                ok, error_text
            ))


# =============================================================================
# CRYPTO.COM SIGNING + REST CLIENT
# =============================================================================

class CryptoComSigner:
    """
    Implements Crypto.com Exchange v1 signing:
    sig = HMAC_SHA256(secret, method + id + api_key + param_string + nonce)

    Where:
    - method: "private/create-order"
    - id: integer request id
    - api_key: your key
    - param_string: sorted keys concatenated as key+value
    - nonce: integer ms timestamp
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")

    @staticmethod
    def _param_string(params: Dict[str, Any]) -> str:
        """
        Create the param string by:
        - sorting keys
        - concatenating key+value (no delimiters)
        """
        out = ""
        for k in sorted(params.keys()):
            v = params[k]
            if v is None or v == "":
                continue
            out += f"{k}{v}"
        return out

    def sign(self, method: str, req_id: int, params: Dict[str, Any], nonce: int) -> str:
        """
        Create the signature according to the docs.
        """
        pstr = self._param_string(params)
        payload = f"{method}{req_id}{self.api_key}{pstr}{nonce}"
        return hmac.new(self.api_secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()


class CryptoComRestClient:
    """
    Thin REST client for trading endpoints.
    """

    def __init__(self, rest_root: str, signer: CryptoComSigner, timeout_s: int = 10):
        self.rest_root = rest_root.rstrip("/")
        self.signer = signer
        self.timeout_s = timeout_s

    @staticmethod
    def _now_ms() -> int:
        """
        Current time in milliseconds. Used for request ids and nonce.
        """
        return int(time.time() * 1000)

    def create_order_market(self, instrument: str, side: str, quantity: str) -> Tuple[dict, dict]:
        """
        Place a MARKET order.

        Returns:
            (response_json, request_payload)

        We also return the request payload so it can be logged exactly to Postgres.

        NOTE:
        - For spot, ensure side is "BUY" or "SELL"
        - Confirm how quantity is interpreted for your pair (base asset quantity is common)
        """
        method = "private/create-order"

        req_id = self._now_ms()
        nonce = self._now_ms()

        params = {
            "instrument_name": instrument,
            "side": side,          # BUY or SELL
            "type": "MARKET",
            "quantity": quantity
        }

        # Produce signature
        sig = self.signer.sign(method=method, req_id=req_id, params=params, nonce=nonce)

        # Full request payload
        payload = {
            "id": req_id,
            "method": method,
            "api_key": self.signer.api_key,
            "params": params,
            "nonce": nonce,
            "sig": sig
        }

        # Submit request
        url = f"{self.rest_root}/{method}"
        r = requests.post(url, json=payload, timeout=self.timeout_s)

        # If non-200, raise exception to be caught/logged by caller
        r.raise_for_status()

        return r.json(), payload


# =============================================================================
# CANDLE STORE + STRATEGY
# =============================================================================

class CandleStore:
    """
    Maintains a rolling window of candles and their ranges.

    Why:
    - We need an average range (volatility baseline)
    - We need previous candle to check "inside candle"
    """

    def __init__(self, maxlen: int):
        self.candles: Deque[Candle] = deque(maxlen=maxlen)
        self.ranges: Deque[float] = deque(maxlen=maxlen)

    def add(self, candle: Candle) -> None:
        """
        Append a candle and its computed range to rolling deques.
        """
        self.candles.append(candle)
        self.ranges.append(candle.range_)

    def ready(self, n: int) -> bool:
        """
        True if we have at least n candles for computing signals.
        """
        return len(self.candles) >= n and len(self.ranges) >= n

    def avg_range(self) -> float:
        """
        Simple moving average of the stored ranges.
        """
        return sum(self.ranges) / len(self.ranges)

    def last(self) -> Candle:
        """
        Most recent candle.
        """
        return self.candles[-1]

    def prev(self) -> Candle:
        """
        Previous candle.
        """
        return self.candles[-2]


class CandleRangeStrategy:
    """
    Candle Range Theory Strategy (systematic version)

    Definitions:
    - range = high - low
    - avg_range = SMA(range, N)

    Setup condition:
    - candle is "inside": high <= prev_high AND low >= prev_low
    - AND candle range is small: range < compression_k * avg_range

    Trigger condition:
    - after a setup exists, look for an expansion candle:
         range > expansion_m * avg_range_at_setup
      AND close beyond the setup range:
         close > setup_high  => BUY
         close < setup_low   => SELL
    """

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.latest_setup: Optional[SetupBox] = None

    @staticmethod
    def _is_inside(cur: Candle, prev: Candle) -> bool:
        """
        Inside candle: current candle entirely contained within previous candle.
        """
        return cur.h <= prev.h and cur.l >= prev.l

    def on_closed_candle(self, store: CandleStore) -> Optional[str]:
        """
        Called each time a new closed candle is added.

        Returns:
            - "SETUP" when a setup candle is found
            - "BUY" or "SELL" when breakout triggers
            - None if no action
        """
        # Need enough candles to compute avg range robustly
        if not store.ready(self.cfg.lookback_n):
            return None

        cur = store.last()
        prev = store.prev()
        avg_r = store.avg_range()

        # --- SETUP DETECTION ---
        # Small range relative to average AND inside candle
        setup_ok = (cur.range_ < self.cfg.compression_k * avg_r) and self._is_inside(cur, prev)

        if setup_ok:
            # Store the setup "box" high/low for future breakout checks
            self.latest_setup = SetupBox(
                setup_high=cur.h,
                setup_low=cur.l,
                avg_range=avg_r,
                t=cur.t
            )
            return "SETUP"

        # --- BREAKOUT DETECTION ---
        # If we don't have a setup, nothing to do
        if self.latest_setup is None:
            return None

        # Require a big candle relative to average range at setup formation
        expansion = cur.range_ > self.cfg.expansion_m * self.latest_setup.avg_range
        if not expansion:
            return None

        # Breakout above setup range -> BUY
        if cur.c > self.latest_setup.setup_high:
            self.latest_setup = None  # clear setup to avoid repeated entries
            return "BUY"

        # Breakout below setup range -> SELL
        if cur.c < self.latest_setup.setup_low:
            self.latest_setup = None
            return "SELL"

        # Expanded but didn't close beyond range -> ignore
        return None


# =============================================================================
# WEBSOCKET MARKET FEED
# =============================================================================

class CryptoComMarketWS:
    """
    Handles connecting to Crypto.com market WebSocket and streaming candle updates.

    It yields Candle objects for each new candle time `t` observed.
    """

    def __init__(self, url: str, channel: str):
        self.url = url
        self.channel = channel
        self._last_t: Optional[int] = None  # used to avoid processing repeated updates

    async def stream_closed_candles(self):
        """
        Async generator yielding candles.

        Steps:
        1) Connect WS
        2) Wait 1 second (avoid TOO_MANY_REQUESTS)
        3) Send subscribe message
        4) Read messages forever
        5) Extract candlestick messages
        6) Yield Candle when a new timestamp appears
        """
        async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
            # Crypto.com docs recommend waiting briefly after connect to avoid rate issues
            await asyncio.sleep(1)

            # Subscribe to desired channel
            sub = {"id": 1, "method": "subscribe", "params": {"channels": [self.channel]}}
            await ws.send(json.dumps(sub))

            while True:
                # Receive next message
                msg = json.loads(await ws.recv())

                # Candlestick messages typically appear under msg["result"]
                result = msg.get("result") or {}

                # Filter for candlestick channel
                if result.get("channel") != "candlestick":
                    continue

                # Data is usually a list of candles; take the most recent
                data = result.get("data") or []
                if not data:
                    continue

                raw = data[-1]
                t = int(raw["t"])

                # If same t as previous, it's likely an update to current candle -> skip
                if self._last_t is not None and t == self._last_t:
                    continue

                # New t => treat as a new candle
                self._last_t = t

                # Yield Candle instance
                yield Candle(
                    t=t,
                    o=float(raw["o"]),
                    h=float(raw["h"]),
                    l=float(raw["l"]),
                    c=float(raw["c"]),
                    v=float(raw["v"]),
                )


# =============================================================================
# BOT ORCHESTRATOR
# =============================================================================

class TradingBot:
    """
    Coordinates:
    - market data streaming
    - strategy evaluation
    - order execution
    - logging to Postgres

    This class is where you would expand to:
    - real position tracking via balances
    - placing stop-loss/TP orders
    - handling reconnections
    - handling rate limits
    - handling partial fills / fills stream
    """

    def __init__(self, bot_cfg: BotConfig, strat_cfg: StrategyConfig, pg_dsn: str):
        # Store bot config
        self.bot_cfg = bot_cfg

        # Rolling candle store; keep more than lookback for safety
        self.store = CandleStore(maxlen=max(strat_cfg.lookback_n, 80))

        # Strategy instance
        self.strategy = CandleRangeStrategy(strat_cfg)

        # REST client for order placement
        self.rest = CryptoComRestClient(
            rest_root=REST_ROOT,
            signer=CryptoComSigner(API_KEY, API_SECRET),
        )

        # WebSocket client for market candles
        self.ws = CryptoComMarketWS(MARKET_WS_URL, bot_cfg.channel)

        # Postgres logger
        self.logger = PostgresTradeLogger(pg_dsn)

        # Minimal position guard:
        # In spot, you can interpret "in position" as holding BTC.
        # Here we just flip a flag after an order to prevent spamming.
        self.in_position = False

    def shutdown(self) -> None:
        """
        Cleanup resources.
        """
        self.logger.close()

    def _execute_and_log(self, side: str, signal: str, candle: Candle) -> None:
        """
        Execute order via REST, log everything to Postgres.

        If REST call fails:
        - log ok=False with error
        - re-raise exception
        """
        # Capture current setup box (might be None if strategy cleared it)
        setup = self.strategy.latest_setup

        try:
            response, request_payload = self.rest.create_order_market(
                instrument=self.bot_cfg.instrument,
                side=side,
                quantity=self.bot_cfg.quantity
            )

            # Log success
            self.logger.log_trade(
                instrument=self.bot_cfg.instrument,
                side=side,
                quantity=self.bot_cfg.quantity,
                signal=signal,
                candle=candle,
                setup=setup,
                request_json=request_payload,
                response_json=response,
                ok=True,
                error_text=None
            )

            print(f"[ORDER {side}] {response}")

        except Exception as e:
            # Log failure
            self.logger.log_trade(
                instrument=self.bot_cfg.instrument,
                side=side,
                quantity=self.bot_cfg.quantity,
                signal=signal,
                candle=candle,
                setup=setup,
                request_json=None,
                response_json=None,
                ok=False,
                error_text=str(e)
            )

            # Re-raise so the bot stops (or you can handle/retry)
            raise

    async def run(self) -> None:
        """
        Main loop:
        - Iterate over new candles from WebSocket
        - Add to store
        - Evaluate strategy
        - If signal => place order and log
        """
        try:
            async for candle in self.ws.stream_closed_candles():
                # Update rolling candle store
                self.store.add(candle)

                # Ask strategy what to do
                sig = self.strategy.on_closed_candle(self.store)

                # When a setup forms, just print info
                if sig == "SETUP":
                    s = self.strategy.latest_setup
                    print(
                        f"[15m SETUP] t={candle.t} close={candle.c:.2f} "
                        f"box=({s.setup_low:.2f}-{s.setup_high:.2f})"
                    )
                    continue

                # When BUY/SELL triggers
                if sig in ("BUY", "SELL"):
                    # Prevent repeated entries (placeholder)
                    if self.in_position:
                        print(f"[SKIP] {sig} signal but already in position")
                        continue

                    print(f"[SIGNAL] {sig} t={candle.t} close={candle.c:.2f}")

                    # Execute and log the trade
                    self._execute_and_log(side=sig, signal="BREAKOUT", candle=candle)

                    # Mark "in position"
                    self.in_position = True

        finally:
            # Always cleanup DB connection when exiting
            self.shutdown()


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    # Bot config: instrument/timeframe/quantity
    bot_cfg = BotConfig(
        instrument=INSTRUMENT,
        timeframe=TIMEFRAME,
        channel=CHANNEL,
        quantity="0.001",  # example fixed size (BTC)
    )

    # Strategy config: how strict compression/expansion are
    strat_cfg = StrategyConfig(
        lookback_n=20,
        compression_k=0.6,
        expansion_m=1.2
    )

    # Create bot and run
    bot = TradingBot(bot_cfg, strat_cfg, POSTGRES_DSN)
    asyncio.run(bot.run())