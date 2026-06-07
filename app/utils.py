# WebSocket client implementation for streaming crypto candle data,
# buffering it in-memory, and persisting finalized candles into PostgreSQL.

import websocket            # WebSocket client library (real-time streaming)
import socket               # Used to retrieve hostname of machine
import os                   # (Unused in current code, likely reserved for env/config)
import json                 # For encoding/decoding WebSocket messages
import time                 # Used for retry/sleep logic in reconnect loops
from dataclasses import dataclass  # Used to define structured Candle object
import pandas as pd         # Used to convert stored candles into DataFrame
from uuid6 import uuid7      # Generates unique IDs for transactions/feeds
import threading            # (Unused here, likely intended for db_worker threading)
import psycopg2             # PostgreSQL driver
from collections import deque  # Efficient FIFO queue for candle buffering
import logging
import requests
import sys
import signal
from typing import Optional

# ----------------------------
# LOGGING CONFIGURATION
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# ----------------------------
# DATA MODEL
# ----------------------------

@dataclass
class Candle:
    """
    Represents a single OHLCV candle (Open, High, Low, Close, Volume)
    with metadata about source feed and execution context.
    """

    # Identifiers and metadata
    transaction_id: str   # Unique ID per incoming message batch
    feed_id: str          # Unique ID per websocket feed instance
    symbol: str           # Trading pair (e.g., BTC_USD)
    interval: str         # Candle interval (e.g., 1m, 5m)
    method: str           # WebSocket method used (subscribe, etc.)
    srv_id: int           # Server-side subscription ID
    hostname: str         # Machine hostname running this process

    # Candle data fields
    start_timestamp: int  # Candle open time (usually ms epoch)
    open: float           # Open price
    high: float           # Highest price in interval
    low: float            # Lowest price in interval
    close: float          # Closing price
    volume: float         # Traded volume in interval
    last_update_timestamp: int  # Last update timestamp of candle


# ----------------------------
# MESSAGE PARSING
# ----------------------------

def parse_candles(msg: dict, transaction_id: str, feed_id: str, symbol: str, interval: str, method: str, srv_id: int, hostname: str):
    """
    Extracts candle data from incoming WebSocket message and converts it
    into a list of Candle objects.

    Expected message structure:
    {
        "result": {
            "data": [ {...candle...}, {...} ]
        }
    }
    """

    # Safely navigate nested dict structure; fallback to empty list if missing
    data = msg.get("result", {}).get("data", [])

    # Convert raw candle dictionaries into Candle dataclass instances
    return [
        Candle(
            transaction_id=transaction_id,
            feed_id=feed_id,
            symbol=symbol,
            interval=interval,
            method=method,
            srv_id=srv_id,
            hostname=hostname,

            # Convert raw string values into proper numeric types
            start_timestamp=int(c["t"]),
            open=float(c["o"]),
            high=float(c["h"]),
            low=float(c["l"]),
            close=float(c["c"]),
            volume=float(c["v"]),
            last_update_timestamp=int(c["ut"])
        )
        for c in data
    ]


# ----------------------------
# IN-MEMORY CANDLE STORAGE
# ----------------------------

class CandleStore:
    """
    Stores streaming candles in memory.

    Responsibilities:
    - Keep track of latest active candle
    - Detect when a candle closes
    - Buffer closed candles for DB insertion
    """

    def __init__(self, feed_id, symbol, interval, method, srv_id, db, hostname):
        # Metadata for identifying this stream
        self.feed_id = feed_id
        self.symbol = symbol
        self.interval = interval
        self.method = method
        self.srv_id = srv_id
        self.db = db                  # DBManager instance (not used directly here yet)
        self.hostname = hostname

        # Storage structures
        self.history = []             # Permanently closed candles
        self.buffer = deque()         # Queue for DB worker consumption
        self.latest = None            # Currently active (in-progress) candle
        self.seen_timestamps = set()  # track what's already been closed+buffered

    def update(self, msg: dict, transaction_id: str):
        """
        Processes incoming WebSocket message and updates candle state.

        Logic:
        - Parse candles
        - Track latest candle
        - Detect candle rollover (new timestamp = new candle)
        - Push closed candles into buffer
        """

        candles = parse_candles(
            msg,
            transaction_id,
            self.feed_id,
            self.symbol,
            self.interval,
            self.method,
            self.srv_id,
            self.hostname
        )

        for c in candles:

            # First ever candle received becomes latest reference
            if self.latest is None:
                self.latest = c
                continue

            # Same candle timestamp → update in-place (still forming candle)
            if c.start_timestamp == self.latest.start_timestamp:
                self.latest = c
                continue

            # New timestamp detected → previous candle is now CLOSED
            if c.start_timestamp > self.latest.start_timestamp:
                closed = self.latest  # finalize previous candle

                if closed.start_timestamp not in self.seen_timestamps:

                    # Store closed candle in persistent history
                    self.history.append(closed)

                    # Add to buffer for DB writing (async-style pipeline)
                    self.buffer.append(closed)

                # Replace latest with new active candle
                self.latest = c

            # Out-of-order candle — timestamp is older than current latest
            if c.start_timestamp < self.latest.start_timestamp:
                logging.warning(
                    "Out-of-order candle for %s — received %s but latest is %s",
                    self.symbol,
                    c.start_timestamp,
                    self.latest.start_timestamp
                )
                continue

    def to_dataframe(self):
        """
        Converts stored candles into a Pandas DataFrame.

        Includes:
        - historical closed candles
        - optionally current in-progress candle
        """

        all_candles = self.history[:]  # copy history list

        # Include latest candle if exists
        if self.latest:
            all_candles.append(self.latest)

        # Convert list of dataclass objects to dicts for DataFrame
        df = pd.DataFrame([c.__dict__ for c in all_candles])

        # Optional conversion to datetime (currently disabled)
        # df["start_timestamp"] = pd.to_datetime(df["start_timestamp"], unit="ms")

        return df


# ----------------------------
# WEBSOCKET CLIENT
# ----------------------------

class CryptoSocket:
    """
    Manages WebSocket connection to crypto data provider.

    Responsibilities:
    - Connect to endpoint
    - Subscribe to channels
    - Receive messages
    - Forward data to CandleStore
    - Handle reconnect logic
    """

    def __init__(self, endpoint, subscribe_message, db, telegram_notifications):
        self.endpoint = endpoint
        self.subscribe_message = subscribe_message
        self.telegram_notifications = telegram_notifications
        self.ws = None
        self.db = db

        # Unique identifier for this feed instance
        self.feed_id = str(uuid7())

        # Machine identity (useful in distributed systems)
        self.hostname = socket.gethostname()

        # Extract subscription details from message
        channel = subscribe_message["params"]["channels"][0]
        parts = channel.split(".")

        # Example: channel = "candlestick.1m.BTC_USD"
        self.interval = parts[1]
        self.symbol = parts[2]

        self.method = subscribe_message["method"]
        self.srv_id = subscribe_message["id"]

        # Candle storage instance tied to this socket
        self.store = CandleStore(
            self.feed_id,
            self.symbol,
            self.interval,
            self.method,
            self.srv_id,
            self.db,
            self.hostname
        )

        self._shutting_down = False  # flag to prevent double Telegram on shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def on_open(self, ws):
        """
        Called when WebSocket connection is established.
        Sends subscription request.
        """
        logging.info("Connected")
        ws.send(json.dumps(self.subscribe_message))
        self.telegram_notifications.send_telegram(f"✅ crypto deamon started on {self.hostname}, subscribed to {self.symbol} {self.interval} candles")

    def on_message(self, ws, message):
        """
        Called whenever a message is received from server.
        """
        transaction_id = str(uuid7())  # unique per message batch

        # Parse JSON message
        data = json.loads(message)

        # --- Heartbeat response ---
        # crypto.com sends {"method": "public/heartbeat", "code": 0}
        # and requires a respond-heartbeat reply or it closes the connection (~90s timeout)
        if data.get("method") == "public/heartbeat":
            ws.send(json.dumps({
                "id": data["id"],
                "method": "public/respond-heartbeat"
            }))
            return

        # Forward candle data to store as before
        self.store.update(data, transaction_id)

    def on_close(self, ws, close_status_code, close_msg):
        """
        Triggered when WebSocket connection closes.
        """
        logging.info("Disconnected")

        # flush the last active candle before losing it
        if self.store.latest is not None:
            self.store.history.append(self.store.latest)
            self.store.buffer.append(self.store.latest)
            self.store.seen_timestamps.add(self.store.latest.start_timestamp)

        if not self._shutting_down:
            self.telegram_notifications.send_telegram(f"✅ crypto deamon disconnected from {self.hostname}, attempting to reconnect...")

    def on_error(self, ws, error):
        """
        Handles WebSocket errors.
        """
        logging.error("Error: %s", error)
        self.telegram_notifications.send_telegram(f"❌ crypto deamon error on {self.hostname}: {error}")

    def run(self):
        """
        Main execution loop.

        Keeps WebSocket alive with automatic reconnection.
        """

        while True:
            try:
                # Create WebSocket connection
                self.ws = websocket.WebSocketApp(
                    self.endpoint,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_close=self.on_close,
                    on_error=self.on_error
                )

                # Run event loop with ping keepalive
                self.ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                )

            except Exception as e:
                # Any unexpected failure triggers reconnect
                logging.error("Reconnect due to: %s", e)
            
            if self._shutting_down:
                break

            # Prevent tight reconnect loop
            time.sleep(3)

    def _handle_shutdown(self, signum, frame):
        """
        Called when Docker sends SIGTERM (compose down) or SIGINT (Ctrl+C).
        Flushes the last candle, sends Telegram notification, then exits cleanly.
        """
        logging.info("Shutdown signal received, closing gracefully...")

        if self._shutting_down:
            return
        self._shutting_down = True

        # Flush latest candle to buffer before closing
        if self.store.latest is not None:
            self.store.history.append(self.store.latest)
            self.store.buffer.append(self.store.latest)
            self.store.latest = None

        # Send Telegram notification
        self.telegram_notifications.send_telegram(
            f"🔴 crypto daemon stopped on {self.hostname} ({self.symbol} {self.interval})"
        )

        # Close the WebSocket connection cleanly
        if self.ws:
            self.ws.close()

        sys.exit(0)

# ----------------------------
# CANDLE RANGE THEORY (CRT)
# ----------------------------

@dataclass
class CRTSignal:
    """
    Represents a CRT-based trade signal.
    """
    direction: str            # "BUY" or "SELL"
    entry: float              # Suggested entry price
    stop_loss: float          # Stop loss level
    take_profit: float        # Take profit target
    reference_candle_ts: int  # Timestamp of the reference candle
    trigger_candle_ts: int    # Timestamp of the sweep candle
    sweep_type: str           # "high_sweep" or "low_sweep"
    risk_reward: float        # R:R ratio


def identify_reference_candle(
    df: pd.DataFrame,
    lookback: int = 3
) -> Optional[pd.Series]:
    """
    Identifies the most recent significant reference candle to anchor CRT logic.

    A valid reference candle has:
    - A range (high - low) larger than the average of the previous N candles
    - A clear directional body (open != close meaningfully)

    Args:
        df:       DataFrame of closed candles with columns:
                [start_timestamp, open, high, low, close, volume]
        lookback: Number of prior candles used to compute the average range.

    Returns:
        The reference candle as a pd.Series, or None if not found.
    """
    if len(df) < lookback + 1:
        logging.warning("Not enough candles to identify reference candle.")
        return None

    df = df.copy().reset_index(drop=True)
    df["range"] = df["high"] - df["low"]
    df["body"] = abs(df["close"] - df["open"])

    # Use all candles except the very last (which may still be forming)
    window = df.iloc[-(lookback + 1):-1]
    avg_range = window["range"].mean()

    # Find the candle with range meaningfully above average
    candidates = window[window["range"] > avg_range * 1.1]

    if candidates.empty:
        logging.info("No significant reference candle found in lookback window.")
        return None

    # Pick the most recent significant candle
    ref = candidates.iloc[-1]
    logging.info(
        "Reference candle identified: ts=%s | H=%.4f | L=%.4f | range=%.4f",
        ref["start_timestamp"], ref["high"], ref["low"], ref["range"]
    )
    return ref


def detect_crt_signal(
    df: pd.DataFrame,
    lookback: int = 3,
    sweep_buffer: float = 0.001,
    min_rr: float = 1.5
) -> Optional[CRTSignal]:
    """
    Detects a Candle Range Theory (CRT) buy or sell signal.

    CRT Logic:
    ┌─────────────────────────────────────────────────────────┐
    │  1. Identify a reference candle (significant range)     │
    │  2. Next candle sweeps ABOVE its high → liquidity grab  │
    │     → expect bearish reversal → SELL signal             │
    │  3. Next candle sweeps BELOW its low → liquidity grab   │
    │     → expect bullish reversal → BUY signal              │
    │  4. Entry on close back inside the reference range      │
    │  5. SL beyond the sweep extreme                         │
    │  6. TP at the opposite end of the reference candle      │
    └─────────────────────────────────────────────────────────┘

    Args:
        df:            DataFrame of closed candles (ascending order).
        lookback:      Candles to look back for reference candle selection.
        sweep_buffer:  Fractional buffer to confirm a sweep (e.g. 0.001 = 0.1%).
                    Avoids false positives from wicks just touching the level.
        min_rr:        Minimum risk/reward ratio required to emit a signal.

    Returns:
        A CRTSignal if conditions are met, otherwise None.
    """
    if len(df) < lookback + 2:
        logging.warning("Not enough candles for CRT analysis.")
        return None

    df = df.copy().reset_index(drop=True)

    ref = identify_reference_candle(df, lookback=lookback)
    if ref is None:
        return None

    ref_high = ref["high"]
    ref_low  = ref["low"]
    ref_ts   = ref["start_timestamp"]

    # The trigger candle is the one immediately after the reference
    ref_idx = df[df["start_timestamp"] == ref_ts].index[0]

    # We need at least one candle after the reference
    if ref_idx + 1 >= len(df):
        logging.info("No candle after reference candle yet.")
        return None

    trigger = df.iloc[ref_idx + 1]
    trigger_ts = trigger["start_timestamp"]

    # --- HIGH SWEEP → SELL signal ---
    # Price wicks above reference high, then closes back inside the range
    high_swept = trigger["high"] > ref_high * (1 + sweep_buffer)
    closed_back_inside_high = trigger["close"] < ref_high

    if high_swept and closed_back_inside_high:
        entry      = trigger["close"]
        stop_loss  = trigger["high"] * 1.001   # just above the sweep wick
        take_profit = ref_low                   # opposite end of reference candle

        risk   = stop_loss - entry
        reward = entry - take_profit

        if risk <= 0:
            return None

        rr = round(reward / risk, 2)
        if rr < min_rr:
            logging.info("SELL signal rejected — R:R %.2f below minimum %.2f", rr, min_rr)
            return None

        logging.info(
            "CRT SELL signal | entry=%.4f | SL=%.4f | TP=%.4f | R:R=%.2f",
            entry, stop_loss, take_profit, rr
        )
        return CRTSignal(
            direction="SELL",
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reference_candle_ts=int(ref_ts),
            trigger_candle_ts=int(trigger_ts),
            sweep_type="high_sweep",
            risk_reward=rr
        )

    # --- LOW SWEEP → BUY signal ---
    # Price wicks below reference low, then closes back inside the range
    low_swept = trigger["low"] < ref_low * (1 - sweep_buffer)
    closed_back_inside_low = trigger["close"] > ref_low

    if low_swept and closed_back_inside_low:
        entry      = trigger["close"]
        stop_loss  = trigger["low"] * 0.999    # just below the sweep wick
        take_profit = ref_high                  # opposite end of reference candle

        risk   = entry - stop_loss
        reward = take_profit - entry

        if risk <= 0:
            return None

        rr = round(reward / risk, 2)
        if rr < min_rr:
            logging.info("BUY signal rejected — R:R %.2f below minimum %.2f", rr, min_rr)
            return None

        logging.info(
            "CRT BUY signal | entry=%.4f | SL=%.4f | TP=%.4f | R:R=%.2f",
            entry, stop_loss, take_profit, rr
        )
        return CRTSignal(
            direction="BUY",
            entry=entry,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reference_candle_ts=int(ref_ts),
            trigger_candle_ts=int(trigger_ts),
            sweep_type="low_sweep",
            risk_reward=rr
        )

    return None


def run_crt_strategy(store) -> Optional[CRTSignal]:
    """
    Convenience wrapper: pulls the latest closed candles from a CandleStore
    and runs the full CRT detection pipeline.

    Call this after each candle closes (e.g. from db_worker or a scheduler).

    Args:
        store: CandleStore instance with populated history.

    Returns:
        CRTSignal if a setup is detected, otherwise None.
    """
    df = store.to_dataframe()

    # Only use closed candles (exclude the in-progress latest candle)
    # to_dataframe() appends self.latest at the end — drop it
    if store.latest is not None and len(df) > 0:
        df = df.iloc[:-1]

    if df.empty:
        return None

    df = df.sort_values("start_timestamp").reset_index(drop=True)
    return detect_crt_signal(df)


def reconcile_open_signals(store, db_thread):
    """
    Checks all open (unresolved) signals against the candle history
    and updates their outcome if TP or SL has been reached.

    Logic per open signal:
    - Find all candles that closed AFTER the trigger candle
    - Walk them in chronological order
    - First candle whose high >= TP (BUY) or low <= TP (SELL) → WIN
    - First candle whose low  <= SL (BUY) or high >= SL (SELL) → LOSS
    - Whichever comes first wins; if same candle, call it LOSS (conservative)

    Args:
        store:     CandleStore with full candle history
        db_thread: DBManager instance wrapping the worker's thread-local connection
    """
    open_signals = db_thread.fetch_open_signals()

    if not open_signals:
        return

    # Build a DataFrame of all closed candles for lookup
    df = store.to_dataframe()
    if store.latest is not None and len(df) > 0:
        df = df.iloc[:-1]  # exclude still-forming candle

    if df.empty:
        return

    df = df.sort_values("start_timestamp").reset_index(drop=True)

    for row in open_signals:
        signal_id, feed_id, direction, entry, stop_loss, take_profit, trigger_ts = row

        # Only look at candles that formed AFTER the trigger candle
        future = df[df["start_timestamp"] > trigger_ts].reset_index(drop=True)

        if future.empty:
            continue  # no new candles since signal fired — still open

        outcome        = None
        close_price    = None
        close_timestamp = None
        pnl_r          = None

        for _, candle in future.iterrows():

            if direction == "BUY":
                tp_hit = candle["high"] >= take_profit
                sl_hit = candle["low"]  <= stop_loss

            else:  # SELL
                tp_hit = candle["low"]  <= take_profit
                sl_hit = candle["high"] >= stop_loss

            if sl_hit and tp_hit:
                # Both hit on the same candle — conservative: call it a LOSS
                outcome         = "LOSS"
                close_price     = stop_loss
                close_timestamp = int(candle["start_timestamp"])
                risk            = abs(entry - stop_loss)
                pnl_r           = round(-1.0, 4)

            elif tp_hit:
                outcome         = "WIN"
                close_price     = take_profit
                close_timestamp = int(candle["start_timestamp"])
                risk            = abs(entry - stop_loss)
                reward          = abs(take_profit - entry)
                pnl_r           = round(reward / risk, 4)

            elif sl_hit:
                outcome         = "LOSS"
                close_price     = stop_loss
                close_timestamp = int(candle["start_timestamp"])
                pnl_r           = round(-1.0, 4)

            if outcome:
                break  # stop at first resolved candle

        if outcome:
            db_thread.update_signal_outcome(
                signal_id=signal_id,
                outcome=outcome,
                close_price=close_price,
                close_timestamp=close_timestamp,
                pnl_r=pnl_r
            )
            logging.info(
                "Signal %s resolved: %s | close=%.4f | pnl_r=%.2f",
                signal_id, outcome, close_price, pnl_r
            )
# ----------------------------
# DATABASE LAYER
# ----------------------------

class DBManager:
    """
    Handles PostgreSQL interactions:
    - Table initialization
    - Feed metadata insertion
    - Candle persistence
    """

    def __init__(self, host, port, database, user, password):
        # Establish persistent DB connection
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def init_db(self):
        """
        Creates required tables if they do not exist.
        """

        cur = self.conn.cursor()

        # Table storing metadata about each websocket feed
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_feeds(
                id                  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                ,feed_id            UUID NOT NULL
                ,srv_id             INT NOT NULL
                ,symbol             TEXT NOT NULL
                ,method             TEXT NOT NULL
                ,hostname           TEXT NOT NULL
                ,insert_timestamp   TIMESTAMPTZ NOT NULL DEFAULT now()
                ,CONSTRAINT uq_feed UNIQUE (feed_id)
            );
        """)

        # Table storing OHLCV candle data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_candles(
                id                      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                ,transaction_id         UUID NOT NULL
                ,feed_id                UUID NOT NULL REFERENCES market_feeds(feed_id)
                ,open                   NUMERIC(18,8) NOT NULL
                ,high                   NUMERIC(18,8) NOT NULL
                ,low                    NUMERIC(18,8) NOT NULL
                ,close                  NUMERIC(18,8) NOT NULL
                ,volume                 NUMERIC(18,8) NOT NULL
                ,start_timestamp        BIGINT NOT NULL
                ,last_update_timestamp  BIGINT NOT NULL
                ,insert_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now()
                ,CONSTRAINT uq_candle   UNIQUE (feed_id, start_timestamp)
            );
        """)

        # Table storing bot signals
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_signals(
                id                          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                ,signal_id                  UUID NOT NULL
                ,feed_id                    UUID NOT NULL REFERENCES market_feeds(feed_id)
                ,direction                  TEXT NOT NULL
                ,sweep_type                 TEXT NOT NULL
                ,entry                      NUMERIC(18,8) NOT NULL
                ,stop_loss                  NUMERIC(18,8) NOT NULL
                ,take_profit                NUMERIC(18,8) NOT NULL
                ,risk_reward                NUMERIC(6,2)  NOT NULL
                ,reference_candle_ts        BIGINT NOT NULL
                ,trigger_candle_ts          BIGINT NOT NULL
                ,lookback                   SMALLINT NOT NULL
                ,sweep_buffer               NUMERIC(8,5) NOT NULL
                ,outcome                    TEXT
                ,close_price                NUMERIC(18,8)
                ,close_timestamp            BIGINT
                ,pnl_r                      NUMERIC(8,4)
                ,insert_timestamp           TIMESTAMPTZ NOT NULL DEFAULT now()
                ,CONSTRAINT uq_signal       UNIQUE (signal_id)
                ,CONSTRAINT chk_direction   CHECK (direction  IN ('BUY',  'SELL'))
                ,CONSTRAINT chk_sweep_type  CHECK (sweep_type IN ('high_sweep', 'low_sweep'))
                ,CONSTRAINT chk_outcome     CHECK (outcome    IN ('WIN', 'LOSS', 'BREAKEVEN') OR outcome IS NULL)
                ,CONSTRAINT fk_reference_candle
                    FOREIGN KEY (feed_id, reference_candle_ts)
                    REFERENCES market_candles(feed_id, start_timestamp)
                ,CONSTRAINT fk_trigger_candle
                    FOREIGN KEY (feed_id, trigger_candle_ts)
                    REFERENCES market_candles(feed_id, start_timestamp)
            );
        """)

        # Commit schema changes
        self.conn.commit()
        cur.close()

    def insert_candle(self, candle):
        """
        Inserts a single candle into PostgreSQL.
        """

        cur = self.conn.cursor()

        cur.execute("""
            INSERT INTO market_candles (
                transaction_id, feed_id, open, high, low, close, volume, start_timestamp, last_update_timestamp
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            candle.transaction_id,
            candle.feed_id,
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
            candle.start_timestamp,
            candle.last_update_timestamp
        ))

        self.conn.commit()
        cur.close()

    def insert_feed(self, feed_id, srv_id, symbol, method, hostname):
        """
        Inserts feed metadata if not already existing.
        """

        cur = self.conn.cursor()

        cur.execute("""
            INSERT INTO market_feeds (
                feed_id, srv_id, symbol, method, hostname
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (feed_id) DO NOTHING;
        """, (
            feed_id,
            srv_id,
            symbol,
            method,
            hostname
        ))

        self.conn.commit()
        cur.close()

    def insert_signal(self, signal: CRTSignal, feed_id: str, lookback: int, sweep_buffer: float, min_rr: float):
        """
        Inserts a detected CRT signal into bot_signals table.
        Both referenced candles must already exist in market_candles
        due to the composite foreign key constraints.
        """
        cur = self.conn.cursor()

        cur.execute("""
            INSERT INTO bot_signals (
                signal_id, feed_id, direction, sweep_type,
                entry, stop_loss, take_profit, risk_reward,
                reference_candle_ts, trigger_candle_ts,
                lookback, sweep_buffer, min_rr
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (signal_id) DO NOTHING;
        """, (
            str(uuid7()),               # unique signal identifier
            feed_id,                    # links to market_feeds and market_candles
            signal.direction,           # 'BUY' or 'SELL'
            signal.sweep_type,          # 'high_sweep' or 'low_sweep'
            signal.entry,               # entry price (trigger candle close)
            signal.stop_loss,           # stop loss beyond sweep wick
            signal.take_profit,         # target at opposite end of reference candle
            signal.risk_reward,         # R:R ratio at signal time
            signal.reference_candle_ts, # FK → market_candles(feed_id, start_timestamp)
            signal.trigger_candle_ts,   # FK → market_candles(feed_id, start_timestamp)
            lookback,                   # strategy param — for audit/backtesting
            sweep_buffer,               # strategy param — for audit/backtesting
            min_rr                      # strategy param — for audit/backtesting
        ))

        self.conn.commit()
        cur.close()
    
    def fetch_open_signals(self):
        """
        Returns all signals where outcome has not yet been determined.
        Called each db_worker cycle to check if any open trades have resolved.
        """
        cur = self.conn.cursor()

        cur.execute("""
            SELECT signal_id, feed_id, direction,
                entry, stop_loss, take_profit,
                trigger_candle_ts
            FROM bot_signals
            WHERE outcome IS NULL
        """)

        rows = cur.fetchall()
        cur.close()
        return rows


    def update_signal_outcome(self, signal_id: str, outcome: str, close_price: float, close_timestamp: int, pnl_r: float):
        """
        Updates a signal row with its trade outcome once TP or SL is reached.
        Called by the reconciliation loop in db_worker.
        """
        cur = self.conn.cursor()

        cur.execute("""
            UPDATE bot_signals
            SET outcome         = %s
            ,close_price     = %s
            ,close_timestamp = %s
            ,pnl_r           = %s
            WHERE signal_id = %s
        """, (
            outcome,        # 'WIN', 'LOSS', or 'BREAKEVEN'
            close_price,    # price at which the trade closed
            close_timestamp,# candle start_timestamp when trade closed (ms epoch)
            pnl_r,          # e.g. +1.5 for a 1.5R win, -1.0 for a full stop out
            signal_id
        ))

        self.conn.commit()
        cur.close()


# ----------------------------
# BACKGROUND DB WORKER
# ----------------------------

def db_worker(store, db_config, telegram_notifications):
    """
    Background worker that batches and persists closed candles to PostgreSQL.
    Runs in its own thread with its own dedicated DB connection.
    
    Args:
        store:     CandleStore instance containing the buffer of closed candles
        db_config: dict of DB connection params (host, port, database, user, password)
    """

    # Create a dedicated connection for this thread.
    # This avoids sharing the main thread's connection which is not thread-safe in psycopg2.
    conn = psycopg2.connect(**db_config)

    while True:

        # Sleep before each drain cycle.
        # This acts as an implicit batching window — candles accumulate in the
        # buffer during this interval and are flushed together in one DB round trip.
        # Adjust this value to trade off latency vs DB write frequency.
        time.sleep(2)

        # Collect all candles currently in the buffer into a local list.
        # We drain the entire buffer at once rather than processing one at a time,
        # so we can insert them all in a single executemany call.
        batch = []
        while store.buffer:
            # popleft() is O(1) on a deque and thread-safe due to Python's GIL.
            # We move each candle from the shared buffer into our local batch list.
            batch.append(store.buffer.popleft())

        # Only attempt a DB write if we actually collected something.
        # Skipping empty batches avoids unnecessary DB round trips during quiet periods.
        if batch:

            cur = conn.cursor()

            # executemany sends ALL rows to PostgreSQL in a single round trip.
            # This is much faster than calling execute() once per candle,
            # where each call would incur a separate network round trip overhead.
            cur.executemany("""
                INSERT INTO market_candles (
                    transaction_id, feed_id, open, high, low, close,
                    volume, start_timestamp, last_update_timestamp
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (feed_id, start_timestamp) DO NOTHING
            """,
                # Build a list of tuples — one per candle in the batch.
                # executemany iterates over this list and substitutes %s placeholders
                # with the actual values, handling SQL escaping safely.
                [
                    (
                        c.transaction_id,          # UUID identifying this message batch
                        c.feed_id,                 # UUID identifying the websocket feed
                        c.open,                    # Opening price of the candle
                        c.high,                    # Highest price in the interval
                        c.low,                     # Lowest price in the interval
                        c.close,                   # Closing price of the candle
                        c.volume,                  # Traded volume in the interval
                        c.start_timestamp,         # Candle open time (ms epoch)
                        c.last_update_timestamp    # Last update time of the candle
                    )
                    for c in batch  # iterate over all candles collected in this cycle
                ]
            )

            # Commit the transaction — makes all inserted rows visible and durable.
            # Without this, inserts are rolled back when the connection closes.
            conn.commit()

            # After persisting candles, run strategy check
            # Candles are guaranteed to be in market_candles at this point,
            # satisfying the composite FK constraints on bot_signals.
            db_thread = DBManager.__new__(DBManager)
            db_thread.conn = conn

            crt_signal = run_crt_strategy(store)
            if crt_signal:

                # Reuse the dedicated insert_signal method on a temporary DBManager
                # that wraps the worker's own thread-local connection
                db_thread.insert_signal(
                    signal=crt_signal,
                    feed_id=store.feed_id,
                    lookback=3,
                    sweep_buffer=0.001,
                    min_rr=1.5
                )

                logging.info(
                    "CRT signal persisted: %s | entry=%.4f | SL=%.4f | TP=%.4f | R:R=%.2f",
                    crt_signal.direction, crt_signal.entry,
                    crt_signal.stop_loss, crt_signal.take_profit,
                    crt_signal.risk_reward
                )

                msg = (
                    f"📊 CRT {crt_signal.direction} signal on {store.symbol}\n"
                    f"Entry: {crt_signal.entry:.2f} | SL: {crt_signal.stop_loss:.2f} | TP: {crt_signal.take_profit:.2f}\n"
                    f"R:R: {crt_signal.risk_reward} | Sweep: {crt_signal.sweep_type}"
                )
                telegram_notifications.send_telegram(msg)

            # Check all open signals against latest candles
            # Done after signal insert so a brand-new signal is never
            # immediately resolved in the same cycle it was created
            reconcile_open_signals(store, db_thread)

            cur.close()

# ----------------------------
# TELEGRAM NOTIFICATION
# ----------------------------

class TelegramNotifications:
    """
    Handles notifications of events via telegram bot.
    """

    def __init__(self, telegram_token, telegram_chat):
        # Establish persistent DB connection
        self.telegram_token = telegram_token
        self.telegram_chat = telegram_chat

    def send_telegram(self, message):
        try:
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            response = requests.post(url, json={
                "chat_id": self.telegram_chat,
                "text": message
            }, timeout=10)
            response.raise_for_status()
        except requests.exceptions.ConnectionError as e:
            logging.error("Telegram connection failed (network/DNS): %s", e)
        except requests.exceptions.Timeout:
            logging.error("Telegram request timed out")
        except requests.exceptions.HTTPError as e:
            logging.error("Telegram HTTP error: %s | response: %s", e, e.response.text)
        except Exception as e:
            logging.error("Telegram unexpected error: %s", e)