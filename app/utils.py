# WebSocket client implementation for streaming crypto candle data,
# buffering it in-memory, and persisting finalized candles into PostgreSQL.

import websocket            # WebSocket client library (real-time streaming)
import socket               # Used to retrieve hostname of machine
import os                   # (Unused in current code, likely reserved for env/config)
import json                 # For encoding/decoding WebSocket messages
import time                 # Used for retry/sleep logic in reconnect loops
from dataclasses import dataclass  # Used to define structured Candle object
import pandas as pd         # Used to convert stored candles into DataFrame
from uuid import uuid4      # Generates unique IDs for transactions/feeds
import threading            # (Unused here, likely intended for db_worker threading)
import psycopg2             # PostgreSQL driver
from collections import deque  # Efficient FIFO queue for candle buffering


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

                # Store closed candle in persistent history
                self.history.append(closed)

                # Add to buffer for DB writing (async-style pipeline)
                self.buffer.append(closed)

                # Replace latest with new active candle
                self.latest = c

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

    def __init__(self, endpoint, subscribe_message, db):
        self.endpoint = endpoint
        self.subscribe_message = subscribe_message
        self.ws = None
        self.db = db

        # Unique identifier for this feed instance
        self.feed_id = str(uuid4())

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

    def on_open(self, ws):
        """
        Called when WebSocket connection is established.
        Sends subscription request.
        """
        print("Connected")
        ws.send(json.dumps(self.subscribe_message))

    def on_message(self, ws, message):
        """
        Called whenever a message is received from server.
        """
        transaction_id = str(uuid4())  # unique per message batch

        # Parse JSON message
        data = json.loads(message)

        # Forward to candle store
        self.store.update(data, transaction_id)

    def on_close(self, ws, close_status_code, close_msg):
        """
        Triggered when WebSocket connection closes.
        """
        print("Disconnected")

    def on_error(self, ws, error):
        """
        Handles WebSocket errors.
        """
        print("Error:", error)

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
                print("Reconnect due to:", e)

            # Prevent tight reconnect loop
            time.sleep(3)


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
                id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                feed_id TEXT NOT NULL,
                srv_id INT NOT NULL,
                symbol TEXT NOT NULL,
                method TEXT NOT NULL,
                hostname TEXT NOT NULL,
                insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
                CONSTRAINT uq_feed UNIQUE (feed_id)
            );
        """)

        # Table storing OHLCV candle data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_candles(
                id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                transaction_id TEXT NOT NULL,
                feed_id TEXT NOT NULL REFERENCES market_feeds(feed_id),
                open NUMERIC(18,8) NOT NULL,
                high NUMERIC(18,8) NOT NULL,
                low NUMERIC(18,8) NOT NULL,
                close NUMERIC(18,8) NOT NULL,
                volume NUMERIC(18,8) NOT NULL,
                start_timestamp BIGINT NOT NULL,
                last_update_timestamp BIGINT NOT NULL,
                insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
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


# ----------------------------
# BACKGROUND DB WORKER
# ----------------------------

def db_worker(store, db):
    """
    Background loop that:
    - consumes buffered candles
    - writes them to PostgreSQL

    Intended to run in a separate thread.
    """

    while True:
        # Sleep to batch DB writes (reduces load)
        time.sleep(2)

        # Drain buffer queue
        while store.buffer:
            candle = store.buffer.popleft()
            db.insert_candle(candle)