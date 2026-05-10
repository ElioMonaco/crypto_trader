import websocket
import socket
import os
import json
import time
from dataclasses import dataclass
import pandas as pd
from uuid import uuid4
import threading
import psycopg2
from collections import deque

@dataclass
class Candle:
    transaction_id: str
    feed_id: str
    symbol: str
    interval: str
    method: str
    srv_id: int
    hostname: str

    start_timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    last_update_timestamp: int

def parse_candles(msg: dict, transaction_id: str, feed_id: str, symbol: str, interval: str, method: str, srv_id: int, hostname: str):
    data = msg.get("result", {}).get("data", [])

    return [
        Candle(
            transaction_id=transaction_id,
            feed_id=feed_id,
            symbol=symbol,
            interval=interval,
            method=method,
            srv_id=srv_id,
            hostname=hostname,
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

class CandleStore:
    def __init__(self, feed_id, symbol, interval, method, srv_id, db, hostname):
        self.feed_id = feed_id
        self.symbol = symbol
        self.interval = interval
        self.method = method
        self.srv_id = srv_id
        self.db = db
        self.hostname = hostname

        self.history = []
        self.buffer = deque()
        self.latest = None

    def update(self, msg: dict, transaction_id: str):
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

            if self.latest is None:
                self.latest = c
                continue

            if c.start_timestamp == self.latest.start_timestamp:
                self.latest = c
                continue

            if c.start_timestamp > self.latest.start_timestamp:
                closed = self.latest

                self.history.append(closed)
                self.buffer.append(closed)

                self.latest = c

    def to_dataframe(self):
        all_candles = self.history[:]
        if self.latest:
            all_candles.append(self.latest)

        df = pd.DataFrame([c.__dict__ for c in all_candles])
        #df["start_timestamp"] = pd.to_datetime(df["start_timestamp"], unit="ms")
        return df

class CryptoSocket:

    def __init__(self, endpoint, subscribe_message, db):
        self.endpoint = endpoint
        self.subscribe_message = subscribe_message
        self.ws = None
        self.db = db

        self.feed_id = str(uuid4())
        self.hostname = socket.gethostname()

        channel = subscribe_message["params"]["channels"][0]
        parts = channel.split(".")

        self.interval = parts[1]
        self.symbol = parts[2]
        self.method = subscribe_message["method"]
        self.srv_id = subscribe_message["id"]

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
        print("Connected")
        ws.send(json.dumps(self.subscribe_message))

    def on_message(self, ws, message):
        transaction_id = str(uuid4())
        data = json.loads(message)
        self.store.update(data, transaction_id)

    def on_close(self, ws, close_status_code, close_msg):
        print("Disconnected")

    def on_error(self, ws, error):
        print("Error:", error)

    def run(self):
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    self.endpoint,
                    on_open=self.on_open,
                    on_message=self.on_message,
                    on_close=self.on_close,
                    on_error=self.on_error
                )

                self.ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10
                )

            except Exception as e:
                print("Reconnect due to:", e)

            time.sleep(3)


class DBManager:
    def __init__(self, host, port, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def init_db(self):
        cur = self.conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_feeds(
                id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                ,feed_id TEXT NOT NULL
                ,srv_id INT NOT NULL
                ,symbol TEXT NOT NULL
                ,method TEXT NOT NULL
                ,hostname TEXT NOT NULL
                ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
                ,CONSTRAINT uq_feed UNIQUE (feed_id)
            );
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_candles(
                id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                ,transaction_id TEXT NOT NULL
                ,feed_id TEXT NOT NULL REFERENCES market_feeds(feed_id)
                ,open NUMERIC(18,8) NOT NULL
                ,high NUMERIC(18,8) NOT NULL
                ,low NUMERIC(18,8) NOT NULL
                ,close NUMERIC(18,8) NOT NULL
                ,volume NUMERIC(18,8) NOT NULL
                ,start_timestamp BIGINT NOT NULL
                ,last_update_timestamp BIGINT NOT NULL
                ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
                ,CONSTRAINT uq_messages UNIQUE (transaction_id)
            );
        """)

        self.conn.commit()
        cur.close()

    def insert_candle(self, candle):
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
    
def db_worker(store, db):
    while True:
        time.sleep(2)
        while store.buffer:
            candle = store.buffer.popleft()
            db.insert_candle(candle)