import pandas as pd 
import json
from pandasql import sqldf
from urllib.parse import quote_plus
from time import sleep, time
from sqlalchemy import create_engine
import asyncio
import websockets
from uuid import uuid4
from config.logging import *


class SimulationBot:
    def __init__(self, message_metadata):
        self.ws_url = message_metadata["end_point"]
        self.subscribe_msg = message_metadata["subscribe_msg"]
        self.data_type = message_metadata["data_type"]

        # setup logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # database connection
        self.driver = "postgresql+psycopg2"
        self.db_user = os.getenv("DB_USER")
        self.db_password = quote_plus(os.getenv("DB_PASSWORD"))
        self.db_host = os.getenv("DB_HOST", "db")
        self.db_port = os.getenv("DB_PORT", 5432)
        self.db_name = os.getenv("DB_NAME")
        self.engine = self.get_sql_engine()

        # get a set of already procesed candles from what was previously written in the SQL database
        self.slow_ma = message_metadata["slow_ma"]
        self.fast_ma = message_metadata["fast_ma"]
        self.window_size = message_metadata["window_size"]
        self.lower_bound = message_metadata["lower_bound"]
        self.position = message_metadata["position"]
        self.entry_price = message_metadata["entry_price"]
        self.balance = message_metadata["balance"]
        self.initial_balance = message_metadata["balance"]
        self.balance_btc = message_metadata["balance_btc"]
        self.fee = message_metadata["fee"]
        self.candles = OrderedDict()
        self.trade_log = []

    def get_sql_engine(self):
        # return the connection string to the SQL database
        return create_engine(
            f"{self.driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )        
    
    def add_moving_averages(self, df):
        df["ma_fast"] = df["close"].rolling(self.fast_ma).mean()
        df["ma_slow"] = df["close"].rolling(self.slow_ma).mean()
        return df
    
    def check_signal(self, df):
        if len(df) < (self.slow_ma + 1):
            return None

        prev = df.iloc[-2]
        curr = df.iloc[-1]

        # Golden cross
        if prev.ma_fast < prev.ma_slow and curr.ma_fast > curr.ma_slow:
            return "buy"

        # Death cross
        if prev.ma_fast > prev.ma_slow and curr.ma_fast < curr.ma_slow:
            return "sell"

        return None
    
    def execute_buy(self, price, timestamp):
        if self.balance <= 0:
            self.logger.info(f"Not enough liquidity to buy. Current liquidity: {self.balance}")
            return

        qty = self.balance / price
        fee = qty * self.fee

        self.balance_btc = qty - fee
        self.balance = 0.0
        self.entry_price = price

        self.trade_log.append({
            "type": "BUY",
            "price": price,
            "btc": self.balance_btc,
            "cash": self.balance,
            "t": timestamp
        })

    def execute_sell(self, price, timestamp):
        if self.balance_btc <= 0:
            self.logger.info(f"Not enough assets to sell. Current assets: {self.balance_btc}")
            return

        proceeds = self.balance_btc * price
        fee = proceeds * self.fee

        self.balance = proceeds - fee
        self.balance_btc = 0.0
        self.entry_price = None

        self.trade_log.append({
            "type": "SELL",
            "price": price,
            "cash": self.balance,
            "pnl": self.balance - self.initial_balance,
            "t": timestamp
        })

    def unrealized_pnl(self, current_price):
        if self.balance_btc == 0:
            return 0.0
        return self.balance_btc * (current_price - self.entry_price)

    async def heartbeat(self, ws, interval):
        # define a heartbeat function to keep the websocket connection open
        # this is because servers will close the connection after a while if you just listen but do not respond
        while True:
            try:
                # send "ping" as most servers count it as client activity
                await ws.send("ping")  
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning(f"Heartbeat error: {e}")
                break


    async def connect(self):
        self.logger.info("Opening websocket connection")

        while True:
            try:
                # Open websocket connection using async context manager
                async with websockets.connect(
                    self.ws_url
                ) as ws:
                    await ws.send(
                        json.dumps(
                            self.subscribe_msg
                        )
                    )
                    self.logger.info("Subscription message sent")

                    # Start heartbeat in the background
                    hb_task = asyncio.create_task(
                        self.heartbeat(
                            ws = ws
                            ,interval = 30
                        )
                    )

                    while True:
                        transaction_id = str(uuid4())
                        msg = await ws.recv()
                        data = json.loads(msg)

                        self.logger.debug(
                            "Websocket message received",
                            extra={"event_id": transaction_id}
                        )

                        if self.data_type == "candlestick":
                            market_events = self.candles_to_df(
                                msg = data
                            )
                            market_events["transaction_id"] = transaction_id
                            market_events = add_moving_averages(market_events)
                            sink_table = "market_candles"

                            signal = self.check_signal(market_events)
                            last_price = market_events.iloc[-1].close
                            last_t = market_events.index[-1]

                            if signal == "buy" and self.balance_btc == 0:
                                self.execute_buy(last_price, last_t)

                            elif signal == "sell" and self.balance_btc > 0:
                                self.execute_sell(last_price, last_t)
                        else:
                            market_events = self.prepare_market_events_dataframe(
                                data = data
                            )
                            sink_table = "market_events"

                        if market_events is None or market_events.empty:
                            self.logger.debug(
                                "Message did not contain any data",
                                extra={"event_id": transaction_id}
                            )
                        else:
                            market_events["transaction_id"] = transaction_id

                            self.logger.info(
                                "Writing market events to database...",
                                extra={"event_id": transaction_id, "rows": len(market_events)}
                            )

                            market_events.drop(columns=["raw_message"]).to_sql(
                                sink_table,
                                con=self.engine,
                                if_exists="append",
                                index=False,
                                method="multi"
                            )

                            market_events["end_point"] = str(self.ws_url)

                            market_events[["transaction_id", "raw_message", "end_point"]].to_sql(
                                "raw_market_events",
                                con=self.engine,
                                if_exists="append",
                                index=False,
                                method="multi"
                            )

                            self.logger.info(
                                "Database write completed",
                                extra={"event_id": transaction_id}
                            )
            # log potential errors and wait some seconds before attempting to reconnect
            except Exception as e:
                self.logger.error(f"Websocket connection error: {e}")
                self.logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

            finally:
                # Cancel heartbeat task when websocket closes or errors
                if 'hb_task' in locals():
                    hb_task.cancel()
                    try:
                        await hb_task
                    except asyncio.CancelledError:
                        pass


    def prepare_market_events_dataframe(self, data):

        if "result" not in data or "data" not in data["result"]:
            return None

        items_list = []
        for item in range(len(data["result"]["data"])):
            dict_data = {}
            dict_data["srv_id"] = int(data["id"])
            dict_data["method"] = str(data["method"])
            dict_data["error_code"] = int(data["code"])
            dict_data["instrument_name"] = str(data["result"]["instrument_name"])
            dict_data["subscription"] = str(data["result"]["subscription"])
            dict_data["channel"] = str(data["result"]["channel"])
            dict_data["high"] = float(data["result"]["data"][item]["h"])
            dict_data["low"] = float(data["result"]["data"][item]["l"])
            dict_data["ask"] = float(data["result"]["data"][item]["a"])
            dict_data["price_change"] = float(data["result"]["data"][item]["c"])
            dict_data["bid"] = float(data["result"]["data"][item]["b"])
            dict_data["bid_size"] = float(data["result"]["data"][item]["bs"])
            dict_data["last_price"] = float(data["result"]["data"][item]["k"])
            dict_data["ask_size"] = float(data["result"]["data"][item]["ks"])
            dict_data["volume"] = float(data["result"]["data"][item]["v"])
            dict_data["quote_volume"] = float(data["result"]["data"][item]["vv"])
            dict_data["open_interest"] = int(data["result"]["data"][item]["oi"])
            dict_data["exchange_timestamp"] = int(data["result"]["data"][item]["t"])
            dict_data["raw_message"] = str(data)
            items_list.append(dict_data)
        return pd.DataFrame(items_list)
    
    def candles_to_df(self, msg):
        msg_data = msg["result"]["data"]

        for candle in msg_data:
            t = candle["t"]

            self.candles[t] = {
                "open": float(candle["o"]),
                "high": float(candle["h"]),
                "low": float(candle["l"]),
                "close": float(candle["c"]),
                "volume": float(candle["v"]),
                "t": int(t),
                "ut": int(candle["ut"])
            }
        candles.move_to_end(t) 
        df = pd.DataFrame(self.candles.values())
        df["srv_id"] = msg["id"]
        df["method"] = msg["method"]
        df["error_code"] = msg["code"]
        df["instrument_name"] = msg["result"]["instrument_name"]
        df["subscription"] = msg["result"]["subscription"]
        df["channel"] = msg["result"]["channel"]
        df["interval"] = msg["result"]["interval"]
        df["start_timestamp"] = df["t"]
        df = df[df['t'] > self.lower_bound]
        df = df.rename(columns={'ut': 'last_update_timestamp'})
        df = df.sort_values("t")
        df.set_index("t", inplace=True)
        return df