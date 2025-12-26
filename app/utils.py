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
        self.query_existing_candlesticks = f"SELECT DISTINCT start_timestamp FROM market_candles WHERE start_timestamp > {self.lower_bound}"
        self.processed_candles = self.get_set_from_sql()[-self.window_size:]
        self.actual_slow_ma = None 
        self.actual_fast_ma = None

    def get_sql_engine(self):
        # return the connection string to the SQL database
        return create_engine(
            f"{self.driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    def get_set_from_sql(self):

        with self.get_sql_engine().connect() as conn:
            result = conn.execute(text(self.query_existing_candlesticks))
            my_set = set(row[0] for row in result)

        return sorted(my_set)
        

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
    
    def compute_mas(self):
        n = len(self.closed_closes)
        ma_fast = None
        ma_slow = None

        if n >= 50:
            last_ma_window = list(self.closed_closes)[-self.fast_ma:]
            ma_fast = sum(last_ma_window) / self.fast_ma

        if n >= 200:
            ma_slow = sum(self.closed_closes) / self.slow_ma
            
        return ma_fast, ma_slow

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
                        if self.data_type == "candlestick":
                            processed_candlesticks = get_set_from_sql()
                        msg = await ws.recv()
                        data = json.loads(msg)

                        self.logger.debug(
                            "Websocket message received",
                            extra={"event_id": transaction_id}
                        )

                        if self.data_type == "candlestick":
                            market_events = self.prepare_market_candles_dataframe(
                                data = data
                            )
                            sink_table = "market_candles"
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

def prepare_market_candles_dataframe(self, data):
    
    if "result" not in data or "data" not in data["result"]:
        return None
    else:
        df = pd.DataFrame(data["result"]["data"])
        df.rename(
            columns = {
                "o": "open"
                ,"h": "high"
                ,"l": "low"
                ,"c": "close"
                ,"v": "volume"
                ,"t": "start_timestamp"
                ,"ut": "last_update_timestamp"
            }
            ,inplace=True
        )

        df["open"] = pd.to_numeric(df["open"])
        df["high"] = pd.to_numeric(df["high"])
        df["low"] = pd.to_numeric(df["low"])
        df["close"] = pd.to_numeric(df["close"])
        df["volume"] = pd.to_numeric(df["volume"])
        df["srv_id"] = int(data["id"])
        df["method"] = str(data["method"])
        df["error_code"] = int(data["code"])
        df["instrument_name"] = str(data["result"]["instrument_name"])
        df["subscription"] = str(data["result"]["subscription"])
        df["channel"] = str(data["result"]["channel"])
        df["interval"] = str(data["result"]["interval"])

        return df