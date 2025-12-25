import pandas as pd 
import json
from pandasql import sqldf
from urllib.parse import quote_plus
from time import sleep
from sqlalchemy import create_engine
import asyncio
import websockets
from uuid import uuid4
from config.logging import *


class SimulationBot:
    def __init__(self, ws_url, subscribe_msg):
        self.ws_url = ws_url
        self.subscribe_msg = subscribe_msg

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

    def get_sql_engine(self):
        # return the connection string to the SQL database
        return create_engine(
            f"{self.driver}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        

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

                        market_events = self.prepare_market_events_dataframe(
                            data = data
                        )

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
                                "market_events",
                                con=self.engine,
                                if_exists="append",
                                index=False,
                                method="multi"
                            )

                            market_events[["transaction_id", "raw_message"]].to_sql(
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