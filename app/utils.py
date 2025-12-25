import pandas as pd 
import json
from pandasql import sqldf
import os
from time import sleep
from sqlalchemy import create_engine
import asyncio
import websockets
from uuid import uuid4

def get_sql_engine(driver):
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", 5432)
    DB_NAME = os.getenv("DB_NAME")

    return create_engine(
        f"{driver}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

async def connect(msg):
    async with websockets.connect(WS_URL) as ws:
        subscribe_msg = msg
        await ws.send(json.dumps(subscribe_msg))

        while True:
            transaction_id = str(uuid4())
            msg = await ws.recv()
            data = json.loads(msg)
            market_events = prepare_market_events_dataframe(data)
            market_events["transaction_id"] = transaction_id

            print(f"writing dataframe for transaction {transaction_id} ...")
            market_events.drop(columns=['raw_message']).to_sql(
                "market_events",   
                con = get_sql_engine("postgresql+psycopg2"),
                if_exists = "append",       
                index = False,             
                method = "multi"            
            )

            market_events[['transaction_id', 'raw_message']].to_sql(
                "raw_market_events",   
                con = get_sql_engine("postgresql+psycopg2"),
                if_exists = "append",       
                index = False,             
                method = "multi"            
            )

def prepare_market_events_dataframe(data):
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


# def get_items_by_collection(coll_name, headers):
#     coll_df = pd.DataFrame(
#         requests.get(
#             "https://api.cardtrader.com/api/v2/expansions"
#             ,headers = headers
#         ).json()
#     )
#     query = """SELECT DISTINCT id, name FROM coll_df WHERE name LIKE '%{}%'""".format(coll_name)
#     return sqldf(query)

def get_expansion_id_listing(exp_id, headers):
    output = []
    response = requests.get(
        "https://api.cardtrader.com/api/v2/marketplace/products?expansion_id={}".format(exp_id)
        ,headers = headers
    ).json()
    for key, value in response.items():
        dict_data = {}   
        for item in value:
            dict_data["expansion_id"] = exp_id
            dict_data["expansion_name"] = item["expansion"]["name_en"]
            dict_data["blueprint_id"] = key
            dict_data["product_id"] = item["id"]
            dict_data["product_name"] = item["name_en"]
            dict_data["product_quantity"] = item["quantity"]
            dict_data["product_price_cents"] = item["price_cents"]
            dict_data["product_currency"] = item["price_currency"]
            dict_data["product_description"] = item["description"]
            dict_data["user_id"] = item["user"]["id"]
            dict_data["user_name"] = item["user"]["username"]
            dict_data["user_country"] = item["user"]["country_code"]
            dict_data["insert_timestamp"] = pd.Timestamp.utcnow()
            output.append(dict_data)
    return pd.DataFrame(output)