from utils import *

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if __name__ == "__main__":

    db = DBManager(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    ) 

    db.init_db()

    crypto_socket = CryptoSocket(
        endpoint="wss://stream.crypto.com/exchange/v1/market",

        subscribe_message={
            "id": 1,
            "method": "subscribe",
            "params": {
                "channels": [
                    "candlestick.1m.BTC_USD"
                ]
            }
        }

        ,db=db
    )

    db.insert_feed(
        crypto_socket.feed_id,
        crypto_socket.srv_id,
        crypto_socket.symbol,
        crypto_socket.method,
        crypto_socket.hostname
    )

    threading.Thread(
        target=db_worker,
        args=(crypto_socket.store, db),
        #daemon=True
    ).start()


    #crypto_socket.run()
    thread_run = threading.Thread(
        target=crypto_socket.run,
        #daemon=True
    )
    
    thread_run.start()