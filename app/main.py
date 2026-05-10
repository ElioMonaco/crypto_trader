# Import everything from local utilities module
from utils import *

# ----------------------------
# ENVIRONMENT CONFIGURATION
# ----------------------------

# Load database configuration from environment variables
# This allows secure configuration without hardcoding credentials

DB_HOST = os.getenv("DB_HOST")               # PostgreSQL host address
DB_PORT = os.getenv("DB_PORT", "5432")       # Default PostgreSQL port if not set
DB_NAME = os.getenv("DB_NAME")               # Database name
DB_USER = os.getenv("DB_USER")               # Database username
DB_PASSWORD = os.getenv("DB_PASSWORD")       # Database password


# ----------------------------
# MAIN ENTRY POINT
# ----------------------------

if __name__ == "__main__":
    """
    Entry point for the application.

    Responsibilities:
    - Initialize DB connection
    - Create schema if needed
    - Start WebSocket feed
    - Start DB worker thread (async persistence)
    """

    # ----------------------------
    # DATABASE INITIALIZATION
    # ----------------------------

    db = DBManager(
        host=DB_HOST,         # DB server address
        port=DB_PORT,         # DB port (default 5432)
        database=DB_NAME,     # DB name
        user=DB_USER,         # DB user
        password=DB_PASSWORD   # DB password
    )

    # Create required tables if they do not already exist
    db.init_db()


    # ----------------------------
    # WEBSOCKET INITIALIZATION
    # ----------------------------

    crypto_socket = CryptoSocket(
        endpoint="wss://stream.crypto.com/exchange/v1/market",

        # Subscription message defining:
        # - method: subscribe
        # - channel: candlestick stream for BTC_USD at 1 minute interval
        subscribe_message={
            "id": 1,
            "method": "subscribe",
            "params": {
                "channels": [
                    "candlestick.1m.BTC_USD"
                ]
            }
        },

        # Pass DB manager so socket can share feed metadata context
        db=db
    )


    # ----------------------------
    # REGISTER FEED IN DATABASE
    # ----------------------------

    # Inserts metadata about this WebSocket feed into DB
    # This ensures the feed is tracked and linked to candle data
    db.insert_feed(
        crypto_socket.feed_id,   # Unique feed identifier (UUID)
        crypto_socket.srv_id,    # Server-side subscription ID
        crypto_socket.symbol,    # Trading pair (BTC_USD)
        crypto_socket.method,    # Subscription method (subscribe)
        crypto_socket.hostname   # Machine hostname
    )


    # ----------------------------
    # START DATABASE WORKER THREAD
    # ----------------------------

    # This thread continuously:
    # - reads closed candles from in-memory buffer
    # - writes them into PostgreSQL asynchronously
    threading.Thread(
        target=db_worker,  # function that processes candle buffer
        args=(crypto_socket.store, db),  # shared candle store + DB connection
        # daemon=True  # (commented out) would make thread exit with main program
    ).start()


    # ----------------------------
    # START WEBSOCKET THREAD
    # ----------------------------

    # Option 1 (disabled): run WebSocket in main thread
    # crypto_socket.run()

    # Option 2 (active): run WebSocket in separate thread
    # This allows DB worker to run concurrently
    thread_run = threading.Thread(
        target=crypto_socket.run,
        # daemon=True  # (commented out) ensures thread does not block shutdown
    )

    # Start WebSocket streaming thread
    thread_run.start()