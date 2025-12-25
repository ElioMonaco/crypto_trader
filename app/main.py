from utils import *

WS_URL = "wss://stream.crypto.com/v2/market"

subscribe_msg = {
    "id": 1,
    "method": "subscribe",
    "params": {
        "channels": ["ticker.BTC_USDT"]
    }
}

async def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Application starting")
    logger.info("Connecting to Crypto.com websocket")

    await connect(WS_URL, subscribe_msg)

if __name__ == "__main__":
    asyncio.run(main())