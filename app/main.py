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
    await connect()

if __name__ == "__main__":
    asyncio.run(main())