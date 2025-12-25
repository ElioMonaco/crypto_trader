from utils import *

WS_URL = "wss://stream.crypto.com/v2/market"

subscribe_msg = {
    "id": 1,
    "method": "subscribe",
    "params": {
        "channels": ["ticker.BTC_USDT"]
    }
}

mode = os.getenv("TRADING_MODE", "simulation").lower()

async def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Application starting")
    mode = os.getenv("TRADING_MODE", "simulation").lower()
    
    if mode == "simulation":
        logger.info("Running in simulation mode")
        bot = SimulationBot(WS_URL, subscribe_msg)
    else:
        logger.info("Running in real trading mode (not implemented yet)")
        return

    logger.info("Connecting to Crypto.com websocket")
    await bot.connect()

if __name__ == "__main__":
    asyncio.run(main())