from utils import *

# determine which bot you want to run, either simulation or real.
mode = os.getenv("TRADING_MODE", "simulation").lower()

# define what websocket subscribe to, what type of data to process and where to get it to instantiate the bot.
message_metadata = {
    "end_point": "wss://stream.crypto.com/v2/market"
    ,"data_type": "candlestick"
    ,"subscribe_msg": {
        "id": 1,
        "method": "subscribe",
        "params": {
            "channels": ["ticker.BTC_USDT"]
        }
    }
    # determine the size of the slow moving average, fast moving average and total dataset windows.
    ,"slow_ma": 200
    ,"fast_ma": 50
    ,"window_size": 1000
    # start by only considering candlesticks from an hiur ago
    ,"lower_bound": int((time() - 3600) * 1000)
}

async def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Application starting")
    
    if mode == "simulation":
        logger.info("Running in simulation mode")

        # instatiate the simulation class
        bot = SimulationBot(message_metadata)

    else:
        logger.info("Running in real trading mode (not implemented yet)")
        return

    logger.info("Connecting to Crypto.com websocket")
    await bot.connect()

if __name__ == "__main__":
    asyncio.run(main())