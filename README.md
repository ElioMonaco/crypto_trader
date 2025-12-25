# crypto_trader
My bot for trading in crypto

## Version

Current version: **v0.2.0**


## Changelog
### v0.2.0 — 2025-12-25
- added candlestick support

### v0.1.3 — 2025-12-25
- added error handling for closed connections in websocket
- added heartbeat to keep connection open

### v0.1.2 — 2025-12-25
- Refactored utilities into `SimulationBot` class
- Added TRADING_MODE environment variable to switch between simulation and real trading
- fixed problem with database password parsing