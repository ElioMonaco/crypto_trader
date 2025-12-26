CLUSTER market_events USING idx_exchange_timestamp;
ANALYZE market_events;

CLUSTER raw_market_events USING idx_insert_timestamp;
ANALYZE raw_market_events;

CLUSTER market_candles USING idx_start_timestamp;
ANALYZE market_candles;