CREATE UNIQUE INDEX idx_transaction_id ON market_events(transaction_id);
CREATE INDEX idx_instrument_name ON market_events(instrument_name);
CREATE INDEX idx_exchange_timestamp ON market_events(exchange_timestamp);

CREATE UNIQUE INDEX idx_transaction_id ON raw_market_events(transaction_id);
CREATE INDEX idx_insert_timestamp ON raw_market_events(insert_timestamp);

CREATE UNIQUE INDEX idx_transaction_id ON market_candles(transaction_id);
CREATE INDEX idx_instrument_name ON market_candles(instrument_name);
CREATE INDEX idx_start_timestamp ON market_candles(start_timestamp);