-- =============================================================================
-- 0100: TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0110: market_candles
-- -----------------------------------------------------------------------------

CREATE TABLE market_candles(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,transaction_id TEXT NOT NULL
    ,srv_id INT NOT NULL
    ,method TEXT NOT NULL
    ,error_code INT NOT NULL
    ,instrument_name TEXT NOT NULL
    ,subscription TEXT NOT NULL
    ,channel TEXT NOT NULL
    ,interval TEXT NOT NULL
    ,open NUMERIC(18,8) NOT NULL
    ,high NUMERIC(18,8) NOT NULL
    ,low NUMERIC(18,8) NOT NULL
    ,close NUMERIC(18,8) NOT NULL
    ,volume NUMERIC(18,8) NOT NULL
    ,start_timestamp BIGINT NOT NULL
    ,last_update_timestamp BIGINT NOT NULL
    ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);



COMMENT ON TABLE market_candles IS
'this table contains candle events from the market.';

COMMENT ON COLUMN market_candles.id IS
'incremental record id.';

COMMENT ON COLUMN market_candles.transaction_id IS
'artificial uuid of the transaction.';

COMMENT ON COLUMN market_candles.srv_id IS
'internal request ID from the server.';

COMMENT ON COLUMN market_candles.method IS
'websocket method that generated the message.';

COMMENT ON COLUMN market_candles.code IS
'error code for the message.';

COMMENT ON COLUMN market_candles.instrument_name IS
'name of the crypto being scanned.';

COMMENT ON COLUMN market_candles.subscription IS
'type of subscription if available.';

COMMENT ON COLUMN market_candles.channel IS
'type of data retrieved by the websocket.';

COMMENT ON COLUMN market_candles.interval IS
'time interval buckt of the candle.';

COMMENT ON COLUMN market_candles.open IS
'the first traded price when the candle starts.';

COMMENT ON COLUMN market_candles.high IS
'the highest traded price during the interval.';

COMMENT ON COLUMN market_candles.low IS
'the lowest traded price during the interval.';

COMMENT ON COLUMN market_candles.close IS
'the last traded price before the candle closed.';

COMMENT ON COLUMN market_candles.volume IS
'volume traded during the candle.';

COMMENT ON COLUMN market_candles.start_timestamp IS
'candle start time in milliseconds.';

COMMENT ON COLUMN market_candles.last_update_timestamp IS
'last update time for this candle in milliseconds.';

COMMENT ON COLUMN market_candles.insert_timestamp IS
'timestamp when the record was written in the table.';