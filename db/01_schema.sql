-- =============================================================================
-- 0100: TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0110: market_candles
-- -----------------------------------------------------------------------------

CREATE TABLE market_candles(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,transaction_id TEXT NOT NULL
    ,feed_id TEXT NOT NULL REFERENCES market_feeds(feed_id)
    ,open NUMERIC(18,8) NOT NULL
    ,high NUMERIC(18,8) NOT NULL
    ,low NUMERIC(18,8) NOT NULL
    ,close NUMERIC(18,8) NOT NULL
    ,volume NUMERIC(18,8) NOT NULL
    ,start_timestamp BIGINT NOT NULL
    ,last_update_timestamp BIGINT NOT NULL
    ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
    ,CONSTRAINT uq_messages UNIQUE (transaction_id)
);



COMMENT ON TABLE market_candles IS
'this table contains candle events from the market.';

COMMENT ON COLUMN market_candles.id IS
'incremental record id.';

COMMENT ON COLUMN market_candles.transaction_id IS
'artificial uuid of the transaction.';

COMMENT ON COLUMN market_candles.feed_id IS
'identifier for the data feed.';

COMMENT ON COLUMN market_candles.instrument_name IS
'name of the crypto being scanned.';

COMMENT ON COLUMN market_candles.subscription IS
'type of subscription if available.';

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

-- -----------------------------------------------------------------------------
-- 0120: market_feeds
-- -----------------------------------------------------------------------------

CREATE TABLE market_feeds(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,feed_id TEXT NOT NULL
    ,srv_id INT NOT NULL
    ,symbol TEXT NOT NULL
    ,method TEXT NOT NULL
    ,hostname TEXT NOT NULL
    ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
    ,CONSTRAINT uq_feed UNIQUE (feed_id)
);

COMMENT ON TABLE market_feeds IS
'this table contains feed metadata for the websocket.';

COMMENT ON COLUMN market_feeds.id IS
'incremental record id.';

COMMENT ON COLUMN market_feeds.feed_id IS
'artificial uuid of the feed.';

COMMENT ON COLUMN market_feeds.srv_id IS
'internal request ID from the server.';

COMMENT ON COLUMN market_feeds.method IS
'websocket method that generated the message.';

COMMENT ON COLUMN market_feeds.symbol IS
'websocket symbol that generated the message.';

COMMENT ON COLUMN market_feeds.hostname IS
'the hostname of the machine that generated the message.';

COMMENT ON COLUMN market_feeds.insert_timestamp IS
'timestamp when the record was written in the table.';
