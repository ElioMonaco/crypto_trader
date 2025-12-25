CREATE TABLE market_events(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,transaction_id TEXT NOT NULL
    ,srv_id INT NOT NULL
    ,method TEXT NOT NULL
    ,error_code INT NOT NULL
    ,instrument_name TEXT NOT NULL
    ,subscription TEXT NOT NULL
    ,channel TEXT NOT NULL
    ,high NUMERIC(18,8)
    ,low NUMERIC(18,8)
    ,ask NUMERIC(18,8) NOT NULL
    ,price_change NUMERIC(8,6)
    ,bid NUMERIC(18,8) NOT NULL
    ,bid_size NUMERIC(18,8)
    ,last_price NUMERIC(18,8) NOT NULL
    ,ask_size NUMERIC(18,8)
    ,volume NUMERIC(18,8)
    ,quote_volume NUMERIC(18,2)
    ,open_interest BIGINT
    ,exchange_timestamp BIGINT NOT NULL
    ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);



COMMENT ON TABLE market_events IS
'this table contains all events that I decide to record directly from what I receive from the websocket.';

COMMENT ON COLUMN market_events.transaction_id IS
'artificial uuid of the transaction.';

COMMENT ON COLUMN market_events.srv_id IS
'internal request ID from the server.';

COMMENT ON COLUMN market_events.method IS
'websocket method that generated the message.';

COMMENT ON COLUMN market_events.code IS
'error code for the message.';

COMMENT ON COLUMN market_events.instrument_name IS
'name of the crypto being scanned.';

COMMENT ON COLUMN market_events.subscription IS
'type of subscription if available.';

COMMENT ON COLUMN market_events.channel IS
'type of data retrieved by the websocket.';

COMMENT ON COLUMN market_events.high IS
'24h high price.';

COMMENT ON COLUMN market_events.low IS
'24h low price.';

COMMENT ON COLUMN market_events.ask IS
'best ask price, used for realistic buy fills.';

COMMENT ON COLUMN market_events.price_change IS
'24h price change ratio.';

COMMENT ON COLUMN market_events.bid IS
'best bid price, used for realistic buy fills.';

COMMENT ON COLUMN market_events.bid_size IS
'size available at best bid.';

COMMENT ON COLUMN market_events.last_price IS
'last traded price.';

COMMENT ON COLUMN market_events.ask_size IS
'size available at best ask.';

COMMENT ON COLUMN market_events.volume IS
'24h base asset volume.';

COMMENT ON COLUMN market_events.quote_volume IS
'24h quote asset volume, total traded value of the quote currency.';

COMMENT ON COLUMN market_events.open_interest IS
'otal number of contracts that are currently open in derivatives markets.';

COMMENT ON COLUMN market_events.exchange_timestamp IS
'exchange timestamp in milliseconds.';

COMMENT ON COLUMN market_events.insert_timestamp IS
'timestamp when the record was written in the table.';



CREATE TABLE raw_market_events(
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,transaction_id TEXT NOT NULL
    ,raw_message TEXT NOT NULL
    ,insert_timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);



COMMENT ON TABLE raw_market_events IS
'this table contains all the raw messages from the websocket that will eventually populate the "market_events" table.';

COMMENT ON COLUMN raw_market_events.transaction_id IS
'artificial uuid of the transaction.';

COMMENT ON COLUMN raw_market_events.raw_message IS
'raw JSON message received from the websocket.';

COMMENT ON COLUMN raw_market_events.insert_timestamp IS
'timestamp when the record was written in the table.';