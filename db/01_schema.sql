-- =============================================================================
-- 0100: TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0110: market_candles
-- -----------------------------------------------------------------------------

CREATE TABLE market_candles(
    id                      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,transaction_id         UUID NOT NULL
    ,feed_id                UUID NOT NULL REFERENCES market_feeds(feed_id)
    ,open                   NUMERIC(18,8) NOT NULL
    ,high                   NUMERIC(18,8) NOT NULL
    ,low                    NUMERIC(18,8) NOT NULL
    ,close                  NUMERIC(18,8) NOT NULL
    ,volume                 NUMERIC(18,8) NOT NULL
    ,start_timestamp        BIGINT NOT NULL
    ,last_update_timestamp  BIGINT NOT NULL
    ,insert_timestamp       TIMESTAMPTZ NOT NULL DEFAULT now()
    ,CONSTRAINT uq_candle   UNIQUE (feed_id, start_timestamp)
);



COMMENT ON TABLE market_candles IS
'this table contains candle events from the market.';

COMMENT ON COLUMN market_candles.id IS
'incremental record id.';

COMMENT ON COLUMN market_candles.transaction_id IS
'artificial uuid of the transaction.';

COMMENT ON COLUMN market_candles.feed_id IS
'identifier for the data feed.';

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
    id                  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,feed_id            UUID NOT NULL
    ,srv_id             INT NOT NULL
    ,symbol             TEXT NOT NULL
    ,method             TEXT NOT NULL
    ,hostname           TEXT NOT NULL
    ,insert_timestamp   TIMESTAMPTZ NOT NULL DEFAULT now()
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


-- -----------------------------------------------------------------------------
-- 0130: bot_signals
-- -----------------------------------------------------------------------------

CREATE TABLE bot_signals(
    id                          INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    ,signal_id                  UUID NOT NULL
    ,feed_id                    UUID NOT NULL REFERENCES market_feeds(feed_id)
    ,direction                  TEXT NOT NULL
    ,sweep_type                 TEXT NOT NULL
    ,entry                      NUMERIC(18,8) NOT NULL
    ,stop_loss                  NUMERIC(18,8) NOT NULL
    ,take_profit                NUMERIC(18,8) NOT NULL
    ,risk_reward                NUMERIC(6,2)  NOT NULL
    ,reference_candle_ts        BIGINT NOT NULL
    ,trigger_candle_ts          BIGINT NOT NULL
    ,lookback                   SMALLINT NOT NULL
    ,sweep_buffer               NUMERIC(8,5) NOT NULL
    ,min_rr                     NUMERIC(6,2) NOT NULL
    ,outcome                    TEXT
    ,close_price                NUMERIC(18,8)
    ,close_timestamp            BIGINT
    ,pnl_r                      NUMERIC(8,4)
    ,insert_timestamp           TIMESTAMPTZ NOT NULL DEFAULT now()
    ,CONSTRAINT uq_signal       UNIQUE (signal_id)
    ,CONSTRAINT chk_direction   CHECK (direction  IN ('BUY',  'SELL'))
    ,CONSTRAINT chk_sweep_type  CHECK (sweep_type IN ('high_sweep', 'low_sweep'))
    ,CONSTRAINT chk_outcome     CHECK (outcome    IN ('WIN', 'LOSS', 'BREAKEVEN') OR outcome IS NULL)
    ,CONSTRAINT fk_reference_candle
        FOREIGN KEY (feed_id, reference_candle_ts)
        REFERENCES market_candles(feed_id, start_timestamp)
    ,CONSTRAINT fk_trigger_candle
        FOREIGN KEY (feed_id, trigger_candle_ts)
        REFERENCES market_candles(feed_id, start_timestamp)
);

COMMENT ON TABLE bot_signals IS
'this table contains trade signals generated by the bot.';

COMMENT ON COLUMN bot_signals.id IS
'incremental record id.';

COMMENT ON COLUMN bot_signals.signal_id IS
'artificial uuid of the signal.';

COMMENT ON COLUMN bot_signals.feed_id IS
'foreign key referencing the market feed.';

COMMENT ON COLUMN bot_signals.direction IS
'BUY or SELL.';

COMMENT ON COLUMN bot_signals.sweep_type IS
'which side of the reference candle was swept.';

COMMENT ON COLUMN bot_signals.entry IS
'suggested entry price; close of the trigger candle.';

COMMENT ON COLUMN bot_signals.stop_loss IS
'stop loss level; placed just beyond the sweep wick.';

COMMENT ON COLUMN bot_signals.take_profit IS
'take profit target; opposite end of the reference candle.';

COMMENT ON COLUMN bot_signals.risk_reward IS
'ratio of potential reward to risk: (TP - entry) / (entry - SL).';

COMMENT ON COLUMN bot_signals.reference_candle_ts IS
'start_timestamp (ms epoch) of the anchor reference candle. Defines the high/low range that CRT trades around';

COMMENT ON COLUMN bot_signals.trigger_candle_ts IS
'start_timestamp (ms epoch) of the sweep candle. The candle whose wick broke the range and closed back inside';

COMMENT ON COLUMN bot_signals.lookback IS
'number of candles scanned to identify the reference candle.';

COMMENT ON COLUMN bot_signals.sweep_buffer IS
'fractional tolerance used to confirm a sweep (e.g. 0.001 = 0.1%). Higher values require a more decisive break of the level';

COMMENT ON COLUMN bot_signals.min_rr IS
'minimum risk/reward ratio required to emit a signal.';

COMMENT ON COLUMN bot_signals.outcome IS
'WIN, LOSS, or BREAKEVEN populated by reconciliation process.';

COMMENT ON COLUMN bot_signals.close_price IS
'actual price at which the trade was closed.';

COMMENT ON COLUMN bot_signals.close_timestamp IS
'candle start_timestamp (ms epoch) when trade closed. NULL until outcome is determined';

COMMENT ON COLUMN bot_signals.pnl_r IS
'profit or loss expressed in R-multiples (1R = initial risk). NULL until outcome is determined';

COMMENT ON COLUMN bot_signals.insert_timestamp IS
'timestamp when the record was written in the table.';