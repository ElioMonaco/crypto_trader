-- =============================================================================
-- 0100: INDEXES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0110: market_candles
-- -----------------------------------------------------------------------------

CREATE UNIQUE INDEX IF NOT EXISTS idx_transaction_id ON market_candles(transaction_id);
CREATE INDEX IF NOT EXISTS idx_instrument_name ON market_candles(instrument_name);
CREATE INDEX IF NOT EXISTS idx_start_timestamp ON market_candles(start_timestamp);

-- -----------------------------------------------------------------------------
-- 0120: bot_signals
-- -----------------------------------------------------------------------------


CREATE UNIQUE INDEX IF NOT EXISTS uq_bot_signals_trigger ON bot_signals (feed_id, trigger_candle_ts, direction);