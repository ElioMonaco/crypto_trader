-- =============================================================================
-- 0100: INDEXES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0110: market_candles
-- -----------------------------------------------------------------------------

CREATE UNIQUE INDEX idx_transaction_id ON market_candles(transaction_id);
CREATE INDEX idx_instrument_name ON market_candles(instrument_name);
CREATE INDEX idx_start_timestamp ON market_candles(start_timestamp);