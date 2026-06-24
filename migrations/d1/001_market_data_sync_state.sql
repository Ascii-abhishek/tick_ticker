-- Stores market data sync state separately from symbol reference data.
-- equity_symbol_reference should remain focused on symbol identity fields.

CREATE TABLE IF NOT EXISTS market_data_sync_state (
    market_type TEXT NOT NULL,
    nse_symbol TEXT NOT NULL,
    status TEXT NOT NULL,
    from_date TEXT,
    to_date TEXT,
    row_count INTEGER,
    local_path TEXT,
    r2_prefix TEXT,
    error TEXT,
    started_at TEXT,
    completed_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (market_type, nse_symbol)
);

CREATE INDEX IF NOT EXISTS idx_market_data_sync_state_status
ON market_data_sync_state (market_type, status, nse_symbol);
