-- Point-in-time security identity, provider mappings, corporate actions, index
-- membership, and synthetic index volume state.
--
-- Conventions:
-- * Dates are ISO TEXT (YYYY-MM-DD).
-- * Intervals are [valid_from, valid_to): valid_from inclusive, valid_to exclusive,
--   valid_to NULL = still valid. '1900-01-01' marks "since before our coverage".
-- * security_id is stable and never reused. It equals the storage_symbol used for
--   the security's rows in Parquet/Iceberg (nse_symbol column) at creation time.
-- * The reviewed seed lives in reference/*.csv and is loaded with `load-reference-data`.
-- equity_symbol_reference stays the current-symbol lookup used by the fetchers.

CREATE TABLE IF NOT EXISTS security_master (
    security_id TEXT PRIMARY KEY NOT NULL,
    storage_symbol TEXT NOT NULL UNIQUE,
    security_type TEXT NOT NULL CHECK (security_type IN ('equity', 'equity_dvr', 'index')),
    company_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('active', 'merged', 'delisted', 'suspended')),
    first_trade_date TEXT,
    last_trade_date TEXT,
    successor_security_id TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS security_identifier_history (
    security_id TEXT NOT NULL,
    identifier_type TEXT NOT NULL CHECK (identifier_type IN ('nse_symbol', 'isin')),
    identifier_value TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    source TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (identifier_type, identifier_value, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_security_identifier_history_security
ON security_identifier_history (security_id, identifier_type, valid_from);

CREATE TABLE IF NOT EXISTS security_provider_mapping (
    security_id TEXT NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('upstox', 'breeze')),
    instrument_key TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    availability TEXT NOT NULL CHECK (availability IN ('verified', 'unverified', 'unavailable')),
    verified_on TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (security_id, provider, valid_from)
);

CREATE TABLE IF NOT EXISTS corporate_action (
    action_id TEXT PRIMARY KEY NOT NULL,
    security_id TEXT NOT NULL,
    action_type TEXT NOT NULL CHECK (action_type IN ('split', 'bonus', 'demerger', 'merger', 'rename', 'isin_change', 'suspension', 'delisting', 'listing')),
    ex_date TEXT,
    effective_date TEXT,
    ratio TEXT,
    related_security_id TEXT,
    description TEXT NOT NULL,
    source_url TEXT,
    verification TEXT NOT NULL CHECK (verification IN ('verified', 'news', 'unverified')),
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_corporate_action_security
ON corporate_action (security_id, ex_date);

CREATE TABLE IF NOT EXISTS index_definition (
    index_code TEXT PRIMARY KEY NOT NULL,
    storage_symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    target_constituents INTEGER,
    methodology_url TEXT,
    notes TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_membership (
    index_code TEXT NOT NULL,
    security_id TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    membership_type TEXT NOT NULL CHECK (membership_type IN ('regular', 'demerger_placeholder')),
    symbol_as_announced TEXT NOT NULL,
    inclusion_reason TEXT,
    exclusion_reason TEXT,
    inclusion_source TEXT,
    exclusion_source TEXT,
    verification TEXT NOT NULL CHECK (verification IN ('verified', 'derived', 'pending')),
    notes TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (index_code, security_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_index_membership_dates
ON index_membership (index_code, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS reference_source (
    source_id TEXT PRIMARY KEY NOT NULL,
    kind TEXT NOT NULL,
    location TEXT NOT NULL,
    sha256 TEXT,
    description TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_volume_state (
    index_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('complete', 'partial', 'no_index_data', 'failed')),
    formula_version TEXT NOT NULL,
    membership_fingerprint TEXT NOT NULL,
    input_fingerprint TEXT NOT NULL,
    expected_members INTEGER NOT NULL,
    observed_members INTEGER NOT NULL,
    missing_members TEXT,
    not_trading_members TEXT,
    session_minutes INTEGER,
    minutes_with_turnover INTEGER,
    total_turnover REAL,
    total_volume INTEGER,
    published_at TEXT,
    error TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (index_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_volume_state_status
ON index_volume_state (index_code, status, trade_date);

-- Membership with the symbol that was live on each interval's first day and the
-- current symbol, for quick human checks.
CREATE VIEW IF NOT EXISTS v_index_membership AS
SELECT
    m.index_code,
    m.security_id,
    s.storage_symbol,
    s.company_name,
    m.valid_from,
    m.valid_to,
    m.membership_type,
    m.symbol_as_announced,
    (
        SELECT h.identifier_value FROM security_identifier_history h
        WHERE h.security_id = m.security_id AND h.identifier_type = 'nse_symbol' AND h.valid_to IS NULL
        LIMIT 1
    ) AS current_nse_symbol,
    m.verification,
    m.notes
FROM index_membership m
JOIN security_master s ON s.security_id = m.security_id;
