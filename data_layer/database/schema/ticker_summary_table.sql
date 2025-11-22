-- Create the ticker_summary table
CREATE TABLE ticker_summary (
    ticker VARCHAR(20) NOT NULL,
    cik INTEGER,
    market_cap BIGINT NOT NULL,
    previous_close NUMERIC(15,2) NOT NULL,
    pe_ratio NUMERIC(10,2),
    forward_pe_ratio NUMERIC(10,2),
    dividend_yield NUMERIC(5,2),
    payout_ratio NUMERIC(5,2),
    fifty_day_average NUMERIC(10,2) NOT NULL,
    two_hundred_day_average NUMERIC(10,2) NOT NULL,
    annual_dividend_growth NUMERIC(5,2),
    five_year_avg_dividend_yield NUMERIC(5,2),
    CONSTRAINT idx_ticker_summary_ticker PRIMARY KEY (ticker),
    CONSTRAINT cik FOREIGN KEY (cik) REFERENCES cik_lookup(cik)
);

-- Indexes (match the structure used in the database dump)
CREATE INDEX idx_ticker_summary_cik ON ticker_summary (cik);
CREATE INDEX idx_ticker_summary_dividend_yield ON ticker_summary (dividend_yield) WHERE dividend_yield IS NOT NULL;
CREATE INDEX idx_ticker_summary_forward_pe_ratio ON ticker_summary (forward_pe_ratio) WHERE forward_pe_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_market_cap ON ticker_summary (market_cap);
CREATE INDEX idx_ticker_summary_payout_ratio ON ticker_summary (payout_ratio) WHERE payout_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_pe_ratio ON ticker_summary (pe_ratio) WHERE pe_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_previous_close ON ticker_summary (previous_close);
CREATE INDEX idx_ticker_summary_annual_dividend_growth ON ticker_summary (annual_dividend_growth);

-- Text-search / case-insensitive helpers for ticker
CREATE INDEX idx_ticker_summary_ticker_lower ON ticker_summary (lower(ticker::text));
-- Trigram index (requires the pg_trgm extension in the DB) for similarity searches
CREATE INDEX idx_ticker_summary_ticker_lower_trgm ON ticker_summary USING gin (lower(ticker::text) gin_trgm_ops);