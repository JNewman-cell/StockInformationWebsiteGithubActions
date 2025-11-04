-- Create the ticker_summary table
CREATE TABLE ticker_summary (
    ticker VARCHAR(7) PRIMARY KEY,
    cik INTEGER,
    market_cap BIGINT NOT NULL,
    previous_close NUMERIC(9,2) NOT NULL,
    pe_ratio NUMERIC(9,2),
    forward_pe_ratio NUMERIC(9,2),
    dividend_yield NUMERIC(4,2),
    payout_ratio NUMERIC(4,2),
    fifty_day_average NUMERIC(9,2) NOT NULL,
    two_hundred_day_average NUMERIC(9,2) NOT NULL,
    FOREIGN KEY (cik) REFERENCES cik_lookup(cik)
);

-- Create indexes
CREATE INDEX idx_ticker_summary_dividend_yield ON ticker_summary (dividend_yield) WHERE dividend_yield IS NOT NULL;
CREATE INDEX idx_ticker_summary_forward_pe_ratio ON ticker_summary (forward_pe_ratio) WHERE forward_pe_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_market_cap ON ticker_summary (market_cap);
CREATE INDEX idx_ticker_summary_payout_ratio ON ticker_summary (payout_ratio) WHERE payout_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_pe_ratio ON ticker_summary (pe_ratio) WHERE pe_ratio IS NOT NULL;
CREATE INDEX idx_ticker_summary_previous_close ON ticker_summary (previous_close);