-- Create the ticker_overview table
CREATE TABLE ticker_overview (
    ticker VARCHAR(7) NOT NULL,
    enterprise_to_ebitda NUMERIC(7,2),
    price_to_book NUMERIC(7,2),
    gross_margin NUMERIC(5,2),
    operating_margin NUMERIC(5,2),
    profit_margin NUMERIC(5,2),
    earnings_growth NUMERIC(9,2),
    revenue_growth NUMERIC(10,2),
    trailing_eps NUMERIC(7,2),
    forward_eps NUMERIC(7,2),
    peg_ratio NUMERIC(7,2),
    CONSTRAINT idx_ticker_overview_ticker PRIMARY KEY (ticker),
    CONSTRAINT ticker FOREIGN KEY (ticker) REFERENCES ticker_summary(ticker)
);
