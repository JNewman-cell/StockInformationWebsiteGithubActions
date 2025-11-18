-- Create ENUM type for ticker directory status
CREATE TYPE ticker_directory_status AS ENUM ('active', 'inactive');

-- Create the ticker_directory table
CREATE TABLE ticker_directory (
    cik INTEGER NOT NULL,
    ticker VARCHAR(7) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status ticker_directory_status NOT NULL,
    id INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY,
    CONSTRAINT ticker_directory_pkey PRIMARY KEY (id),
    CONSTRAINT cik FOREIGN KEY (cik) REFERENCES cik_lookup(cik),
    CONSTRAINT ticker FOREIGN KEY (ticker) REFERENCES ticker_summary(ticker)
);

-- Create indexes
CREATE INDEX ticker_directory_cik_idx ON ticker_directory (cik);
CREATE INDEX ticker_directory_ticker_idx ON ticker_directory (ticker);
