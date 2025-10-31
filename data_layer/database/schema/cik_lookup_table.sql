-- CIK Lookup Table
-- This table stores the mapping between CIK (Central Index Key) and company names
-- CIK is used by the SEC to uniquely identify companies

CREATE TABLE IF NOT EXISTS cik_lookup (
    cik INTEGER PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index on company_name for fast lookups by company name
-- Using a case-insensitive index for better search performance
CREATE INDEX IF NOT EXISTS idx_cik_lookup_company_name 
ON cik_lookup (LOWER(company_name));

-- Additional index for pattern matching searches
CREATE INDEX IF NOT EXISTS idx_cik_lookup_company_name_pattern 
ON cik_lookup (company_name varchar_pattern_ops);

-- Comment on table and columns
COMMENT ON TABLE cik_lookup IS 'Maps SEC Central Index Keys (CIK) to company names';
COMMENT ON COLUMN cik_lookup.cik IS 'SEC Central Index Key - unique identifier for companies';
COMMENT ON COLUMN cik_lookup.company_name IS 'Name of the company associated with the CIK';
COMMENT ON COLUMN cik_lookup.created_at IS 'Timestamp when the record was created';
COMMENT ON COLUMN cik_lookup.last_updated_at IS 'Timestamp when the record was last updated';
