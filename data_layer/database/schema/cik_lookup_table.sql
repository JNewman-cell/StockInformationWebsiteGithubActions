-- Create the cik_lookup table
CREATE TABLE cik_lookup (
    cik INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    company_name CHARACTER VARYING(255) NOT NULL DEFAULT 'Company'::character varying,
    company_name_search CHARACTER VARYING(255),
    CONSTRAINT idx_cik_lookup_cik PRIMARY KEY (cik)
);

-- Index to support case-insensitive lookups on company_name
CREATE INDEX IF NOT EXISTS idx_cik_lookup_company_name_lower
    ON cik_lookup (lower(company_name::text));

-- Trigram GIN index for fast similarity / partial matching on company_name
-- Requires the pg_trgm extension to be available in the database
CREATE INDEX IF NOT EXISTS idx_cik_lookup_company_name_lower_trgm
    ON cik_lookup USING gin (lower(company_name::text) gin_trgm_ops);
