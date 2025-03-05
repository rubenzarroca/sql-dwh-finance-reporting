/*
===============================================================================
DDL Script: Create Silver Tables for Financial Data Warehouse
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema that transform and normalize
    data from the Bronze layer, applying business rules and enrichment.
    
    Main transformations:
    - Normalize account structures
    - Create standardized journal entries
    - Apply business rules for financial analysis
    - Prepare data for dimensional modeling in Gold layer
===============================================================================
*/

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS silver;

-- Drop table if it exists
DROP TABLE IF EXISTS silver.accounts CASCADE;

-- Create the Silver accounts table
CREATE TABLE silver.accounts (
    -- Identification fields
    account_id VARCHAR(24) NOT NULL,                -- Original ID from Holded
    account_number BIGINT NOT NULL,                 -- Account number (8 digits)
    account_name VARCHAR(255) NOT NULL,             -- Account name
    account_group VARCHAR(100) NOT NULL,            -- Account group/category
    
    -- Classification fields
    account_type VARCHAR(50) NOT NULL,              -- Asset, Liability, Equity, Income, Expense
    account_subtype VARCHAR(50),                    -- More detailed classification
    is_analytic BOOLEAN DEFAULT TRUE,               -- All 8-digit accounts are considered analytic
    
    -- Hierarchy fields
    parent_account_number BIGINT,                   -- Truncated account number for hierarchy
    account_level INTEGER DEFAULT 5,                -- Level in account hierarchy (5 for most detailed)
    
    -- Status fields
    is_active BOOLEAN DEFAULT TRUE,                 -- Account status
    
    -- Balance fields (point-in-time snapshot)
    current_balance NUMERIC(15, 2),                 -- Current balance
    debit_balance NUMERIC(15, 2),                   -- Accumulated debits
    credit_balance NUMERIC(15, 2),                  -- Accumulated credits
    last_movement_date DATE,                        -- Date of last transaction
    
    -- PGC specific fields
    pgc_group INTEGER,                              -- Main group (1-9)
    pgc_subgroup INTEGER,                           -- Subgroup (10-99)
    tax_relevant BOOLEAN DEFAULT FALSE,             -- Relevant for tax calculations
    
    -- Technical metadata
    dwh_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_source_table VARCHAR(100) DEFAULT 'bronze.holded_accounts',
    dwh_batch_id VARCHAR(50),
    
    -- Constraints
    CONSTRAINT pk_silver_accounts PRIMARY KEY (account_id),
    CONSTRAINT uk_silver_accounts_number UNIQUE (account_number)
);

-- Create indexes for common query patterns
CREATE INDEX idx_silver_accounts_group ON silver.accounts(account_group);
CREATE INDEX idx_silver_accounts_type ON silver.accounts(account_type);
CREATE INDEX idx_silver_accounts_parent ON silver.accounts(parent_account_number);
CREATE INDEX idx_silver_accounts_pgc_group ON silver.accounts(pgc_group);

-- Add table comment
COMMENT ON TABLE silver.accounts IS 'Normalized chart of accounts with enriched metadata and hierarchy based on PGC';

-- Add column comments
COMMENT ON COLUMN silver.accounts.account_id IS 'Original identifier from Holded';
COMMENT ON COLUMN silver.accounts.account_number IS '8-digit account number from PGC';
COMMENT ON COLUMN silver.accounts.account_type IS 'Main classification: Asset, Liability, Equity, Income, Expense';
COMMENT ON COLUMN silver.accounts.parent_account_number IS 'Truncated account number for hierarchical analysis';
COMMENT ON COLUMN silver.accounts.pgc_group IS 'Main group from PGC (1-9)';
COMMENT ON COLUMN silver.accounts.pgc_subgroup IS 'Subgroup from PGC (10-99)';
