/*
===========================================================================
DDL Script: Silver Accounts Table
===========================================================================
Purpose:
  Definition of the normalized accounts table in the Silver layer.
  This table enriches the raw account data with business metadata.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.accounts CASCADE;

-- Create the Silver accounts table
CREATE TABLE silver.accounts (
    -- Existing definition from your file
  account_id character varying(24) not null,
  account_number bigint not null,
  account_name character varying(255) not null,
  account_group character varying(100) not null,
  account_type character varying(100) not null,
  account_subtype character varying(200) null,
  is_analytic boolean null default true,
  parent_account_number bigint null,
  account_level integer null default 5,
  is_active boolean null default true,
  current_balance numeric(15, 2) null,
  debit_balance numeric(15, 2) null,
  credit_balance numeric(15, 2) null,
  last_movement_date date null,
  pgc_group integer null,
  pgc_subgroup integer null,
  tax_relevant boolean null default false,
  dwh_created_at timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_updated_at timestamp without time zone null default CURRENT_TIMESTAMP,
  dwh_source_table character varying(200) null default 'bronze.holded_accounts'::character varying,
  dwh_batch_id character varying(100) null,
  constraint pk_silver_accounts primary key (account_id),
  constraint uk_silver_accounts_number unique (account_number)
) TABLESPACE pg_default;
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_accounts_group ON silver.accounts(account_group);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_type ON silver.accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_parent ON silver.accounts(parent_account_number);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_pgc_group ON silver.accounts(pgc_group);

-- Add table comment
COMMENT ON TABLE silver.accounts IS 'Normalized chart of accounts with enriched metadata and hierarchy based on PGC';

-- Add column comments
COMMENT ON COLUMN silver.accounts.account_id IS 'Original identifier from Holded';
COMMENT ON COLUMN silver.accounts.account_number IS '8-digit account number from PGC';
-- Additional column comments...