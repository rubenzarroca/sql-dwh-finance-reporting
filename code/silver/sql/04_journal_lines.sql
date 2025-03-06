/*
===========================================================================
DDL Script: Silver Journal Lines Table
===========================================================================
Purpose:
  Definition of journal lines that make up journal entries.
  Each line represents a debit or credit to a specific account.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.journal_lines CASCADE;

-- Create the journal lines table
CREATE TABLE silver.journal_lines (
  line_id serial NOT NULL,
  entry_id integer NOT NULL,
  line_number integer NOT NULL,
  account_id character varying(24) NOT NULL,
  account_number bigint NOT NULL,
  debit_amount numeric(15, 2) NULL DEFAULT 0,
  credit_amount numeric(15, 2) NULL DEFAULT 0,
  description text NULL,
  tags jsonb NULL,
  is_checked boolean NULL DEFAULT false,
  is_reconciled boolean NULL DEFAULT false,
  is_tax_relevant boolean NULL DEFAULT false,
  tax_code character varying(20) NULL,
  cost_center character varying(50) NULL,
  business_line character varying(50) NULL,
  customer_id character varying(50) NULL,
  vendor_id character varying(50) NULL,
  project_id character varying(50) NULL,
  dwh_created_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_updated_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_source_table character varying(100) NULL DEFAULT 'bronze.holded_dailyledger'::character varying,
  dwh_batch_id character varying(50) NULL,
  CONSTRAINT journal_lines_pkey PRIMARY KEY (line_id),
  CONSTRAINT uk_silver_journal_lines UNIQUE (entry_id, line_number),
  CONSTRAINT fk_silver_journal_lines_account FOREIGN KEY (account_id) 
      REFERENCES silver.accounts (account_id),
  CONSTRAINT fk_silver_journal_lines_entry FOREIGN KEY (entry_id) 
      REFERENCES silver.journal_entries (entry_id) ON DELETE CASCADE
) TABLESPACE pg_default;

-- Add computed column for net amount (debit - credit)
ALTER TABLE silver.journal_lines ADD COLUMN IF NOT EXISTS 
    net_amount NUMERIC(15, 2) GENERATED ALWAYS AS (debit_amount - credit_amount) STORED;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_account 
ON silver.journal_lines USING btree (account_id) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_account_number 
ON silver.journal_lines USING btree (account_number) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tax 
ON silver.journal_lines USING btree (is_tax_relevant) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tags 
ON silver.journal_lines USING gin (tags) 
TABLESPACE pg_default;

-- Add table comment
COMMENT ON TABLE silver.journal_lines IS 'Journal lines from accounting entries with extended metadata';

-- Add column comments
COMMENT ON COLUMN silver.journal_lines.line_id IS 'Surrogate key for the journal line';
COMMENT ON COLUMN silver.journal_lines.entry_id IS 'Reference to the parent journal entry';
-- Additional column comments...