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
  tag1 character varying(100) NULL,
  tag2 character varying(100) NULL,
  tag3 character varying(100) NULL,
  tag4 character varying(100) NULL,
  is_checked boolean NULL DEFAULT false,
  is_reconciled boolean NULL DEFAULT false,
  is_tax_relevant boolean NULL DEFAULT false,
  cost_center character varying(50) NULL,
  business_line character varying(50) NULL,
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

-- Índices para las nuevas columnas de tags individuales
CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tag1
ON silver.journal_lines USING btree (tag1) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tag2
ON silver.journal_lines USING btree (tag2) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tag3
ON silver.journal_lines USING btree (tag3) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_tag4
ON silver.journal_lines USING btree (tag4) 
TABLESPACE pg_default;

-- Índices para dimensiones analíticas
CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_cost_center
ON silver.journal_lines USING btree (cost_center) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_lines_business_line
ON silver.journal_lines USING btree (business_line) 
TABLESPACE pg_default;

-- Add table comment
COMMENT ON TABLE silver.journal_lines IS 'Journal lines from accounting entries with extended metadata';

-- Add column comments
COMMENT ON COLUMN silver.journal_lines.line_id IS 'Surrogate key for the journal line';
COMMENT ON COLUMN silver.journal_lines.entry_id IS 'Reference to the parent journal entry';
COMMENT ON COLUMN silver.journal_lines.line_number IS 'Line number within the journal entry';
COMMENT ON COLUMN silver.journal_lines.account_id IS 'Reference to the account in silver.accounts';
COMMENT ON COLUMN silver.journal_lines.account_number IS 'Account number for reference';
COMMENT ON COLUMN silver.journal_lines.debit_amount IS 'Debit amount of the line';
COMMENT ON COLUMN silver.journal_lines.credit_amount IS 'Credit amount of the line';
COMMENT ON COLUMN silver.journal_lines.description IS 'Description of the line';
COMMENT ON COLUMN silver.journal_lines.tags IS 'JSON array of tags associated with the line';
COMMENT ON COLUMN silver.journal_lines.tag1 IS 'First tag from the tags array';
COMMENT ON COLUMN silver.journal_lines.tag2 IS 'Second tag from the tags array';
COMMENT ON COLUMN silver.journal_lines.tag3 IS 'Third tag from the tags array';
COMMENT ON COLUMN silver.journal_lines.tag4 IS 'Fourth tag from the tags array';
COMMENT ON COLUMN silver.journal_lines.is_checked IS 'Flag indicating if the line has been checked';
COMMENT ON COLUMN silver.journal_lines.is_reconciled IS 'Flag indicating if the line has been reconciled';
COMMENT ON COLUMN silver.journal_lines.is_tax_relevant IS 'Flag indicating if the line is relevant for tax calculations';
COMMENT ON COLUMN silver.journal_lines.cost_center IS 'Cost center associated with the line';
COMMENT ON COLUMN silver.journal_lines.business_line IS 'Business line associated with the line';
COMMENT ON COLUMN silver.journal_lines.net_amount IS 'Calculated net amount (debit - credit)';