/*
===========================================================================
DDL Script: Silver Journal Entries Table
===========================================================================
Purpose:
  Definition of the journal entries table in the Silver layer.
  Journal entries represent accounting transactions with metadata.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.journal_entries CASCADE;

-- Create the journal entries table
CREATE TABLE silver.journal_entries (
  entry_id serial NOT NULL,
  entry_number integer NOT NULL,
  entry_date date NOT NULL,
  original_timestamp bigint NOT NULL,
  period_id integer NULL,
  entry_type character varying(50) NULL,
  description text NULL,
  document_description text NULL,
  is_closing_entry boolean NULL DEFAULT false,
  is_opening_entry boolean NULL DEFAULT false,
  is_adjustment boolean NULL DEFAULT false,
  is_checked boolean NULL DEFAULT false,
  entry_status character varying(20) NULL DEFAULT 'Posted'::character varying,
  total_debit numeric(15, 2) NULL DEFAULT 0,
  total_credit numeric(15, 2) NULL DEFAULT 0,
  dwh_created_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_updated_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_source_table character varying(100) NULL DEFAULT 'bronze.holded_dailyledger'::character varying,
  dwh_batch_id character varying(50) NULL,
  CONSTRAINT journal_entries_pkey PRIMARY KEY (entry_id),
  CONSTRAINT uk_silver_journal_entries UNIQUE (entry_number),
  CONSTRAINT fk_silver_journal_entries_period FOREIGN KEY (period_id)
      REFERENCES silver.fiscal_periods (period_id)
) TABLESPACE pg_default;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_journal_entries_date 
ON silver.journal_entries USING btree (entry_date) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_entries_period 
ON silver.journal_entries USING btree (period_id) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_journal_entries_type 
ON silver.journal_entries USING btree (entry_type) 
TABLESPACE pg_default;

-- Add table comment
COMMENT ON TABLE silver.journal_entries IS 'Journal entries from accounting system, normalized and enriched';

-- Add column comments
COMMENT ON COLUMN silver.journal_entries.entry_id IS 'Surrogate key for the journal entry';
COMMENT ON COLUMN silver.journal_entries.entry_number IS 'Original entry number from the source system';
-- Additional column comments...