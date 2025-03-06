/*
===========================================================================
DDL Script: Silver Fiscal Periods Table
===========================================================================
Purpose:
  Definition of fiscal periods for financial reporting and analysis.
  This table establishes the time dimension for all financial data.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.fiscal_periods CASCADE;

-- Create the fiscal periods table
CREATE TABLE silver.fiscal_periods (
  period_id serial NOT NULL,
  period_year integer NOT NULL,
  period_quarter integer NOT NULL,
  period_month integer NOT NULL,
  period_name character varying(50) NOT NULL,
  start_date date NOT NULL,
  end_date date NOT NULL,
  is_closed boolean NULL DEFAULT false,
  closing_date date NULL,
  CONSTRAINT fiscal_periods_pkey PRIMARY KEY (period_id),
  CONSTRAINT uk_silver_fiscal_periods UNIQUE (period_year, period_month)
) TABLESPACE pg_default;

-- Create index for date filtering
CREATE INDEX IF NOT EXISTS idx_silver_fiscal_periods_dates 
ON silver.fiscal_periods USING btree (start_date, end_date) 
TABLESPACE pg_default;

-- Add table comment
COMMENT ON TABLE silver.fiscal_periods IS 'Fiscal periods for financial reporting and analysis';

-- Add column comments
COMMENT ON COLUMN silver.fiscal_periods.period_id IS 'Unique identifier for the fiscal period';
COMMENT ON COLUMN silver.fiscal_periods.period_year IS 'Year of the fiscal period';
COMMENT ON COLUMN silver.fiscal_periods.period_month IS 'Month number (1-12) of the fiscal period';
COMMENT ON COLUMN silver.fiscal_periods.is_closed IS 'Flag indicating if the period has been closed for accounting';