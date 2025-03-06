/*
===========================================================================
DDL Script: Silver Account Balances Table
===========================================================================
Purpose:
  Definition of account balances by period for financial reporting.
  Contains opening balance, period movements, and closing balance.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.account_balances CASCADE;

-- Create the account balances table
CREATE TABLE silver.account_balances (
  balance_id serial NOT NULL,
  account_id character varying(24) NOT NULL,
  account_number bigint NOT NULL,
  period_id integer NOT NULL,
  start_balance numeric(15, 2) NULL DEFAULT 0,
  period_debit numeric(15, 2) NULL DEFAULT 0,
  period_credit numeric(15, 2) NULL DEFAULT 0,
  end_balance numeric(15, 2) NULL DEFAULT 0,
  is_calculated boolean NULL DEFAULT true,
  dwh_created_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_updated_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_batch_id character varying(50) NULL,
  CONSTRAINT account_balances_pkey PRIMARY KEY (balance_id),
  CONSTRAINT uk_silver_account_balances UNIQUE (account_id, period_id),
  CONSTRAINT fk_silver_account_balances_account FOREIGN KEY (account_id) 
      REFERENCES silver.accounts (account_id),
  CONSTRAINT fk_silver_account_balances_period FOREIGN KEY (period_id) 
      REFERENCES silver.fiscal_periods (period_id)
) TABLESPACE pg_default;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_account_balances_account 
ON silver.account_balances USING btree (account_id) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_account_balances_period 
ON silver.account_balances USING btree (period_id) 
TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_silver_account_balances_number 
ON silver.account_balances USING btree (account_number) 
TABLESPACE pg_default;

-- Add table comment
COMMENT ON TABLE silver.account_balances IS 'Account balances by fiscal period with start/end balances';

-- Add column comments
COMMENT ON COLUMN silver.account_balances.balance_id IS 'Surrogate key for the account balance record';
COMMENT ON COLUMN silver.account_balances.start_balance IS 'Opening balance at the start of the period';
-- Additional column comments...