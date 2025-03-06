/*
===========================================================================
DDL Script: Silver Accounts Table
===========================================================================
Purpose:
  Definition of the normalized accounts table in the Silver layer,
  enhanced with hierarchical structure for financial reporting according
  to the Plan General Contable español.
===========================================================================
*/

-- Drop table if it exists
DROP TABLE IF EXISTS silver.accounts CASCADE;

-- Create the Silver accounts table with enhanced hierarchy
CREATE TABLE silver.accounts (
  account_id character varying(24) NOT NULL,
  account_number bigint NOT NULL,
  account_name character varying(255) NOT NULL,
  account_group character varying(100) NOT NULL,
  account_type character varying(100) NOT NULL,
  account_subtype character varying(200) NULL,
  
  -- Hierarchical structure for financial statements
  -- Balance de Situación hierarchy
  balance_section character varying(50) NULL, -- ACTIVO, PATRIMONIO NETO, PASIVO
  balance_subsection character varying(100) NULL, -- ACTIVO NO CORRIENTE, ACTIVO CORRIENTE, etc.
  balance_group character varying(200) NULL, -- Inmovilizado intangible, Existencias, etc.
  balance_subgroup character varying(200) NULL, -- Más detalle si es necesario
  
  -- Cuenta de Pérdidas y Ganancias hierarchy
  pyg_section character varying(50) NULL, -- OPERACIONES CONTINUADAS, OPERACIONES INTERRUMPIDAS
  pyg_group character varying(200) NULL, -- Importe neto de la cifra de negocios, Gastos de personal, etc.
  pyg_subgroup character varying(200) NULL, -- Más detalle dentro de cada grupo
  
  -- Reporting order
  balance_order integer NULL, -- Para ordenación en Balance
  pyg_order integer NULL, -- Para ordenación en PyG
  
  -- Original PGC structure
  is_analytic boolean NULL DEFAULT true,
  parent_account_number bigint NULL,
  account_level integer NULL DEFAULT 5,
  is_active boolean NULL DEFAULT true,
  current_balance numeric(15, 2) NULL,
  debit_balance numeric(15, 2) NULL,
  credit_balance numeric(15, 2) NULL,
  last_movement_date date NULL,
  
  -- PGC classification - Expanded to 3 levels
  pgc_group integer NULL,
  pgc_subgroup integer NULL,
  pgc_detail integer NULL, -- Nivel adicional para mayor detalle del PGC
  
  tax_relevant boolean NULL DEFAULT false,
  dwh_created_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_updated_at timestamp without time zone NULL DEFAULT CURRENT_TIMESTAMP,
  dwh_source_table character varying(200) NULL DEFAULT 'bronze.holded_accounts'::character varying,
  dwh_batch_id character varying(100) NULL,
  
  CONSTRAINT pk_silver_accounts PRIMARY KEY (account_id),
  CONSTRAINT uk_silver_accounts_number UNIQUE (account_number)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_accounts_group ON silver.accounts(account_group);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_type ON silver.accounts(account_type);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_parent ON silver.accounts(parent_account_number);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_pgc_group ON silver.accounts(pgc_group);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_pgc_subgroup ON silver.accounts(pgc_subgroup);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_pgc_detail ON silver.accounts(pgc_detail);

-- Create indexes for financial reporting
CREATE INDEX IF NOT EXISTS idx_silver_accounts_balance_section ON silver.accounts(balance_section, balance_order);
CREATE INDEX IF NOT EXISTS idx_silver_accounts_pyg_section ON silver.accounts(pyg_section, pyg_order);

-- Add table comment
COMMENT ON TABLE silver.accounts IS 'Normalized chart of accounts with enriched metadata, hierarchy and classification based on PGC español';

-- Add column comments
COMMENT ON COLUMN silver.accounts.account_id IS 'Original identifier from Holded';
COMMENT ON COLUMN silver.accounts.account_number IS '8-digit account number from PGC';
COMMENT ON COLUMN silver.accounts.balance_section IS 'Main section in Balance Sheet (ACTIVO, PATRIMONIO NETO, PASIVO)';
COMMENT ON COLUMN silver.accounts.balance_subsection IS 'Subsection in Balance Sheet (ACTIVO NO CORRIENTE, PASIVO CORRIENTE, etc.)';
COMMENT ON COLUMN silver.accounts.balance_group IS 'Group within subsection (Inmovilizado intangible, Existencias, etc.)';
COMMENT ON COLUMN silver.accounts.balance_subgroup IS 'Subgroup for more detailed classification';
COMMENT ON COLUMN silver.accounts.pyg_section IS 'Main section in Profit & Loss statement';
COMMENT ON COLUMN silver.accounts.pyg_group IS 'Group within P&L section';
COMMENT ON COLUMN silver.accounts.pyg_subgroup IS 'Subgroup for more detailed P&L classification';
COMMENT ON COLUMN silver.accounts.pgc_detail IS 'Third level detail of PGC classification';
COMMENT ON COLUMN silver.accounts.balance_order IS 'Display order in Balance Sheet reports';
COMMENT ON COLUMN silver.accounts.pyg_order IS 'Display order in Profit & Loss reports';

-- Create views for financial reporting

-- Balance de Situación view
CREATE OR REPLACE VIEW silver.vw_balance_sheet_structure AS
SELECT
    balance_section,
    balance_subsection,
    balance_group,
    balance_subgroup,
    string_agg(DISTINCT account_number::text, ',') as account_numbers,
    string_agg(DISTINCT account_name, ', ') as account_names,
    MIN(balance_order) as display_order
FROM silver.accounts
WHERE balance_section IS NOT NULL
GROUP BY
    balance_section,
    balance_subsection,
    balance_group,
    balance_subgroup
ORDER BY
    MIN(balance_order);

-- Cuenta de Pérdidas y Ganancias view
CREATE OR REPLACE VIEW silver.vw_profit_loss_structure AS
SELECT
    pyg_section,
    pyg_group,
    pyg_subgroup,
    string_agg(DISTINCT account_number::text, ',') as account_numbers,
    string_agg(DISTINCT account_name, ', ') as account_names,
    MIN(pyg_order) as display_order
FROM silver.accounts
WHERE pyg_section IS NOT NULL
GROUP BY
    pyg_section,
    pyg_group,
    pyg_subgroup
ORDER BY
    MIN(pyg_order);