/*
===========================================================================
Master DDL Script: Silver Layer Schema and Tables
===========================================================================
Purpose:
  This master script executes all the DDL scripts for the Silver layer
  in the correct order, respecting the dependencies between tables.
===========================================================================
*/

-- Record execution start
\echo 'Starting Silver layer DDL deployment'
\echo 'Execution time: ' `date`

-- Create Schema
\echo 'Creating silver schema...'
\i '00_create_schema.sql'

-- Create Tables in order of dependency
\echo 'Creating silver.accounts table...'
\i '01_accounts.sql'

\echo 'Creating silver.fiscal_periods table...'
\i '02_fiscal_periods.sql'

\echo 'Creating silver.journal_entries table...'
\i '03_journal_entries.sql'

\echo 'Creating silver.journal_lines table...'
\i '04_journal_lines.sql'

\echo 'Creating silver.account_balances table...'
\i '05_account_balances.sql'

\echo 'Creating silver layer financial statement views...'
\i '06_views_financial_statements.sql'

-- Record execution end
\echo 'Silver layer DDL deployment completed successfully'
\echo 'Execution completed at: ' `date`