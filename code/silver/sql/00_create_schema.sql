/*
===========================================================================
DDL Script: Create Silver Schema
===========================================================================
Purpose:
  Creates the silver schema in the data warehouse if it doesn't exist.
  This is the first script that debe ejecutarse.
===========================================================================
*/

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS silver;

COMMENT ON SCHEMA silver IS 'Silver layer containing cleaned and normalized financial data';