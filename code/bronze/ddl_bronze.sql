/*
===============================================================================
DDL Script: Create Bronze Tables for Holded Data
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema for Holded accounting data,
    dropping existing tables if they already exist.
    Run this script to re-define the DDL structure of Holded tables in the Bronze layer.
===============================================================================
*/

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS bronze;

-- Drop table if it exists and recreate it
DROP TABLE IF EXISTS bronze.holded_accounts;

-- Create the Bronze table for Holded accounts data
CREATE TABLE bronze.holded_accounts (
    -- Original fields from Holded API
    id VARCHAR(24),              -- MongoDB ObjectId format
    color VARCHAR(7),            -- Hex color code (e.g., "#64DB46")
    num BIGINT,                  -- Account number
    name VARCHAR(255),           -- Account name
    "group" VARCHAR(255),        -- Account group/category
    debit NUMERIC(15, 2),        -- Debit amount
    credit NUMERIC(15, 2),       -- Credit amount
    balance NUMERIC(15, 2),      -- Account balance
    
    -- Technical metadata columns with dwh_ prefix
    dwh_source_system VARCHAR(50) DEFAULT 'holded',
    dwh_source_entity VARCHAR(50) DEFAULT 'accounts',
    dwh_insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_batch_id VARCHAR(50),
    dwh_process_id VARCHAR(50)
);

-- Create primary key for data identification
ALTER TABLE bronze.holded_accounts ADD CONSTRAINT pk_holded_accounts PRIMARY KEY (id);

-- Create indexes for common query patterns
CREATE INDEX idx_holded_accounts_num ON bronze.holded_accounts(num);
CREATE INDEX idx_holded_accounts_group ON bronze.holded_accounts("group");

-- Add table comment
COMMENT ON TABLE bronze.holded_accounts IS 'Chart of accounts data from Holded API in Bronze layer';

-- Drop table if it exists and recreate it
DROP TABLE IF EXISTS bronze.holded_dailyledger;

-- Create the Bronze table for Holded daily ledger data
CREATE TABLE bronze.holded_dailyledger (
    -- Original fields from Holded API
    entrynumber INTEGER,         -- Entry number in the ledger
    line INTEGER,                -- Line number within the entry
    timestamp BIGINT,            -- Unix timestamp of the entry
    type VARCHAR(50),            -- Type of entry
    description TEXT,            -- Entry description
    docdescription TEXT,         -- Document description
    account BIGINT,              -- Account number
    debit NUMERIC(15, 2),        -- Debit amount
    credit NUMERIC(15, 2),       -- Credit amount
    tags JSONB,                  -- Tags associated with the entry (array in JSON)
    checked VARCHAR(3),          -- Check status (e.g., "Yes")
    
    -- Technical metadata columns with dwh_ prefix
    dwh_source_system VARCHAR(50) DEFAULT 'holded',
    dwh_source_entity VARCHAR(50) DEFAULT 'dailyledger',
    dwh_insert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_update_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dwh_batch_id VARCHAR(50),
    dwh_process_id VARCHAR(50),
    dwh_page_number INTEGER      -- To track pagination from the API
);

-- Create a composite primary key
ALTER TABLE bronze.holded_dailyledger ADD CONSTRAINT pk_holded_dailyledger PRIMARY KEY (entryNumber, line);

-- Create indexes for common query patterns
CREATE INDEX idx_holded_dailyledger_timestamp ON bronze.holded_dailyledger(timestamp);
CREATE INDEX idx_holded_dailyledger_account ON bronze.holded_dailyledger(account);
CREATE INDEX idx_holded_dailyledger_type ON bronze.holded_dailyledger(type);

-- Add table comment
COMMENT ON TABLE bronze.holded_dailyledger IS 'Daily ledger entries from Holded API in Bronze layer';
