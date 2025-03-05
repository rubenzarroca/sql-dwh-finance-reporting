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
    group VARCHAR(255),          -- Account group/category
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
CREATE INDEX idx_holded_accounts_group ON bronze.holded_accounts(group);

-- Add table comment
COMMENT ON TABLE bronze.holded_accounts IS 'Chart of accounts data from Holded API in Bronze layer';
