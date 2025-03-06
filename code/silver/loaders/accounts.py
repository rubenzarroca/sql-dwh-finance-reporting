"""
Loader for silver.accounts table.

This module extracts account data from the bronze layer,
applies transformations based on the Spanish Chart of Accounts (PGC),
and loads the results into the silver.accounts table.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Importar utilidades compartidas
from utils import get_db_connection

def extract_bronze_accounts(conn) -> pd.DataFrame:
    """
    Extract account data from bronze.holded_accounts.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with account data from bronze layer
    """
    try:
        query = """
        SELECT 
            id, 
            color, 
            num, 
            name, 
            "group", 
            debit, 
            credit, 
            balance,
            dwh_insert_timestamp,
            dwh_update_timestamp,
            dwh_batch_id
        FROM bronze.holded_accounts
        ORDER BY num
        """
        
        logger.info("Extracting accounts data from bronze.holded_accounts")
        df = pd.read_sql(query, conn)
        
        logger.info(f"Extracted {len(df)} accounts from bronze layer")
        return df
    
    except Exception as e:
        logger.error(f"Error extracting bronze accounts: {str(e)}")
        raise

def determine_account_type(account_number: int) -> str:
    """
    Determine account type based on Spanish Chart of Accounts (PGC).
    
    Args:
        account_number: 8-digit account number
        
    Returns:
        Account type: Asset, Liability, Equity, Income, or Expense
    """
    # Extract first digit (main group)
    first_digit = account_number // 10000000
    
    # Extract second digit (subgroup)
    second_digit = (account_number // 1000000) % 10
    
    # Simple group mapping
    group_type_map = {
        2: "Asset",      # Group 2: Non-current assets
        3: "Asset",      # Group 3: Inventories
        6: "Expense",    # Group 6: Expenses
        7: "Income",     # Group 7: Income
        8: "Expense",    # Group 8: Expenses allocated to equity
        9: "Income"      # Group 9: Income allocated to equity
    }
    
    # Return type directly if in simple mapping
    if first_digit in group_type_map:
        return group_type_map[first_digit]
    
    # For groups that need additional analysis
    if first_digit == 1:  # Group 1: BASIC FINANCING
        # Subgroups 10-13 are Equity
        if second_digit <= 3:
            return "Equity"
        # Subgroups 14-19 are generally Liabilities
        else:
            return "Liability"
            
    elif first_digit == 4:  # Group 4: CREDITORS AND DEBTORS
        # Specific subgroups that are liabilities
        if second_digit in [0, 1] or second_digit == 6 or \
           (second_digit == 7 and (account_number // 10000) % 100 in [50, 51, 52, 58, 59, 60, 61, 70, 79]):
            return "Liability"
        # The rest are assets
        else:
            return "Asset"
            
    elif first_digit == 5:  # Group 5: FINANCIAL ACCOUNTS
        # Subgroups that are liabilities
        if second_digit in [0, 1, 2, 5, 6]:
            return "Liability"
        # Subgroups that are assets
        else:
            return "Asset"
    
    # Default case (should not happen with valid PGC accounts)
    logger.warning(f"Account type not determined for number: {account_number}")
    return "Unknown"

def determine_account_subtype(account_number: int) -> str:
    """
    Determine the account subtype based on the Spanish Chart of Accounts.
    
    Args:
        account_number: 8-digit account number
        
    Returns:
        Account subtype description
    """
    # Extract first two digits (subgroup)
    subgroup = account_number // 1000000
    
    # Map of subgroups to subtypes
    subtype_map = {
        # Grupo 1: FINANCIACIÓN BÁSICA
        10: "Capital",
        11: "Reservas y otros instrumentos de patrimonio",
        12: "Resultados pendientes de aplicación",
        13: "Subvenciones, donaciones y ajustes por cambios de valor",
        14: "Provisiones",
        15: "Deudas a largo plazo con características especiales",
        16: "Deudas a largo plazo con partes vinculadas",
        17: "Deudas a largo plazo por préstamos y otros",
        18: "Pasivos por fianzas y garantías a largo plazo",
        19: "Situaciones transitorias de financiación",
        
        # Grupo 2: ACTIVO NO CORRIENTE
        20: "Inmovilizaciones intangibles",
        21: "Inmovilizaciones materiales",
        22: "Inversiones inmobiliarias",
        23: "Inmovilizaciones materiales en curso",
        24: "Inversiones financieras en partes vinculadas",
        25: "Otras inversiones financieras a largo plazo",
        26: "Fianzas y depósitos constituidos a largo plazo",
        28: "Amortización acumulada del inmovilizado",
        29: "Deterioro de valor de activos no corrientes",
        
        # Grupo 3: EXISTENCIAS
        30: "Comerciales",
        31: "Materias primas",
        32: "Otros aprovisionamientos",
        33: "Productos en curso",
        34: "Productos semiterminados",
        35: "Productos terminados",
        36: "Subproductos, residuos y materiales recuperados",
        39: "Deterioro de valor de las existencias",
        
        # Grupo 4: ACREEDORES Y DEUDORES
        40: "Proveedores",
        41: "Acreedores varios",
        43: "Clientes",
        44: "Deudores varios",
        46: "Personal",
        47: "Administraciones públicas",
        48: "Ajustes por periodificación",
        49: "Deterioro de valor de créditos comerciales",
        
        # Grupo 5: CUENTAS FINANCIERAS
        50: "Empréstitos y deudas a corto plazo",
        51: "Deudas a corto plazo con partes vinculadas",
        52: "Deudas a corto plazo por préstamos y otros",
        53: "Inversiones financieras a corto plazo en partes vinculadas",
        54: "Otras inversiones financieras a corto plazo",
        55: "Otras cuentas no bancarias",
        56: "Fianzas y depósitos recibidos y constituidos a corto plazo",
        57: "Tesorería",
        58: "Activos no corrientes mantenidos para la venta",
        59: "Deterioro del valor de inversiones financieras a corto plazo",
        
        # Grupo 6: COMPRAS Y GASTOS
        60: "Compras",
        61: "Variación de existencias",
        62: "Servicios exteriores",
        63: "Tributos",
        64: "Gastos de personal",
        65: "Otros gastos de gestión",
        66: "Gastos financieros",
        67: "Pérdidas procedentes de activos no corrientes",
        68: "Dotaciones para amortizaciones",
        69: "Pérdidas por deterioro y otras dotaciones",
        
        # Grupo 7: VENTAS E INGRESOS
        70: "Ventas de mercaderías y producción",
        71: "Variación de existencias",
        73: "Trabajos realizados para la empresa",
        74: "Subvenciones, donaciones y legados",
        75: "Otros ingresos de gestión",
        76: "Ingresos financieros",
        77: "Beneficios procedentes de activos no corrientes",
        79: "Excesos y aplicaciones de provisiones"
    }
    
    return subtype_map.get(subgroup, f"Subgroup {subgroup}")

def determine_parent_account(account_number: int) -> int:
    """
    Determine the parent account number for hierarchy.
    For 8-digit accounts, truncate to 7 digits.
    
    Args:
        account_number: 8-digit account number
        
    Returns:
        Parent account number
    """
    return (account_number // 10) * 10

def is_tax_relevant(account_number: int) -> bool:
    """
    Determine if an account is relevant for tax calculations.
    
    Args:
        account_number: 8-digit account number
        
    Returns:
        True if account is tax relevant, False otherwise
    """
    # VAT accounts
    if account_number // 10000 in [4720, 4770]:
        return True
    
    # Income tax accounts
    if account_number // 10000 in [4740, 4745, 4752]:
        return True
    
    # All Income accounts (for VAT declarations)
    if account_number // 10000000 == 7:
        return True
    
    # All Expense accounts (for VAT declarations)
    if account_number // 10000000 == 6:
        return True
    
    return False

def transform_accounts_data(df: pd.DataFrame) -> List[Tuple]:
    """
    Transform and enrich accounts data for silver layer.
    
    Args:
        df: DataFrame with bronze accounts data
        
    Returns:
        List of tuples with transformed data ready for insertion
    """
    logger.info("Transforming accounts data for silver.accounts")
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    transformed_data = []
    skipped_accounts = 0
    
    for _, row in df.iterrows():
        account_id = row['id']
        
        # Validate account number exists
        if pd.isna(row['num']) or row['num'] is None:
            logger.warning(f"Account with ID {account_id} has no number, skipping")
            skipped_accounts += 1
            continue
        
        # Ensure account number is an integer
        try:
            account_number = int(row['num'])
        except (ValueError, TypeError):
            logger.warning(f"Invalid account number for ID {account_id}: {row['num']}, skipping")
            skipped_accounts += 1
            continue
        
        # Pad account number to 8 digits if needed
        if account_number < 10000000:
            digits = len(str(account_number))
            logger.info(f"Account {account_number} has {digits} digits instead of 8, padding")
            account_number = account_number * 10**(8 - digits)
        
        # Determine account type and subtype
        account_type = determine_account_type(account_number)
        account_subtype = determine_account_subtype(account_number)
        
        # Determine parent account for hierarchy
        parent_account = determine_parent_account(account_number)
        
        # Get PGC group and subgroup
        pgc_group = account_number // 10000000
        pgc_subgroup = account_number // 1000000
        
        # Determine if account is tax relevant
        tax_relevant = is_tax_relevant(account_number)
        
        # Calculate last movement date
        last_movement = None
        if row['debit'] > 0 or row['credit'] > 0:
            last_movement = row['dwh_update_timestamp'].date() if isinstance(row['dwh_update_timestamp'], datetime) else None
        
        # Create data tuple for insertion
        account_data = (
            account_id,                  # account_id
            account_number,              # account_number
            row['name'] or f"Account {account_number}",  # account_name
            row['group'] or "No Group",  # account_group
            account_type,                # account_type
            account_subtype,             # account_subtype
            True,                        # is_analytic (all 8-digit accounts are analytic)
            parent_account,              # parent_account_number
            5,                           # account_level (level 5 for 8-digit accounts)
            True,                        # is_active
            row['balance'] or 0,         # current_balance
            row['debit'] or 0,           # debit_balance
            row['credit'] or 0,          # credit_balance
            last_movement,               # last_movement_date
            pgc_group,                   # pgc_group
            pgc_subgroup,                # pgc_subgroup
            tax_relevant,                # tax_relevant
            datetime.now(),              # dwh_created_at
            datetime.now(),              # dwh_updated_at
            'bronze.holded_accounts',    # dwh_source_table
            batch_id                     # dwh_batch_id
        )
        
        transformed_data.append(account_data)
    
    logger.info(f"Transformation completed. {len(transformed_data)} accounts processed, {skipped_accounts} skipped")
    return transformed_data

def load_accounts_to_silver(conn, accounts_data: List[Tuple], full_refresh: bool = False) -> int:
    """
    Load transformed accounts data into silver.accounts table.
    
    Args:
        conn: Database connection
        accounts_data: List of tuples with transformed data
        full_refresh: If True, truncate target table before loading
        
    Returns:
        Number of records inserted
    """
    if not accounts_data:
        logger.warning("No data to load into silver.accounts")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # If full refresh, truncate target table
        if full_refresh:
            logger.info("Truncating silver.accounts table for full refresh")
            cursor.execute("TRUNCATE TABLE silver.accounts CASCADE")
        
        # Prepare insert query
        insert_query = """
        INSERT INTO silver.accounts (
            account_id, account_number, account_name, account_group, 
            account_type, account_subtype, is_analytic, parent_account_number,
            account_level, is_active, current_balance, debit_balance,
            credit_balance, last_movement_date, pgc_group, pgc_subgroup,
            tax_relevant, dwh_created_at, dwh_updated_at, dwh_source_table,
            dwh_batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Execute batch insert
        cursor.executemany(insert_query, accounts_data)
        conn.commit()
        
        # Get number of inserted records
        cursor.execute("SELECT COUNT(*) FROM silver.accounts")
        count = cursor.fetchone()[0]
        
        logger.info(f"Successfully loaded {count} records into silver.accounts")
        
        # Generate load statistics
        cursor.execute("""
            SELECT account_type, COUNT(*) 
            FROM silver.accounts 
            GROUP BY account_type 
            ORDER BY account_type
        """)
        
        type_stats = cursor.fetchall()
        logger.info("Account type distribution:")
        for account_type, type_count in type_stats:
            logger.info(f"  {account_type}: {type_count} accounts")
        
        cursor.close()
        return count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data into silver.accounts: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_accounts(full_refresh: bool = True) -> bool:
    """
    Main function to orchestrate the ETL process for accounts.
    
    Args:
        full_refresh: If True, perform full refresh instead of incremental load
        
    Returns:
        True if load was successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"Starting accounts ETL process at {start_time}")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Extract data from bronze layer
        df_accounts = extract_bronze_accounts(conn)
        
        # Transform data
        transformed_data = transform_accounts_data(df_accounts)
        
        # Load data into silver layer
        inserted_count = load_accounts_to_silver(conn, transformed_data, full_refresh)
        
        # Close connection
        conn.close()
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Accounts ETL completed in {duration:.2f} seconds, {inserted_count} records loaded")
        
        return True
        
    except Exception as e:
        logger.error(f"Accounts ETL failed: {str(e)}")
        return False

if __name__ == "__main__":
    # This allows running this module as a standalone script
    import argparse
    
    parser = argparse.ArgumentParser(description='Load accounts into silver layer')
    parser.add_argument('--full-refresh', action='store_true', help='Perform full refresh instead of incremental load')
    
    args = parser.parse_args()
    
    success = load_accounts(full_refresh=args.full_refresh)
    exit(0 if success else 1)