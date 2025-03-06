"""
Loader for silver.account_balances table.

This module calculates and maintains account balances by fiscal period,
providing the foundation for financial reporting and analysis.
Account balances include starting balance, period movements, and ending balance.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import utility functions
from utils import get_db_connection

def calculate_account_balances(conn) -> int:
    """
    Calculate and load account balances for all accounts and fiscal periods.
    This function performs the following steps:
    1. Calculates period movements (debits/credits) based on journal lines
    2. Calculates starting and ending balances for each period
    3. Stores the results in silver.account_balances
    
    Args:
        conn: Database connection
        
    Returns:
        Number of balance records processed
    """
    try:
        cursor = conn.cursor()
        
        # Generate a batch ID for tracking
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        logger.info("Starting account balances calculation")
        
        # Step 1: Calculate period movements (debits/credits) for each account and period
        # We combine journal entries with journal lines to get the right period for each transaction
        logger.info("Calculating period movements from journal entries and lines")
        
        movements_query = """
        WITH account_movements AS (
            -- Calcular débitos y créditos por cuenta y período
            SELECT 
                jl.account_id,
                jl.account_number,
                je.period_id,
                SUM(jl.debit_amount) as total_debit,
                SUM(jl.credit_amount) as total_credit
            FROM silver.journal_lines jl
            JOIN silver.journal_entries je ON jl.entry_id = je.entry_id
            WHERE je.period_id IS NOT NULL
            GROUP BY jl.account_id, jl.account_number, je.period_id
        ),
        periods_with_accounts AS (
            -- Generar todas las combinaciones de cuentas y períodos
            SELECT 
                a.account_id,
                a.account_number,
                fp.period_id
            FROM silver.accounts a
            CROSS JOIN silver.fiscal_periods fp
            -- Solo incluimos períodos hasta el actual
            WHERE fp.end_date <= CURRENT_DATE
        )
        -- Insert/update account balances with period movements
        INSERT INTO silver.account_balances (
            account_id, account_number, period_id,
            start_balance, period_debit, period_credit, end_balance,
            is_calculated, dwh_created_at, dwh_updated_at, dwh_batch_id
        )
        SELECT 
            pwa.account_id,
            pwa.account_number,
            pwa.period_id,
            0 as start_balance, -- Inicialmente 0, se actualizará luego
            COALESCE(am.total_debit, 0) as period_debit,
            COALESCE(am.total_credit, 0) as period_credit,
            0 as end_balance, -- Inicialmente 0, se actualizará luego
            TRUE as is_calculated,
            CURRENT_TIMESTAMP as dwh_created_at,
            CURRENT_TIMESTAMP as dwh_updated_at,
            %s as dwh_batch_id
        FROM periods_with_accounts pwa
        LEFT JOIN account_movements am 
            ON pwa.account_id = am.account_id 
            AND pwa.period_id = am.period_id
        -- Solo incluir combinaciones donde hay movimientos o la cuenta es relevante
        WHERE am.total_debit IS NOT NULL 
            OR am.total_credit IS NOT NULL
            OR EXISTS (
                SELECT 1 FROM silver.journal_lines jl
                WHERE jl.account_id = pwa.account_id
            )
        ON CONFLICT (account_id, period_id) DO UPDATE SET
            period_debit = EXCLUDED.period_debit,
            period_credit = EXCLUDED.period_credit,
            is_calculated = TRUE,
            dwh_updated_at = CURRENT_TIMESTAMP,
            dwh_batch_id = EXCLUDED.dwh_batch_id
        RETURNING account_id, period_id
        """
        
        cursor.execute(movements_query, (batch_id,))
        movements_result = cursor.fetchall()
        logger.info(f"Updated period movements for {len(movements_result)} account-period combinations")
        
        # Step 2: Update starting and ending balances
        # This needs to be done in chronological order to ensure correct balance carry-forward
        logger.info("Updating starting and ending balances in chronological order")
        
        update_balances_query = """
        WITH ordered_periods AS (
            -- Get periods in chronological order
            SELECT period_id, start_date
            FROM silver.fiscal_periods
            ORDER BY start_date
        ),
        period_pairs AS (
            -- Create pairs of current and previous periods
            SELECT 
                op.period_id as current_period_id,
                LAG(op.period_id) OVER(ORDER BY op.start_date) as prev_period_id
            FROM ordered_periods op
        )
        UPDATE silver.account_balances ab
        SET 
            -- Start balance is the end balance of the previous period, or 0 if first period
            start_balance = CASE 
                WHEN pp.prev_period_id IS NULL THEN 0
                ELSE (
                    SELECT prev.end_balance
                    FROM silver.account_balances prev
                    WHERE prev.account_id = ab.account_id
                    AND prev.period_id = pp.prev_period_id
                )
            END,
            -- End balance is start balance plus period movements
            end_balance = CASE 
                WHEN pp.prev_period_id IS NULL THEN ab.period_debit - ab.period_credit
                ELSE (
                    SELECT prev.end_balance
                    FROM silver.account_balances prev
                    WHERE prev.account_id = ab.account_id
                    AND prev.period_id = pp.prev_period_id
                ) + ab.period_debit - ab.period_credit
            END
        FROM period_pairs pp
        WHERE ab.period_id = pp.current_period_id
        """
        
        cursor.execute(update_balances_query)
        balances_updated = cursor.rowcount
        conn.commit()
        
        logger.info(f"Updated starting and ending balances for {balances_updated} records")
        
        # Step 3: Get statistics on the balances
        cursor.execute("SELECT COUNT(*) FROM silver.account_balances")
        total_balances = cursor.fetchone()[0]
        
        # Get statistics by account type
        cursor.execute("""
            SELECT 
                a.account_type,
                COUNT(DISTINCT ab.account_id) as account_count,
                COUNT(*) as balance_records,
                SUM(ab.end_balance) as total_balance
            FROM silver.account_balances ab
            JOIN silver.accounts a ON ab.account_id = a.account_id
            GROUP BY a.account_type
            ORDER BY a.account_type
        """)
        
        type_stats = cursor.fetchall()
        logger.info("Account balances by account type:")
        for account_type, account_count, balance_records, total_balance in type_stats:
            logger.info(f"  {account_type}: {account_count} accounts, {balance_records} records, {total_balance:.2f} total balance")
        
        # Get statistics by period
        cursor.execute("""
            SELECT 
                fp.period_name,
                COUNT(*) as balance_count,
                SUM(CASE WHEN a.account_type = 'Asset' THEN ab.end_balance ELSE 0 END) as assets,
                SUM(CASE WHEN a.account_type = 'Liability' THEN ab.end_balance ELSE 0 END) as liabilities,
                SUM(CASE WHEN a.account_type = 'Equity' THEN ab.end_balance ELSE 0 END) as equity
            FROM silver.account_balances ab
            JOIN silver.accounts a ON ab.account_id = a.account_id
            JOIN silver.fiscal_periods fp ON ab.period_id = fp.period_id
            GROUP BY fp.period_id, fp.period_name
            ORDER BY fp.period_id DESC
            LIMIT 5
        """)
        
        period_stats = cursor.fetchall()
        logger.info("Recent periods balance summary:")
        for period_name, balance_count, assets, liabilities, equity in period_stats:
            logger.info(f"  {period_name}: {balance_count} balances, Assets: {assets:.2f}, Liabilities: {liabilities:.2f}, Equity: {equity:.2f}")
        
        cursor.close()
        return total_balances
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error calculating account balances: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def recalculate_specific_period(conn, period_id: int) -> int:
    """
    Recalculate account balances for a specific period.
    Useful when journal entries for a specific period have changed.
    
    Args:
        conn: Database connection
        period_id: ID of the fiscal period to recalculate
        
    Returns:
        Number of account balances updated
    """
    try:
        cursor = conn.cursor()
        
        # Generate a batch ID for tracking
        batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
        
        logger.info(f"Recalculating account balances for period_id {period_id}")
        
        # Step 1: Recalculate period movements for the specific period
        movements_query = """
        WITH account_movements AS (
            -- Calculate debits and credits by account for the specific period
            SELECT 
                jl.account_id,
                jl.account_number,
                je.period_id,
                SUM(jl.debit_amount) as total_debit,
                SUM(jl.credit_amount) as total_credit
            FROM silver.journal_lines jl
            JOIN silver.journal_entries je ON jl.entry_id = je.entry_id
            WHERE je.period_id = %s
            GROUP BY jl.account_id, jl.account_number, je.period_id
        ),
        active_accounts AS (
            -- Only include accounts that have had activity
            SELECT DISTINCT account_id, account_number 
            FROM silver.journal_lines
        )
        -- Update account balances for the specific period
        UPDATE silver.account_balances ab
        SET 
            period_debit = COALESCE(am.total_debit, 0),
            period_credit = COALESCE(am.total_credit, 0),
            dwh_updated_at = CURRENT_TIMESTAMP,
            dwh_batch_id = %s
        FROM active_accounts aa
        LEFT JOIN account_movements am 
            ON aa.account_id = am.account_id 
            AND am.period_id = %s
        WHERE ab.account_id = aa.account_id 
          AND ab.period_id = %s
        RETURNING ab.account_id
        """
        
        cursor.execute(movements_query, (period_id, batch_id, period_id, period_id))
        movements_result = cursor.fetchall()
        movements_updated = len(movements_result)
        logger.info(f"Updated period movements for {movements_updated} accounts in period {period_id}")
        
        # Step 2: Update balances for this period and all future periods
        # First, get the start date of this period to identify future periods
        cursor.execute("SELECT start_date FROM silver.fiscal_periods WHERE period_id = %s", (period_id,))
        period_start_date = cursor.fetchone()[0]
        
        # Now update this period and all future periods
        update_balances_query = """
        WITH RECURSIVE period_chain AS (
            -- Start with the specified period
            SELECT 
                fp.period_id,
                fp.start_date,
                fp.period_name,
                1 as level
            FROM silver.fiscal_periods fp
            WHERE fp.period_id = %s
            
            UNION ALL
            
            -- Add all subsequent periods
            SELECT 
                fp.period_id,
                fp.start_date,
                fp.period_name,
                pc.level + 1
            FROM silver.fiscal_periods fp
            JOIN period_chain pc ON fp.start_date > pc.start_date
            ORDER BY fp.start_date
        ),
        period_sequence AS (
            -- Create a sequence of periods in chronological order
            SELECT 
                period_id,
                start_date,
                period_name,
                level,
                LAG(period_id) OVER (ORDER BY start_date) as prev_period_id
            FROM period_chain
        )
        -- Update balances for each period in the sequence
        UPDATE silver.account_balances ab
        SET 
            -- Start balance is the end balance of the previous period, or 0 if first period
            start_balance = CASE 
                WHEN ps.prev_period_id IS NULL AND ps.level = 1 THEN (
                    -- For the first period in our sequence, get start balance from the previous period if it exists
                    SELECT prev.end_balance
                    FROM silver.account_balances prev
                    JOIN silver.fiscal_periods fp_prev ON prev.period_id = fp_prev.period_id
                    WHERE prev.account_id = ab.account_id
                    AND fp_prev.start_date = (
                        SELECT MAX(start_date) 
                        FROM silver.fiscal_periods 
                        WHERE start_date < (SELECT start_date FROM silver.fiscal_periods WHERE period_id = %s)
                    )
                )
                WHEN ps.prev_period_id IS NULL THEN 0
                ELSE (
                    -- For subsequent periods, use the end balance of the previous period
                    SELECT prev.end_balance
                    FROM silver.account_balances prev
                    WHERE prev.account_id = ab.account_id
                    AND prev.period_id = ps.prev_period_id
                )
            END,
            -- End balance is start balance plus period movements
            end_balance = CASE 
                WHEN ps.prev_period_id IS NULL AND ps.level = 1 THEN (
                    -- First period: Previous end balance + current movements
                    SELECT COALESCE(prev.end_balance, 0)
                    FROM silver.account_balances prev
                    JOIN silver.fiscal_periods fp_prev ON prev.period_id = fp_prev.period_id
                    WHERE prev.account_id = ab.account_id
                    AND fp_prev.start_date = (
                        SELECT MAX(start_date) 
                        FROM silver.fiscal_periods 
                        WHERE start_date < (SELECT start_date FROM silver.fiscal_periods WHERE period_id = %s)
                    )
                ) + ab.period_debit - ab.period_credit
                WHEN ps.prev_period_id IS NULL THEN ab.period_debit - ab.period_credit
                ELSE (
                    -- Subsequent periods: Previous end balance + current movements
                    SELECT prev.end_balance
                    FROM silver.account_balances prev
                    WHERE prev.account_id = ab.account_id
                    AND prev.period_id = ps.prev_period_id
                ) + ab.period_debit - ab.period_credit
            END,
            dwh_updated_at = CURRENT_TIMESTAMP
        FROM period_sequence ps
        WHERE ab.period_id = ps.period_id
        """
        
        cursor.execute(update_balances_query, (period_id, period_id, period_id))
        balances_updated = cursor.rowcount
        conn.commit()
        
        logger.info(f"Updated balances for {balances_updated} records across {period_id} and subsequent periods")
        
        cursor.close()
        return balances_updated
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error recalculating account balances for period {period_id}: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_account_balances_main(full_refresh: bool = False, period_id: Optional[int] = None) -> bool:
    """
    Main function to orchestrate the account balances ETL process.
    
    Args:
        full_refresh: If True, recalculate all balances
        period_id: If specified, only recalculate balances for this period and subsequent periods
        
    Returns:
        True if load was successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"Starting account balances ETL process at {start_time}")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Determine process type
        if full_refresh:
            logger.info("Performing full refresh of all account balances")
            # Truncate the table first if full refresh
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE silver.account_balances")
            conn.commit()
            cursor.close()
            
            # Calculate all balances
            total_balances = calculate_account_balances(conn)
            logger.info(f"Full refresh completed. {total_balances} account balance records created.")
        
        elif period_id is not None:
            logger.info(f"Recalculating account balances for period {period_id} and subsequent periods")
            # Recalculate balances for the specified period and all future periods
            updated_count = recalculate_specific_period(conn, period_id)
            logger.info(f"Period-specific recalculation completed. {updated_count} records updated.")
        
        else:
            logger.info("Performing standard account balances update")
            # Standard process: add any missing periods and update existing ones
            total_balances = calculate_account_balances(conn)
            logger.info(f"Standard balance calculation completed. {total_balances} total account balance records.")
        
        # Close connection
        conn.close()
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Account balances ETL completed in {duration:.2f} seconds")
        
        return True
        
    except Exception as e:
        logger.error(f"Account balances ETL failed: {str(e)}")
        if 'conn' in locals() and not conn.closed:
            conn.close()
        return False

if __name__ == "__main__":
    # This allows running this module as a standalone script
    import argparse
    
    parser = argparse.ArgumentParser(description='Load account balances into silver layer')
    parser.add_argument('--full-refresh', action='store_true', help='Perform full refresh of all account balances')
    parser.add_argument('--period-id', type=int, help='Recalculate balances for a specific period ID and all subsequent periods')
    
    args = parser.parse_args()
    
    success = load_account_balances_main(full_refresh=args.full_refresh, period_id=args.period_id)
    exit(0 if success else 1)