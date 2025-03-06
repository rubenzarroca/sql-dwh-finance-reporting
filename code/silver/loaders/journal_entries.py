"""
Loader for silver.journal_entries table.

This module transforms journal entries from the bronze layer, enriches them with
metadata, and loads them into the silver.journal_entries table, establishing
relationships with fiscal periods.
"""

import os
import logging
import pandas as pd
from datetime import datetime, date
from typing import List, Tuple, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import utility functions
from utils import get_db_connection

def extract_bronze_journal_entries(conn) -> pd.DataFrame:
    """
    Extract journal entries from bronze.holded_dailyledger, aggregated by entry number.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with aggregated journal entries
    """
    try:
        query = """
        SELECT 
            entrynumber,
            MIN(timestamp) as timestamp,
            MAX(description) as description,
            MAX(docdescription) as docdescription,
            MAX(type) as type,
            SUM(debit) as total_debit,
            SUM(credit) as total_credit,
            MAX(dwh_update_timestamp) as last_update
        FROM bronze.holded_dailyledger
        GROUP BY entrynumber
        ORDER BY entrynumber
        """
        
        logger.info("Extracting journal entries from bronze.holded_dailyledger")
        df = pd.read_sql(query, conn)
        
        logger.info(f"Extracted {len(df)} journal entries from bronze layer")
        return df
    
    except Exception as e:
        logger.error(f"Error extracting bronze journal entries: {str(e)}")
        raise

def get_fiscal_periods(conn) -> Dict[Tuple[int, int], int]:
    """
    Get mapping of date ranges to period IDs for all fiscal periods.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary mapping (start_timestamp, end_timestamp) to period_id
    """
    try:
        query = """
        SELECT 
            period_id, 
            start_date, 
            end_date
        FROM silver.fiscal_periods
        ORDER BY start_date
        """
        
        logger.info("Fetching fiscal periods for mapping")
        cursor = conn.cursor()
        cursor.execute(query)
        periods = cursor.fetchall()
        cursor.close()
        
        # Create mapping of date ranges to period IDs
        period_map = {}
        
        for period_id, start_date, end_date in periods:
            # Convert dates to timestamps for comparison with journal entries
            start_timestamp = int(datetime.combine(start_date, datetime.min.time()).timestamp())
            end_timestamp = int(datetime.combine(end_date, datetime.max.time()).timestamp())
            period_map[(start_timestamp, end_timestamp)] = period_id
        
        logger.info(f"Fetched {len(period_map)} fiscal periods")
        return period_map
    
    except Exception as e:
        logger.error(f"Error fetching fiscal periods: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def transform_journal_entries(df: pd.DataFrame, period_map: Dict[Tuple[int, int], int]) -> List[Tuple]:
    """
    Transform and enrich journal entries data for silver layer.
    
    Args:
        df: DataFrame with bronze journal entries data
        period_map: Mapping of date ranges to period IDs
        
    Returns:
        List of tuples with transformed data ready for insertion
    """
    logger.info("Transforming journal entries data for silver.journal_entries")
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    transformed_data = []
    
    for _, row in df.iterrows():
        # Convert timestamp to date
        entry_date = datetime.fromtimestamp(row['timestamp']).date()
        
        # Determine fiscal period
        period_id = None
        entry_timestamp = row['timestamp']
        
        for (start_timestamp, end_timestamp), pid in period_map.items():
            if start_timestamp <= entry_timestamp <= end_timestamp:
                period_id = pid
                break
        
        # If no period found, log warning but continue
        if period_id is None:
            logger.warning(f"No fiscal period found for entry {row['entrynumber']} with date {entry_date}")
        
        # Determine if this is a special entry type
        description = str(row['description'] or '')
        is_closing_entry = "CIERRE" in description.upper() or "CLOSING" in description.upper()
        is_opening_entry = "APERTURA" in description.upper() or "OPENING" in description.upper()
        is_adjustment = "AJUSTE" in description.upper() or "ADJUSTMENT" in description.upper()
        
        # Create data tuple for insertion
        entry_data = (
            row['entrynumber'],         # entry_number
            entry_date,                 # entry_date
            row['timestamp'],           # original_timestamp
            period_id,                  # period_id
            row['type'],                # entry_type
            row['description'],         # description
            row['docdescription'],      # document_description
            is_closing_entry,           # is_closing_entry
            is_opening_entry,           # is_opening_entry
            is_adjustment,              # is_adjustment
            False,                      # is_checked - default to False
            'Posted',                   # entry_status - default to Posted
            row['total_debit'] or 0,    # total_debit
            row['total_credit'] or 0,   # total_credit
            datetime.now(),             # dwh_created_at
            datetime.now(),             # dwh_updated_at
            'bronze.holded_dailyledger', # dwh_source_table
            batch_id                    # dwh_batch_id
        )
        
        transformed_data.append(entry_data)
    
    logger.info(f"Transformation completed. {len(transformed_data)} journal entries processed")
    return transformed_data

def load_journal_entries(conn, entries_data: List[Tuple], full_refresh: bool = False) -> int:
    """
    Load transformed journal entries into silver.journal_entries table.
    
    Args:
        conn: Database connection
        entries_data: List of tuples with transformed data
        full_refresh: If True, truncate target table before loading
        
    Returns:
        Number of records inserted
    """
    if not entries_data:
        logger.warning("No data to load into silver.journal_entries")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # If full refresh, truncate target table
        if full_refresh:
            logger.info("Truncating silver.journal_entries table for full refresh")
            cursor.execute("TRUNCATE TABLE silver.journal_entries CASCADE")
        
        # Prepare insert query
        if full_refresh:
            insert_query = """
            INSERT INTO silver.journal_entries (
                entry_number, entry_date, original_timestamp, period_id,
                entry_type, description, document_description,
                is_closing_entry, is_opening_entry, is_adjustment,
                is_checked, entry_status, total_debit, total_credit,
                dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            # For incremental loads, use upsert pattern
            insert_query = """
            INSERT INTO silver.journal_entries (
                entry_number, entry_date, original_timestamp, period_id,
                entry_type, description, document_description,
                is_closing_entry, is_opening_entry, is_adjustment,
                is_checked, entry_status, total_debit, total_credit,
                dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entry_number) DO UPDATE SET
                entry_date = EXCLUDED.entry_date,
                original_timestamp = EXCLUDED.original_timestamp,
                period_id = EXCLUDED.period_id,
                entry_type = EXCLUDED.entry_type,
                description = EXCLUDED.description,
                document_description = EXCLUDED.document_description,
                is_closing_entry = EXCLUDED.is_closing_entry,
                is_opening_entry = EXCLUDED.is_opening_entry,
                is_adjustment = EXCLUDED.is_adjustment,
                total_debit = EXCLUDED.total_debit,
                total_credit = EXCLUDED.total_credit,
                dwh_updated_at = CURRENT_TIMESTAMP,
                dwh_batch_id = EXCLUDED.dwh_batch_id
            """
        
        # Execute batch insert
        for entry in entries_data:
            cursor.execute(insert_query, entry)
        
        conn.commit()
        
        # Get number of inserted records
        cursor.execute("SELECT COUNT(*) FROM silver.journal_entries")
        count = cursor.fetchone()[0]
        
        logger.info(f"Successfully loaded {count} records into silver.journal_entries")
        
        # Generate load statistics
        cursor.execute("""
            SELECT 
                date_trunc('month', entry_date)::date as month,
                COUNT(*) as entry_count,
                SUM(total_debit) as total_amount
            FROM silver.journal_entries
            GROUP BY date_trunc('month', entry_date)
            ORDER BY month
        """)
        
        month_stats = cursor.fetchall()
        logger.info("Journal entries by month:")
        for month, entry_count, total_amount in month_stats:
            logger.info(f"  {month}: {entry_count} entries, {total_amount:.2f} in total debits")
        
        cursor.close()
        return count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data into silver.journal_entries: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_journal_entries_main(full_refresh: bool = True) -> bool:
    """
    Main function to orchestrate the ETL process for journal entries.
    
    Args:
        full_refresh: If True, perform full refresh instead of incremental load
        
    Returns:
        True if load was successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"Starting journal entries ETL process at {start_time}")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Extract data from bronze layer
        df_entries = extract_bronze_journal_entries(conn)
        
        # Get fiscal periods mapping
        period_map = get_fiscal_periods(conn)
        
        # Transform data
        transformed_data = transform_journal_entries(df_entries, period_map)
        
        # Load data into silver layer
        inserted_count = load_journal_entries(conn, transformed_data, full_refresh)
        
        # Close connection
        conn.close()
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Journal entries ETL completed in {duration:.2f} seconds, {inserted_count} records loaded")
        
        return True
        
    except Exception as e:
        logger.error(f"Journal entries ETL failed: {str(e)}")
        return False

if __name__ == "__main__":
    # This allows running this module as a standalone script
    import argparse
    
    parser = argparse.ArgumentParser(description='Load journal entries into silver layer')
    parser.add_argument('--full-refresh', action='store_true', help='Perform full refresh instead of incremental load')
    
    args = parser.parse_args()
    
    success = load_journal_entries_main(full_refresh=args.full_refresh)
    exit(0 if success else 1)