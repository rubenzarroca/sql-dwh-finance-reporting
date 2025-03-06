"""
Loader for silver.fiscal_periods table.

This module generates and loads fiscal periods into the silver layer
based on the date range found in transaction data from the bronze layer.
"""

import os
import logging
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import utility functions
from utils import get_db_connection

def determine_date_range(conn) -> Tuple[date, date]:
    """
    Determine the date range for which to generate fiscal periods
    based on transaction dates in the bronze layer.
    
    Args:
        conn: Database connection
        
    Returns:
        Tuple with start_date and end_date
    """
    try:
        # Query the earliest transaction date in journal entries
        logger.info("Determining date range from bronze.holded_dailyledger")
        cursor = conn.cursor()
        
        # Get the minimum timestamp (earliest transaction)
        cursor.execute("""
            SELECT MIN(timestamp), MAX(timestamp)
            FROM bronze.holded_dailyledger
        """)
        
        min_timestamp, max_timestamp = cursor.fetchone()
        
        if min_timestamp is None:
            # No data available, use a default range
            logger.warning("No transaction data found. Using default date range.")
            today = date.today()
            start_date = date(today.year - 1, 1, 1)  # First day of previous year
            end_date = date(today.year + 1, 12, 31)  # Last day of next year
        else:
            # Convert timestamps to dates
            min_date = datetime.fromtimestamp(min_timestamp).date()
            max_date = datetime.fromtimestamp(max_timestamp).date()
            
            # Set start date to first day of the month for the earliest transaction
            start_date = date(min_date.year, min_date.month, 1)
            
            # Set end date to include the current date plus one year (for future transactions)
            today = date.today()
            end_date = date(today.year + 1, 12, 31)
            
            logger.info(f"Date range determined: {start_date} to {end_date}")
        
        cursor.close()
        return start_date, end_date
        
    except Exception as e:
        logger.error(f"Error determining date range: {str(e)}")
        raise

def generate_fiscal_periods(start_date: date, end_date: date) -> List[Tuple]:
    """
    Generate fiscal periods for the specified date range.
    
    Args:
        start_date: Start date for fiscal periods
        end_date: End date for fiscal periods
        
    Returns:
        List of tuples with fiscal period data
    """
    logger.info(f"Generating fiscal periods from {start_date} to {end_date}")
    
    periods = []
    current_date = start_date
    
    # Generate periods until we reach the end date
    while current_date <= end_date:
        year = current_date.year
        month = current_date.month
        quarter = (month - 1) // 3 + 1
        
        # Calculate end date of this month
        if month == 12:
            last_day = 31
        else:
            next_month = date(year if month < 12 else year + 1, 
                            (month % 12) + 1, 1)
            last_day = (next_month - timedelta(days=1)).day
        
        end_of_month = date(year, month, last_day)
        
        # Create period name (e.g., "2023-01" for January 2023)
        period_name = f"{year}-{month:02d}"
        
        # Determine if period is closed (historical periods)
        today = date.today()
        is_closed = end_of_month < date(today.year, today.month, 1)
        
        # Create fiscal period tuple
        period = (
            year,                # period_year
            quarter,             # period_quarter
            month,               # period_month
            period_name,         # period_name
            current_date,        # start_date
            end_of_month,        # end_date
            is_closed,           # is_closed
            None if not is_closed else end_of_month  # closing_date
        )
        
        periods.append(period)
        
        # Move to next month
        if month == 12:
            current_date = date(year + 1, 1, 1)
        else:
            current_date = date(year, month + 1, 1)
    
    logger.info(f"Generated {len(periods)} fiscal periods")
    return periods

def load_fiscal_periods(conn, periods: List[Tuple], full_refresh: bool = False) -> int:
    """
    Load fiscal periods into the silver.fiscal_periods table.
    
    Args:
        conn: Database connection
        periods: List of fiscal period tuples
        full_refresh: If True, truncate the table before inserting
        
    Returns:
        Number of periods loaded
    """
    if not periods:
        logger.warning("No fiscal periods to load")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # If full refresh, truncate the table
        if full_refresh:
            logger.info("Truncating silver.fiscal_periods for full refresh")
            cursor.execute("TRUNCATE TABLE silver.fiscal_periods CASCADE")
        
        # Insert or update periods
        for period in periods:
            year, quarter, month, name, start, end, closed, closing = period
            
            if full_refresh:
                # Simple insert for full refresh
                cursor.execute("""
                    INSERT INTO silver.fiscal_periods 
                    (period_year, period_quarter, period_month, period_name, 
                     start_date, end_date, is_closed, closing_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, period)
            else:
                # Upsert for incremental refresh
                cursor.execute("""
                    INSERT INTO silver.fiscal_periods 
                    (period_year, period_quarter, period_month, period_name, 
                     start_date, end_date, is_closed, closing_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (period_year, period_month) DO UPDATE SET
                        period_quarter = EXCLUDED.period_quarter,
                        period_name = EXCLUDED.period_name,
                        start_date = EXCLUDED.start_date,
                        end_date = EXCLUDED.end_date,
                        is_closed = EXCLUDED.is_closed,
                        closing_date = EXCLUDED.closing_date
                """, period)
        
        # Commit changes
        conn.commit()
        
        # Get count of periods in the table
        cursor.execute("SELECT COUNT(*) FROM silver.fiscal_periods")
        count = cursor.fetchone()[0]
        
        logger.info(f"Successfully loaded {count} fiscal periods")
        
        # Log period distribution
        cursor.execute("""
            SELECT period_year, COUNT(*) 
            FROM silver.fiscal_periods 
            GROUP BY period_year 
            ORDER BY period_year
        """)
        
        year_stats = cursor.fetchall()
        logger.info("Fiscal periods by year:")
        for year, year_count in year_stats:
            logger.info(f"  {year}: {year_count} periods")
        
        cursor.close()
        return count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading fiscal periods: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_fiscal_periods_main(full_refresh: bool = True) -> bool:
    """
    Main function to orchestrate the generation and loading of fiscal periods.
    
    Args:
        full_refresh: If True, perform full refresh instead of incremental load
        
    Returns:
        True if successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"Starting fiscal periods ETL process at {start_time}")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Determine date range
        start_date, end_date = determine_date_range(conn)
        
        # Generate fiscal periods
        periods = generate_fiscal_periods(start_date, end_date)
        
        # Load periods into silver layer
        loaded_count = load_fiscal_periods(conn, periods, full_refresh)
        
        # Close connection
        conn.close()
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Fiscal periods ETL completed in {duration:.2f} seconds, {loaded_count} periods loaded")
        
        return True
        
    except Exception as e:
        logger.error(f"Fiscal periods ETL failed: {str(e)}")
        return False

if __name__ == "__main__":
    # This allows running this module as a standalone script
    import argparse
    
    parser = argparse.ArgumentParser(description='Load fiscal periods into silver layer')
    parser.add_argument('--full-refresh', action='store_true', help='Perform full refresh instead of incremental load')
    
    args = parser.parse_args()
    
    success = load_fiscal_periods_main(full_refresh=args.full_refresh)
    exit(0 if success else 1)