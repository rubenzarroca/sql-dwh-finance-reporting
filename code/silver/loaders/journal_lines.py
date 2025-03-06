"""
Loader for silver.journal_lines table.

This module transforms individual journal entry lines from the bronze layer,
enriches them with metadata, and loads them into the silver.journal_lines table,
establishing relationships with journal entries and accounts.
"""

import os
import logging
import pandas as pd
import json
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

def extract_bronze_journal_lines(conn) -> pd.DataFrame:
    """
    Extract journal lines from bronze.holded_dailyledger, joining with silver.journal_entries
    to get the entry_id for each line.
    
    Args:
        conn: Database connection
        
    Returns:
        DataFrame with journal lines and their corresponding entry_ids
    """
    try:
        query = """
        SELECT 
            dl.entrynumber,
            dl.line,
            dl.timestamp,  -- Incluimos el timestamp para identificación única
            je.entry_id,
            dl.account,
            dl.debit,
            dl.credit,
            dl.description,
            dl.tags::text as tags,  -- Forzar conversión a texto para consistencia
            dl.checked,
            dl.dwh_update_timestamp
        FROM bronze.holded_dailyledger dl
        JOIN silver.journal_entries je ON dl.entrynumber = je.entry_number
            AND dl.timestamp = je.original_timestamp  -- Aseguramos la unión correcta
        ORDER BY dl.entrynumber, dl.timestamp, dl.line
        """
        
        logger.info("Extracting journal lines from bronze.holded_dailyledger")
        
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        cursor.close()
        
        df = pd.DataFrame(results, columns=columns)
        
        # Examinar el formato de tags para diagnóstico
        sample_tags = df['tags'].head(5).tolist()
        logger.info(f"Sample tags format: {sample_tags}")
        
        logger.info(f"Extracted {len(df)} journal lines from bronze layer")
        
        # Verificamos duplicados considerando la clave completa
        duplicate_check = df.duplicated(subset=['entrynumber', 'line', 'timestamp'])
        if duplicate_check.any():
            logger.warning(f"Found {duplicate_check.sum()} duplicate rows with identical (entrynumber, line, timestamp)")
            # Estas sí serían duplicados verdaderos ya que violan la clave primaria
            
            duplicates = df[df.duplicated(subset=['entrynumber', 'line', 'timestamp'], keep=False)]
            logger.warning(f"Detailed info about true duplicates: {duplicates.shape[0]} rows")
            
            # En este caso, mantener solo una de las filas es correcto
            df = df.drop_duplicates(subset=['entrynumber', 'line', 'timestamp'])
            logger.info(f"After removing true duplicates: {len(df)} journal lines")
        
        return df
    
    except Exception as e:
        logger.error(f"Error extracting bronze journal lines: {str(e)}")
        if 'cursor' in locals() and cursor:
            cursor.close()
        raise

def get_accounts_mapping(conn) -> Dict[int, str]:
    """
    Get mapping of account numbers to account IDs from silver.accounts.
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary mapping account_number to account_id
    """
    try:
        query = """
        SELECT account_number, account_id
        FROM silver.accounts
        """
        
        logger.info("Fetching accounts mapping")
        cursor = conn.cursor()
        cursor.execute(query)
        accounts = cursor.fetchall()
        cursor.close()
        
        # Create mapping of account numbers to account IDs
        account_map = {row[0]: row[1] for row in accounts}
        
        logger.info(f"Fetched mapping for {len(account_map)} accounts")
        return account_map
    
    except Exception as e:
        logger.error(f"Error fetching accounts mapping: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def is_tax_relevant(account_number: int) -> bool:
    """
    Determine if an account is relevant for tax calculations.
    
    Args:
        account_number: Account number
        
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

def extract_tags_as_list(tags) -> List[str]:
    """
    Extract tags from JSON or string format to a list of strings.
    
    Args:
        tags: Tags in JSON or string format
        
    Returns:
        List of tag strings
    """
    # Default empty list
    tags_list = []
    
    # Process tags if they exist
    if tags and tags != "null":
        try:
            # Parse tags from JSON if it's a string
            if isinstance(tags, str):
                # Try to handle various string formats
                if tags.strip() == "":
                    tags_list = []
                elif tags.strip().lower() == "null":
                    tags_list = []
                else:
                    try:
                        parsed_tags = json.loads(tags)
                        if isinstance(parsed_tags, list):
                            tags_list = [str(tag).strip() for tag in parsed_tags if tag]
                    except json.JSONDecodeError:
                        # If it fails, maybe it's already a stringified list
                        logger.warning(f"Failed to parse tags as JSON: {tags}")
                        # If it looks like a list but isn't valid JSON
                        if tags.startswith('[') and tags.endswith(']'):
                            # Basic extraction with string operations
                            items = tags[1:-1].split(',')
                            tags_list = [item.strip(' "\'') for item in items if item.strip()]
            elif isinstance(tags, list):
                tags_list = [str(tag).strip() for tag in tags if tag]
        except Exception as e:
            logger.warning(f"Error processing tags: {e}")
    
    return tags_list

def extract_business_metadata_from_tags(tags) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract business metadata from the tags.
    
    Args:
        tags: Tags in any format
        
    Returns:
        Tuple of (cost_center, business_line)
    """
    # Default values
    cost_center = None
    business_line = None
    
    # Get tags as list
    tags_list = extract_tags_as_list(tags)
    
    # Process tag list to extract business metadata
    for tag in tags_list:
        if tag.startswith("CC:"):
            cost_center = tag[3:]
        elif tag.startswith("BL:"):
            business_line = tag[3:]
    
    return cost_center, business_line

def transform_journal_lines(df: pd.DataFrame, account_map: Dict[int, str]) -> List[Tuple]:
    """
    Transform and enrich journal lines data for silver layer.
    
    Args:
        df: DataFrame with bronze journal lines data
        account_map: Mapping of account numbers to account IDs
        
    Returns:
        List of tuples with transformed data ready for insertion
    """
    logger.info("Transforming journal lines data for silver.journal_lines")
    
    batch_id = datetime.now().strftime("%Y%m%d%H%M%S")
    transformed_data = []
    skipped_lines = 0
    
    # Track processed keys to avoid duplicates
    processed_keys = set()
    
    for _, row in df.iterrows():
        # Check for duplicate unique key
        key = (row['entry_id'], row['line'], row['timestamp'])
        if key in processed_keys:
            logger.warning(f"Skipping duplicate key during transform: entry_id={row['entry_id']}, line={row['line']}, timestamp={row['timestamp']}")
            skipped_lines += 1
            continue
        
        # Add to processed keys
        processed_keys.add(key)
        
        # Get account_id from account number
        account_number = row['account']
        
        if account_number is None:
            logger.warning(f"Missing account number for entry {row['entrynumber']} line {row['line']}, skipping")
            skipped_lines += 1
            continue
            
        account_id = account_map.get(account_number)
        
        if not account_id:
            logger.warning(f"Account {account_number} not found in silver.accounts for entry {row['entrynumber']} line {row['line']}, skipping")
            skipped_lines += 1
            continue
        
        # Determine if account is tax relevant
        tax_relevant = is_tax_relevant(account_number)
        
        # Determine reconciliation status
        is_reconciled = row['checked'] == 'Yes' if row['checked'] else False
        is_checked = is_reconciled  # For now, use same value
        
        # Extract individual tags
        tags_list = extract_tags_as_list(row['tags'])
        
        # Get individual tags (up to 4)
        tag1 = tags_list[0] if len(tags_list) > 0 else None
        tag2 = tags_list[1] if len(tags_list) > 1 else None
        tag3 = tags_list[2] if len(tags_list) > 2 else None
        tag4 = tags_list[3] if len(tags_list) > 3 else None
        
        # Process tags to extract business metadata
        cost_center, business_line = extract_business_metadata_from_tags(row['tags'])
        
        # Ensure tags is a valid JSON string for database storage
        try:
            tags_json = json.dumps(tags_list)
        except Exception as e:
            logger.warning(f"Error serializing tags to JSON: {e}")
            tags_json = '[]'
        
        # Create data tuple for insertion
        line_data = (
            row['entry_id'],           # entry_id
            row['line'],               # line_number
            account_id,                # account_id
            account_number,            # account_number
            row['debit'] or 0,         # debit_amount
            row['credit'] or 0,        # credit_amount
            row['description'],        # description
            tags_json,                 # tags (as JSON string)
            tag1,                      # tag1
            tag2,                      # tag2
            tag3,                      # tag3
            tag4,                      # tag4
            is_checked,                # is_checked
            is_reconciled,             # is_reconciled
            tax_relevant,              # is_tax_relevant
            cost_center,               # cost_center
            business_line,             # business_line
            datetime.now(),            # dwh_created_at
            datetime.now(),            # dwh_updated_at
            'bronze.holded_dailyledger', # dwh_source_table
            batch_id                   # dwh_batch_id
        )
        
        transformed_data.append(line_data)
    
    logger.info(f"Transformation completed. {len(transformed_data)} journal lines processed, {skipped_lines} skipped")
    return transformed_data

def load_journal_lines(conn, lines_data: List[Tuple], full_refresh: bool = False) -> int:
    """
    Load transformed journal lines into silver.journal_lines table.
    
    Args:
        conn: Database connection
        lines_data: List of tuples with transformed data
        full_refresh: If True, truncate target table before loading
        
    Returns:
        Number of records inserted
    """
    if not lines_data:
        logger.warning("No data to load into silver.journal_lines")
        return 0
    
    try:
        cursor = conn.cursor()
        
        # Make sure we have a clean slate for the transaction
        conn.rollback()
        
        # If full refresh, truncate target table in its own transaction
        if full_refresh:
            logger.info("Truncating silver.journal_lines table for full refresh")
            cursor.execute("TRUNCATE TABLE silver.journal_lines")
            conn.commit()
        
        # Prepare insert query with proper JSONB casting for tags
        if full_refresh:
            insert_query = """
            INSERT INTO silver.journal_lines (
                entry_id, line_number, account_id, account_number,
                debit_amount, credit_amount, description, tags,
                tag1, tag2, tag3, tag4,
                is_checked, is_reconciled, is_tax_relevant,
                cost_center, business_line,
                dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        else:
            # For incremental loads, use upsert pattern
            insert_query = """
            INSERT INTO silver.journal_lines (
                entry_id, line_number, account_id, account_number,
                debit_amount, credit_amount, description, tags,
                tag1, tag2, tag3, tag4,
                is_checked, is_reconciled, is_tax_relevant,
                cost_center, business_line,
                dwh_created_at, dwh_updated_at, dwh_source_table, dwh_batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (entry_id, line_number) DO UPDATE SET
                account_id = EXCLUDED.account_id,
                account_number = EXCLUDED.account_number,
                debit_amount = EXCLUDED.debit_amount,
                credit_amount = EXCLUDED.credit_amount,
                description = EXCLUDED.description,
                tags = EXCLUDED.tags,
                tag1 = EXCLUDED.tag1,
                tag2 = EXCLUDED.tag2,
                tag3 = EXCLUDED.tag3,
                tag4 = EXCLUDED.tag4,
                is_checked = EXCLUDED.is_checked,
                is_reconciled = EXCLUDED.is_reconciled,
                is_tax_relevant = EXCLUDED.is_tax_relevant,
                cost_center = EXCLUDED.cost_center,
                business_line = EXCLUDED.business_line,
                dwh_updated_at = CURRENT_TIMESTAMP,
                dwh_batch_id = EXCLUDED.dwh_batch_id
            """
        
        # Track processed keys again to ensure no duplicates during loading
        processed_keys = set()
        
        # Execute batch insert
        batch_size = 1000  # Process in batches to avoid memory issues
        total_inserted = 0
        
        for i in range(0, len(lines_data), batch_size):
            batch = []
            for line in lines_data[i:i+batch_size]:
                key = (line[0], line[1])  # entry_id, line_number
                if key not in processed_keys:
                    processed_keys.add(key)
                    batch.append(line)
                else:
                    logger.warning(f"Skipping duplicate key during load: entry_id={line[0]}, line_number={line[1]}")
            
            # Process the batch
            if batch:
                for line in batch:
                    cursor.execute(insert_query, line)
                
                conn.commit()
                total_inserted += len(batch)
                logger.info(f"Inserted batch of {len(batch)} lines, total progress: {total_inserted}/{len(lines_data)}")
        
        # Get total count of lines
        cursor.execute("SELECT COUNT(*) FROM silver.journal_lines")
        count = cursor.fetchone()[0]
        
        logger.info(f"Successfully loaded {count} total records in silver.journal_lines")
        
        # Generate statistics by account type
        cursor.execute("""
            SELECT 
                a.account_type,
                COUNT(*) as line_count,
                SUM(jl.debit_amount) as total_debit,
                SUM(jl.credit_amount) as total_credit
            FROM silver.journal_lines jl
            JOIN silver.accounts a ON jl.account_id = a.account_id
            GROUP BY a.account_type
            ORDER BY a.account_type
        """)
        
        type_stats = cursor.fetchall()
        logger.info("Journal lines by account type:")
        for account_type, line_count, total_debit, total_credit in type_stats:
            logger.info(f"  {account_type}: {line_count} lines, {total_debit:.2f} debit, {total_credit:.2f} credit")
        
        # Generate statistics on tag usage
        cursor.execute("""
            SELECT 
                COUNT(*) FILTER (WHERE tag1 IS NOT NULL) as tag1_count,
                COUNT(*) FILTER (WHERE tag2 IS NOT NULL) as tag2_count,
                COUNT(*) FILTER (WHERE tag3 IS NOT NULL) as tag3_count,
                COUNT(*) FILTER (WHERE tag4 IS NOT NULL) as tag4_count
            FROM silver.journal_lines
        """)
        
        tag_stats = cursor.fetchone()
        logger.info("Tag usage statistics:")
        logger.info(f"  Tag1: {tag_stats[0]} lines")
        logger.info(f"  Tag2: {tag_stats[1]} lines")
        logger.info(f"  Tag3: {tag_stats[2]} lines")
        logger.info(f"  Tag4: {tag_stats[3]} lines")
        
        cursor.close()
        return count
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data into silver.journal_lines: {str(e)}")
        if 'cursor' in locals():
            cursor.close()
        raise

def load_journal_lines_main(full_refresh: bool = True) -> bool:
    """
    Main function to orchestrate the ETL process for journal lines.
    
    Args:
        full_refresh: If True, perform full refresh instead of incremental load
        
    Returns:
        True if load was successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"Starting journal lines ETL process at {start_time}")
    
    try:
        # Get database connection
        conn = get_db_connection()
        
        # Extract data from bronze layer with journal entry IDs
        df_lines = extract_bronze_journal_lines(conn)
        
        # Get accounts mapping
        account_map = get_accounts_mapping(conn)
        
        # Transform data
        transformed_data = transform_journal_lines(df_lines, account_map)
        
        # Load data into silver layer
        inserted_count = load_journal_lines(conn, transformed_data, full_refresh)
        
        # Close connection
        conn.close()
        
        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Journal lines ETL completed in {duration:.2f} seconds, {inserted_count} records loaded")
        
        return True
        
    except Exception as e:
        logger.error(f"Journal lines ETL failed: {str(e)}")
        if 'conn' in locals() and not conn.closed:
            conn.close()
        return False

if __name__ == "__main__":
    # This allows running this module as a standalone script
    import argparse
    
    parser = argparse.ArgumentParser(description='Load journal lines into silver layer')
    parser.add_argument('--full-refresh', action='store_true', help='Perform full refresh instead of incremental load')
    
    args = parser.parse_args()
    
    success = load_journal_lines_main(full_refresh=args.full_refresh)
    exit(0 if success else 1)