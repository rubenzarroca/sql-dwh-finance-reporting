"""
Utility functions for silver layer data loaders.
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establish connection to the database using environment variables.
    
    Returns:
        Database connection object
    """
    try:
        conn = psycopg2.connect(
            host=os.environ.get("SUPABASE_DB_HOST"),
            database=os.environ.get("SUPABASE_DB_NAME", "postgres"),
            user=os.environ.get("SUPABASE_DB_USER"),
            password=os.environ.get("SUPABASE_DB_PASSWORD"),
            port=os.environ.get("SUPABASE_DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        raise