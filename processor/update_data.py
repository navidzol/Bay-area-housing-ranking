#!/usr/bin/env python3
"""
Data update script for Bay Area Housing Criteria Map
This script fetches and updates data from various sources
"""

import os
from dotenv import load_dotenv
import sys
import time
import logging
import json
import requests
import psycopg2
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('update_data')

# Add data_collectors directory to Python path
sys.path.append('/app/data_collectors')
from data_collection_system import update_all_data

# Load environment variables
load_dotenv()

# Construct DATABASE_URL from components
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_name = os.environ.get('POSTGRES_DB_NAME')
db_host = os.environ.get('POSTGIS_HOST', 'postgis_db')
db_port = os.environ.get('POSTGRES_PORT', '5433')  # Note: Using 5433 for internal connection

# Build the connection string
db_url = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
if not db_url:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

# Test database connection
try:
    logger.info(f"Testing database connection to {db_host}:{db_port}")
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    logger.info(f"Database connection successful: {version[0]}")
    cursor.close()
    conn.close()
except Exception as e:
    logger.error(f"Database connection error: {e}")
    sys.exit(1)

# Check if data_collection_system.py exists (without hyphen)
data_collection_path = "/app/data_collectors/data_collection_system.py"
if not os.path.exists(data_collection_path):
    # Check for hyphenated version
    hyphen_path = "/app/data_collectors/data_collection_system.py"
    if os.path.exists(hyphen_path):
        logger.warning(f"Found hyphenated filename '{hyphen_path}', consider renaming to '{data_collection_path}'")
        data_collection_path = hyphen_path
    else:
        logger.error("Could not find data collection system module")
        sys.exit(1)

# Database helper functions
def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def check_data_source_needs_update(conn, source_name):
    """Check if a data source needs to be updated"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        SELECT 1 FROM data_sources 
        WHERE source_name = %s AND CURRENT_TIMESTAMP >= next_update
        """, (source_name,))
        
        return bool(cursor.fetchone())
    except Exception as e:
        logger.error(f"Error checking data source update status: {e}")
        return True  # If error, assume update is needed

def update_data_source(conn, source_name, update_frequency_days, url="", notes=""):
    """Update or insert a data source record"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO data_sources
        (source_name, last_updated, next_update, update_frequency, url, notes)
        VALUES (%s, CURRENT_TIMESTAMP, 
                CURRENT_TIMESTAMP + (%s || ' days')::INTERVAL, 
                (%s || ' days')::INTERVAL, %s, %s)
        ON CONFLICT (source_name) DO UPDATE
        SET last_updated = CURRENT_TIMESTAMP,
            next_update = CURRENT_TIMESTAMP + (%s || ' days')::INTERVAL,
            update_frequency = (%s || ' days')::INTERVAL,
            url = EXCLUDED.url,
            notes = EXCLUDED.notes
        """, (source_name, update_frequency_days, update_frequency_days, url, notes,
              update_frequency_days, update_frequency_days))
        
        conn.commit()
        logger.info(f"Updated data source: {source_name}")
    except Exception as e:
        logger.error(f"Error updating data source: {e}")
        conn.rollback()

def update_rating(conn, zipcode, rating_type, rating_value, confidence, source, source_url):
    """Update or insert a rating for a zipcode"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO zipcode_ratings 
        (zip, rating_type, rating_value, confidence, source, source_url, last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (zip, rating_type) DO UPDATE
        SET rating_value = EXCLUDED.rating_value,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            source_url = EXCLUDED.source_url,
            last_updated = CURRENT_TIMESTAMP
        """, (zipcode, rating_type, rating_value, confidence, source, source_url))
        
        conn.commit()
        logger.info(f"Updated {rating_type} rating for zipcode {zipcode}")
    except Exception as e:
        logger.error(f"Error updating rating: {e}")
        conn.rollback()

# Main update function
def run_update():
    """Main function to run the update process"""
    try:
        logger.info("Starting data update process")
        
        update_all_data()
        
        logger.info("Data update process completed successfully")
    except Exception as e:
        logger.error(f"Data update process failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    run_update()