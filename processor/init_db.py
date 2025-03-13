#!/usr/bin/env python3
"""
Database initialization script for Bay Area Housing Criteria Map
This script ensures the database has the necessary tables and schema
"""

import os
import sys
import time
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('init_db')

# Database connection parameters
db_url = os.environ.get('DATABASE_URL')
if not db_url:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

# Retry connection a few times
max_retries = 5
retry_interval = 5  # seconds

def connect_with_retry():
    """Connect to PostgreSQL with retry logic"""
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(db_url)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logger.info("Successfully connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect after {max_retries} attempts: {e}")
                raise

# Initialize database schema
def init_database():
    """Initialize database schema if not already present"""
    conn = None
    try:
        conn = connect_with_retry()
        cursor = conn.cursor()
        
        # Check if PostGIS extension is installed
        cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'postgis'")
        if not cursor.fetchone():
            logger.info("Creating PostGIS extension")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        
        # Create zipcodes table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS zipcodes (
            zip TEXT PRIMARY KEY,
            name TEXT,
            county TEXT,
            state TEXT,
            geometry GEOMETRY(MULTIPOLYGON, 4326),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create zipcode_ratings table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS zipcode_ratings (
            id SERIAL PRIMARY KEY,
            zip TEXT REFERENCES zipcodes(zip),
            rating_type TEXT NOT NULL,
            rating_value REAL,
            confidence REAL,
            source TEXT,
            source_url TEXT,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Create indices for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_zip ON zipcode_ratings(zip)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_type ON zipcode_ratings(rating_type)")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_zipcode_ratings_zip_type ON zipcode_ratings(zip, rating_type)")
        
        # Create data_sources table to track when sources were last updated
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_sources (
            source_name TEXT PRIMARY KEY,
            last_updated TIMESTAMP,
            next_update TIMESTAMP,
            update_frequency INTERVAL,
            url TEXT,
            notes TEXT
        )
        """)
        
        conn.commit()
        logger.info("Database schema initialization complete")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        logger.info("Starting database initialization")
        init_database()
        logger.info("Database initialization completed successfully")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)
