#!/usr/bin/env python3
"""
Database initialization script for Bay Area Housing Criteria Map
This script ensures the database has the necessary tables and schema
"""

import os
from dotenv import load_dotenv
import sys
import time
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import traceback

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('init_db')

# Load environment variables
load_dotenv()

# Construct database connection parameters
db_user = os.environ.get('POSTGRES_USER', 'bayarea_housing')
db_password = os.environ.get('POSTGRES_PASSWORD', 'password')
db_name = os.environ.get('POSTGRES_DB_NAME', 'bayarea_housing_db')
db_host = os.environ.get('POSTGIS_HOST', 'postgis_db')
db_port = os.environ.get('POSTGRES_PORT', '5433')

# Build the connection string
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Retry configuration
max_retries = 15  # Increased from 10 to 15
retry_interval = 5  # seconds

def connect_with_retry():
    """Connect to PostgreSQL with retry logic"""
    logger.info(f"Attempting to connect to PostgreSQL at {db_host}:{db_port}")
    logger.info(f"Database user: {db_user}, database name: {db_name}")
    
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

def check_schema_file():
    """Check if schema file exists and is readable"""
    schema_file = '/app/data_collectors/database-schema.sql'
    
    if os.path.exists(schema_file):
        try:
            with open(schema_file, 'r') as f:
                content = f.read()
                if content.strip():
                    logger.info(f"Schema file exists and contains {len(content)} characters")
                    return True, schema_file
                else:
                    logger.warning(f"Schema file exists but is empty")
                    return False, schema_file
        except Exception as e:
            logger.error(f"Error reading schema file: {e}")
            return False, schema_file
    else:
        logger.warning(f"Schema file {schema_file} not found")
        # Try alternative locations
        alt_paths = [
            '/app/database-schema.sql',
            '/app/processor/database-schema.sql',
            '/data_collectors/database-schema.sql'
        ]
        
        for path in alt_paths:
            if os.path.exists(path):
                logger.info(f"Found alternative schema file at {path}")
                return True, path
                
        return False, schema_file

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
        else:
            logger.info("PostGIS extension already installed")
        
        # Check if schema already exists
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'zipcodes'")
        has_tables = cursor.fetchone()[0] > 0
        
        if has_tables:
            logger.info("Tables already exist in database, skipping schema creation")
            return
            
        # Check and load schema file
        schema_exists, schema_file = check_schema_file()
        
        if schema_exists:
            logger.info(f"Loading schema from {schema_file}")
            with open(schema_file, 'r') as f:
                schema_sql = f.read()
                cursor.execute(schema_sql)
            logger.info("Schema loaded from file")
        else:
            logger.warning("Schema file not found, creating minimal schema")
            
            # Create basic tables if schema file is not available
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS zipcodes (
                zip TEXT PRIMARY KEY,
                name TEXT,
                county TEXT,
                state TEXT,
                geometry GEOMETRY(MULTIPOLYGON, 4326),
                population INTEGER,
                median_income NUMERIC,
                median_home_value NUMERIC,
                median_rent NUMERIC,
                ownership_percent NUMERIC(5,2),
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
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
        
        # Create indices for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_zip ON zipcode_ratings(zip)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_type ON zipcode_ratings(rating_type)")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_zipcode_ratings_zip_type ON zipcode_ratings(zip, rating_type)")
        
        # Create spatial index on zipcode geometries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_zipcodes_geometry ON zipcodes USING GIST(geometry)")
        
        conn.commit()
        logger.info("Database schema initialization complete")
        
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        logger.error(traceback.format_exc())
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