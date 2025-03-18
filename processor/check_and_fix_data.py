#!/usr/bin/env python3
"""
Script to check and fix database issues
Ensures data integrity and loads initial data if missing
"""

import os
from dotenv import load_dotenv
import sys
import logging
import psycopg2
import time
import traceback

# Initialize logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('check_and_fix_data')

# Load environment variables
load_dotenv()

# Database connection parameters
db_user = os.environ.get('POSTGRES_USER', 'bayarea_housing')
db_password = os.environ.get('POSTGRES_PASSWORD', 'password')
db_name = os.environ.get('POSTGRES_DB_NAME', 'bayarea_housing_db')
db_host = os.environ.get('POSTGIS_HOST', 'postgis_db')
db_port = os.environ.get('POSTGRES_PORT', '5433')

# Build the connection string
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

# Retry configuration
max_retries = 5
retry_interval = 3  # seconds

def get_db_connection_with_retry():
    """Get a database connection with retry logic"""
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(db_url)
            logger.info("Successfully connected to database")
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                logger.info(f"Retrying in {retry_interval} seconds...")
                time.sleep(retry_interval)
            else:
                logger.error(f"Failed to connect after {max_retries} attempts: {e}")
                raise

def check_and_fix_data():
    """Check and fix data issues"""
    conn = None
    try:
        conn = get_db_connection_with_retry()
        cursor = conn.cursor()
        
        # Check if zipcodes table has data
        cursor.execute("SELECT COUNT(*) FROM zipcodes")
        zipcode_count = cursor.fetchone()[0]
        
        if zipcode_count == 0:
            logger.warning("No zipcodes found in database. Loading zipcode data...")
            
            # Check if we should use the load_zipcode_data.py script
            zipcode_loader_path = os.path.join(os.path.dirname(__file__), 'load_zipcode_data.py')
            
            if os.path.exists(zipcode_loader_path):
                logger.info("Found load_zipcode_data.py script. Using it to load data...")
                
                # Import the load_zipcode_data module dynamically
                sys.path.append(os.path.dirname(zipcode_loader_path))
                try:
                    from load_zipcode_data import download_zcta_data, load_bay_area_zipcodes, download_commute_data, insert_zipcodes_into_db
                    
                    # Download zipcode data
                    try:
                        shapefile_path = download_zcta_data()
                        
                        # Load Bay Area zipcodes
                        gdf = load_bay_area_zipcodes(shapefile_path)
                        
                        # Download commute data
                        commute_df = download_commute_data()
                        
                        # Insert into database
                        insert_zipcodes_into_db(gdf, commute_df, conn)
                        
                        logger.info("Zipcode data loaded successfully")
                    except Exception as e:
                        logger.error(f"Error loading zipcode data: {e}")
                        logger.error(traceback.format_exc())
                except ImportError as e:
                    logger.error(f"Error importing load_zipcode_data functions: {e}")
                    logger.error(traceback.format_exc())
            else:
                logger.warning("load_zipcode_data.py not found. Unable to load initial zipcode data.")
                logger.warning("You'll need to load zipcode data manually or run the data update script.")
        else:
            logger.info(f"Found {zipcode_count} zipcodes in database")
            
            # Check for orphaned ratings (ratings without zipcode)
            cursor.execute("""
            SELECT COUNT(*) FROM zipcode_ratings zr 
            LEFT JOIN zipcodes z ON zr.zip = z.zip 
            WHERE z.zip IS NULL
            """)
            
            orphaned_count = cursor.fetchone()[0]
            
            if orphaned_count > 0:
                logger.warning(f"Found {orphaned_count} ratings with no matching zipcode. Cleaning up...")
                cursor.execute("""
                DELETE FROM zipcode_ratings
                WHERE zip IN (
                    SELECT zr.zip FROM zipcode_ratings zr
                    LEFT JOIN zipcodes z ON zr.zip = z.zip
                    WHERE z.zip IS NULL
                )
                """)
                conn.commit()
                logger.info("Orphaned ratings removed")
            
            # Check for issues with data_sources table
            cursor.execute("SELECT COUNT(*) FROM data_sources")
            source_count = cursor.fetchone()[0]
            
            if source_count == 0:
                logger.info("Initializing data_sources table with default sources")
                # Add default data sources with reasonable update intervals
                sources = [
                    ("census_data", 90, "https://www.census.gov/data/developers/data-sets.html", "Census Bureau data"),
                    ("niche_ratings", 30, "https://www.niche.com/", "Niche.com neighborhood ratings"),
                    ("education_data", 180, "https://www.cde.ca.gov/ds/ad/filesschperf.asp", "Education data"),
                    ("crime_data", 60, "https://openjustice.doj.ca.gov/data", "Crime data"),
                    ("osm_data", 60, "https://www.openstreetmap.org/", "OpenStreetMap data")
                ]
                
                for source_name, days, url, notes in sources:
                    # Fixed: Use proper interval casting syntax
                    cursor.execute("""
                    INSERT INTO data_sources
                    (source_name, last_updated, next_update, update_frequency, url, notes)
                    VALUES (%s, NOW() - INTERVAL '7 days', NOW() - INTERVAL '1 day', %s::text::interval, %s, %s)
                    ON CONFLICT (source_name) DO NOTHING
                    """, (source_name, f'{days} days', url, notes))
                
                conn.commit()
                logger.info("Data sources initialized to trigger immediate updates")
        
        # Set additional diagnostic info
        conn.commit()
        logger.info("Data check and fix completed successfully")
        
    except Exception as e:
        logger.error(f"Error checking and fixing data: {e}")
        logger.error(traceback.format_exc())
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    check_and_fix_data()