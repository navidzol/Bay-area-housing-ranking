i#!/usr/bin/env python3
"""
Data update script for Bay Area Housing Criteria Map
This script fetches and updates data from various sources
"""

import os
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

# Database connection parameters
db_url = os.environ.get('DATABASE_URL')
if not db_url:
    logger.error("DATABASE_URL environment variable not set")
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

# Data fetchers
class NicheDataFetcher:
    """Class to fetch neighborhood data from Niche.com"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def fetch_niche_data(self, zipcode):
        """Fetch data for a zipcode from Niche.com"""
        url = f"https://www.niche.com/places-to-live/z/{zipcode}/"
        logger.info(f"Fetching Niche data for zipcode {zipcode}")
        
        try:
            response = self.session.get(url, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Failed to get data for zipcode {zipcode}: {response.status_code}")
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract the overall grade
            grade_element = soup.select_one("div.overall-grade span.niche__grade")
            if not grade_element:
                logger.warning(f"No grade found for zipcode {zipcode}")
                return None
            
            grade = grade_element.text.strip()
            
            # Convert grade to numeric value (A+ = 10, A = 9.5, A- = 9, etc.)
            grade_map = {
                'A+': 10.0, 'A': 9.5, 'A-': 9.0,
                'B+': 8.5, 'B': 8.0, 'B-': 7.5,
                'C+': 7.0, 'C': 6.5, 'C-': 6.0,
                'D+': 5.5, 'D': 5.0, 'D-': 4.5,
                'F': 4.0
            }
            
            niche_rating = grade_map.get(grade, 5.0)  # Default to 5.0 if not found
            
            # Add a small delay to avoid rate limiting
            time.sleep(1)
            
            return {
                'grade': grade,
                'rating': niche_rating,
                'url': url
            }
        
        except Exception as e:
            logger.error(f"Error fetching Niche data for zipcode {zipcode}: {e}")
            return None
    
    def update_niche_ratings(self, conn, zipcodes=None):
        """Update Niche ratings for all or specified zipcodes"""
        try:
            cursor = conn.cursor()
            
            # If no zipcodes provided, get all from database
            if not zipcodes:
                cursor.execute("SELECT zip FROM zipcodes")
                zipcodes = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"Updating Niche ratings for {len(zipcodes)} zipcodes")
            
            # Check if we need to update
            if not check_data_source_needs_update(conn, "niche_ratings"):
                logger.info("Niche ratings are up to date, skipping update")
                return
            
            # Process zipcodes
            for i, zipcode in enumerate(zipcodes):
                logger.info(f"Processing zipcode {zipcode} ({i+1}/{len(zipcodes)})")
                
                niche_data = self.fetch_niche_data(zipcode)
                
                if niche_data:
                    update_rating(
                        conn,
                        zipcode,
                        'nicheRating',
                        niche_data['rating'],
                        0.8,  # Confidence
                        'Niche.com',
                        niche_data['url']
                    )
            
            # Update data source record
            update_data_source(
                conn,
                "niche_ratings",
                30,  # Update every 30 days
                "https://www.niche.com/",
                "Niche.com neighborhood ratings"
            )
            
            logger.info("Niche ratings update complete")
        
        except Exception as e:
            logger.error(f"Error updating Niche ratings: {e}")
            raise

# Main update function
def update_all_data():
    """Update all data sources"""
    conn = None
    try:
        conn = get_db_connection()
        
        # Update Niche ratings
        niche_fetcher = NicheDataFetcher()
        niche_fetcher.update_niche_ratings(conn)
        
        # Here you would add calls to other data fetchers:
        # - School ratings fetcher
        # - Crime rate fetcher
        # - Commute time calculator
        # - etc.
        
        logger.info("All data updates completed successfully")
        
    except Exception as e:
        logger.error(f"Error during data update: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        logger.info("Starting data update process")
        update_all_data()
        logger.info("Data update process completed")
    except Exception as e:
        logger.error(f"Data update process failed: {e}")
        sys.exit(1)
