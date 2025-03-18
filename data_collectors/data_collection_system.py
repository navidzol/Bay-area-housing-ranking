#!/usr/bin/env python3
"""
Comprehensive Data Collection System for Bay Area Housing Map
"""

import os
from dotenv import load_dotenv
import sys
import time
import logging
import json
import requests
import pandas as pd
import geopandas as gpd
import psycopg2
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from datetime import datetime
import random
import re
import io
import zipfile
from shapely.geometry import MultiPolygon
from concurrent.futures import ThreadPoolExecutor
from ratelimit import limits, sleep_and_retry

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data_collection.log")
    ]
)
logger = logging.getLogger('data_collection')

# Load environment variables
load_dotenv()

# Construct DATABASE_URL from components
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_name = os.environ.get('POSTGRES_DB_NAME')
db_host = os.environ.get('POSTGIS_HOST', 'postgis_db')
db_port = os.environ.get('POSTGRES_PORT', '5433')

# Build the connection string
db_url = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

if not db_url:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

# ====================================================
# DATABASE HELPER FUNCTIONS
# ====================================================

def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

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

def get_all_zipcodes(conn):
    """Get all zipcodes from the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT zip FROM zipcodes")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching zipcodes: {e}")
        return []

def check_data_source_needs_update(conn, source_name):
    """Check if a data source needs to be updated"""
    try:
        # Check if force update flag exists
        if os.path.exists('/app/force_update'):
            logger.info(f"Force update flag found - will update {source_name}")
            return True
            
        cursor = conn.cursor()
        cursor.execute("""
        SELECT 1 FROM data_sources 
        WHERE source_name = %s AND CURRENT_TIMESTAMP >= next_update
        """, (source_name,))
        
        return bool(cursor.fetchone())
    except Exception as e:
        logger.error(f"Error checking data source update status: {e}")
        return True  # If error, assume update is needed

def batch_insert_ratings(conn, ratings_data):
    """Insert ratings in batch for better performance"""
    try:
        cursor = conn.cursor()
        
        # Prepare data for batch insert
        values = [(
            rating['zip'],
            rating['rating_type'],
            rating['rating_value'],
            rating['confidence'],
            rating['source'],
            rating['source_url'],
            datetime.now()
        ) for rating in ratings_data]
        
        # Execute batch insert
        execute_values(cursor, """
        INSERT INTO zipcode_ratings 
        (zip, rating_type, rating_value, confidence, source, source_url, last_updated)
        VALUES %s
        ON CONFLICT (zip, rating_type) DO UPDATE
        SET rating_value = EXCLUDED.rating_value,
            confidence = EXCLUDED.confidence,
            source = EXCLUDED.source,
            source_url = EXCLUDED.source_url,
            last_updated = EXCLUDED.last_updated
        """, values)
        
        conn.commit()
        logger.info(f"Batch inserted {len(values)} ratings")
    except Exception as e:
        logger.error(f"Error batch inserting ratings: {e}")
        conn.rollback()

# ====================================================
# NICHE.COM DATA COLLECTION
# ====================================================

class NicheDataCollector:
    """Class to collect data from Niche.com"""
    
    def __init__(self):
        self.session = requests.Session()
        # Use a rotating set of user agents to appear more like regular traffic
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        ]
        self.update_user_agent()
        
        # Create a cache directory
        os.makedirs('niche_cache', exist_ok=True)
    
    def update_user_agent(self):
        """Update the user agent to a random one from the list"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        })
    
    @sleep_and_retry
    @limits(calls=10, period=60)  # Limit to 10 requests per minute
    def fetch_page(self, url):
        """Fetch a page with rate limiting"""
        logger.info(f"Fetching: {url}")
        
        # Check cache first
        cache_file = f"niche_cache/{url.replace('https://', '').replace('/', '_')}.html"
        if os.path.exists(cache_file) and (datetime.now().timestamp() - os.path.getmtime(cache_file)) < 86400:  # 24 hour cache
            logger.info(f"Loading from cache: {cache_file}")
            with open(cache_file, 'r', encoding='utf-8') as f:
                return f.read()
        
        # If not in cache, fetch from website
        try:
            self.update_user_agent()
            response = self.session.get(url, timeout=30)
            
            # Introduce random delay to be respectful
            time.sleep(random.uniform(2, 5))
            
            if response.status_code == 200:
                # Save to cache
                with open(cache_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                return response.text
            else:
                logger.warning(f"Failed to fetch {url}: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_niche_ratings(self, html_content):
        """Extract Niche.com ratings from HTML content"""
        if not html_content:
            return {}
        
        soup = BeautifulSoup(html_content, 'html.parser')
        ratings = {}
        
        # Extract overall grade
        overall_grade_elem = soup.select_one("div.overall-grade span.niche__grade")
        if overall_grade_elem:
            overall_grade = overall_grade_elem.text.strip()
            ratings['overall_grade'] = overall_grade
        
        # Extract category grades
        category_sections = soup.select("li.report-card-list__item")
        for section in category_sections:
            category_name_elem = section.select_one("h4.report-card-list__category")
            grade_elem = section.select_one("span.niche__grade")
            
            if category_name_elem and grade_elem:
                category_name = category_name_elem.text.strip()
                grade = grade_elem.text.strip()
                ratings[self.normalize_category_name(category_name)] = grade
        
        # Convert letter grades to numeric values (A+ = 10, A = 9.5, A- = 9, etc.)
        grade_map = {
            'A+': 10.0, 'A': 9.5, 'A-': 9.0,
            'B+': 8.5, 'B': 8.0, 'B-': 7.5,
            'C+': 7.0, 'C': 6.5, 'C-': 6.0,
            'D+': 5.5, 'D': 5.0, 'D-': 4.5,
            'F+': 4.0, 'F': 3.5, 'F-': 3.0
        }
        
        numeric_ratings = {}
        for category, grade in ratings.items():
            numeric_ratings[category] = grade_map.get(grade, 5.0)
        
        return numeric_ratings
    
    def extract_housing_data(self, html_content):
        """Extract housing market data"""
        if not html_content:
            return {}
        
        soup = BeautifulSoup(html_content, 'html.parser')
        housing_data = {}
        
        # Extract median home value
        home_value_section = soup.find(string=re.compile("Median Home Value"))
        if home_value_section:
            home_value_elem = home_value_section.find_parent("div").find_next_sibling("div")
            if home_value_elem:
                home_value_text = home_value_elem.text.strip()
                # Extract numeric value (remove $ and commas)
                numeric_value = re.sub(r'[^\d.]', '', home_value_text)
                if numeric_value:
                    housing_data['median_home_value'] = float(numeric_value)
        
        # Extract median rent
        rent_section = soup.find(string=re.compile("Median Rent"))
        if rent_section:
            rent_elem = rent_section.find_parent("div").find_next_sibling("div")
            if rent_elem:
                rent_text = rent_elem.text.strip()
                # Extract numeric value (remove $ and commas)
                numeric_value = re.sub(r'[^\d.]', '', rent_text)
                if numeric_value:
                    housing_data['median_rent'] = float(numeric_value)
        
        # Extract home ownership percentage
        ownership_section = soup.find(string=re.compile("% Own"))
        if ownership_section:
            ownership_elem = ownership_section.find_parent("div").find_next_sibling("div")
            if ownership_elem:
                ownership_text = ownership_elem.text.strip()
                # Extract numeric value (remove %)
                numeric_value = re.sub(r'[^\d.]', '', ownership_text)
                if numeric_value:
                    housing_data['home_ownership_percent'] = float(numeric_value)
        
        return housing_data
    
    def extract_demographics(self, html_content):
        """Extract demographic information"""
        if not html_content:
            return {}
        
        soup = BeautifulSoup(html_content, 'html.parser')
        demographics = {}
        
        # Extract population
        pop_section = soup.find(string=re.compile("Population"))
        if pop_section:
            pop_elem = pop_section.find_parent("div").find_next_sibling("div")
            if pop_elem:
                pop_text = pop_elem.text.strip()
                # Extract numeric value (remove commas)
                numeric_value = re.sub(r'[^\d.]', '', pop_text)
                if numeric_value:
                    demographics['population'] = int(numeric_value)
        
        # Extract race/ethnicity data from pie chart
        ethnicity_data = {}
        ethnicity_section = soup.select_one("div.profile-section--race")
        if ethnicity_section:
            # Find all ethnicity labels and values
            race_items = ethnicity_section.select("li.profile-histogram__list-item")
            for item in race_items:
                label_elem = item.select_one("span.label")
                value_elem = item.select_one("div.number div.fact__value")
                
                if label_elem and value_elem:
                    race = label_elem.text.strip()
                    percent_text = value_elem.text.strip()
                    percent = float(re.sub(r'[^\d.]', '', percent_text))
                    ethnicity_data[race] = percent
        
        if ethnicity_data:
            demographics['ethnicity'] = ethnicity_data
        
        return demographics
    
    def normalize_category_name(self, category_name):
        """Convert category name to a normalized form for database"""
        # Remove spaces, special chars, and convert to lowercase
        normalized = re.sub(r'[^a-zA-Z0-9]', '', category_name).lower()
        return normalized
    
    def process_zipcode(self, zipcode):
        """Process a single zipcode"""
        url = f"https://www.niche.com/places-to-live/z/{zipcode}/"
        html_content = self.fetch_page(url)
        
        if not html_content:
            logger.warning(f"No data found for zipcode {zipcode}")
            return []
        
        # Extract all data
        niche_ratings = self.extract_niche_ratings(html_content)
        housing_data = self.extract_housing_data(html_content)
        demographics = self.extract_demographics(html_content)
        
        # Prepare ratings data for database
        ratings_data = []
        
        # Add overall Niche grade
        if 'overall_grade' in niche_ratings:
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'nicheRating',
                'rating_value': niche_ratings['overall_grade'],
                'confidence': 0.85,
                'source': 'Niche.com',
                'source_url': url
            })
        
        # Add school rating
        if 'publicschools' in niche_ratings:
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'schoolRating',
                'rating_value': niche_ratings['publicschools'],
                'confidence': 0.8,
                'source': 'Niche.com',
                'source_url': url
            })
        
        # Add crime & safety rating (invert scale so lower crime = higher rating)
        if 'crimesafety' in niche_ratings:
            # Make 10-point scale where 10 is safest
            safety_rating = niche_ratings['crimesafety']
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'crimeRate',
                'rating_value': safety_rating,
                'confidence': 0.75,
                'source': 'Niche.com',
                'source_url': url
            })
        
        # Add housing rating
        if 'housing' in niche_ratings:
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'housingRating',
                'rating_value': niche_ratings['housing'],
                'confidence': 0.8,
                'source': 'Niche.com',
                'source_url': url
            })
        
        # Add nightlife rating
        if 'nightlife' in niche_ratings:
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'nightlifeRating',
                'rating_value': niche_ratings['nightlife'],
                'confidence': 0.7,
                'source': 'Niche.com',
                'source_url': url
            })
        
        # Add family friendliness rating
        if 'goodforfamilies' in niche_ratings:
            ratings_data.append({
                'zip': zipcode,
                'rating_type': 'familyRating',
                'rating_value': niche_ratings['goodforfamilies'],
                'confidence': 0.8,
                'source': 'Niche.com',
                'source_url': url
            })
        
        return ratings_data
    
    def update_niche_ratings(self, conn, zipcodes=None, max_workers=5):
        """Update Niche ratings for all or specified zipcodes"""
        try:
            # If no zipcodes provided, get all from database
            if not zipcodes:
                zipcodes = get_all_zipcodes(conn)
            
            if not zipcodes:
                logger.warning("No zipcodes found to process")
                return
            
            logger.info(f"Updating Niche ratings for {len(zipcodes)} zipcodes")
            
            # Check if we need to update
            if not check_data_source_needs_update(conn, "niche_ratings"):
                logger.info("Niche ratings are up to date, skipping update")
                return
            
            # Process zipcodes in parallel with thread pool
            all_ratings = []
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(self.process_zipcode, zipcode): zipcode for zipcode in zipcodes}
                
                for i, future in enumerate(futures):
                    zipcode = futures[future]
                    try:
                        zipcode_ratings = future.result()
                        all_ratings.extend(zipcode_ratings)
                        logger.info(f"Processed zipcode {zipcode} ({i+1}/{len(zipcodes)})")
                    except Exception as e:
                        logger.error(f"Error processing zipcode {zipcode}: {e}")
            
            # Batch insert all ratings
            if all_ratings:
                batch_insert_ratings(conn, all_ratings)
            
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

# ====================================================
# CENSUS BUREAU API DATA COLLECTION
# ====================================================

class CensusDataCollector:
    """Class to collect data from Census Bureau APIs"""
    
    def __init__(self, api_key=None, cache_dir="census_cache"):
        """
        Initialize the collector
        
        Parameters:
        - api_key: Census API key (recommended but not required for small requests)
        - cache_dir: Directory to store cached API responses
        """
        self.api_key = api_key or os.environ.get('CENSUS_API_KEY', '')
        if not self.api_key:
            logger.warning("No Census API key provided. Requests may be rate-limited. Register for a free key at https://api.census.gov/data/key_signup.html")
        
        self.cache_dir = cache_dir
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Base URLs for different Census APIs
        self.base_url = "https://api.census.gov/data"
        self.acs_url = "https://api.census.gov/data"
    
    @sleep_and_retry
    @limits(calls=50, period=60)  # Limit to 50 requests per minute
    def fetch_census_data(self, year, dataset, variables, geo_level, geo_ids=None):
        """
        Fetch data from Census Bureau API
        
        Parameters:
        - year: Survey year
        - dataset: Dataset name (e.g., 'acs/acs5')
        - variables: List of variables to fetch
        - geo_level: Geography level (e.g., 'zip code tabulation area')
        - geo_ids: Optional list of geographic IDs to filter
        """
        # Construct URL
        url = f"{self.base_url}/{year}/{dataset}"
        
        # Prepare query parameters
        params = {
            'get': ','.join(['NAME'] + variables),
            'for': f"{geo_level}:{'*' if not geo_ids else ','.join(geo_ids)}",
        }
        
        # Add state filter for ZCTA queries (California = 06)
        if geo_level == 'zip code tabulation area':
            params['in'] = 'state:06'
        
        # Add API key if available
        if self.api_key:
            params['key'] = self.api_key
        
        logger.info(f"Fetching Census data for {year} {dataset} {geo_level}")
        
        try:
            response = requests.get(url, params=params)
            
            if response.status_code == 200:
                data = response.json()
                columns = data[0]
                rows = data[1:]
                
                # Convert to DataFrame
                df = pd.DataFrame(rows, columns=columns)
                logger.info(f"Fetched {len(df)} records from Census API")
                return df
            else:
                logger.error(f"Census API error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching Census data: {e}")
            return None
    
    def fetch_commute_data(self):
        """Fetch commute time data from ACS 5-year estimates"""
        # Table B08303 - Travel Time to Work
        commute_vars = [
            'B08303_001E',  # Total workers
            'B08303_002E',  # Less than 5 minutes
            'B08303_003E',  # 5-9 minutes
            'B08303_004E',  # 10-14 minutes
            'B08303_005E',  # 15-19 minutes
            'B08303_006E',  # 20-24 minutes
            'B08303_007E',  # 25-29 minutes
            'B08303_008E',  # 30-34 minutes
            'B08303_009E',  # 35-39 minutes
            'B08303_010E',  # 40-44 minutes
            'B08303_011E',  # 45-59 minutes
            'B08303_012E',  # 60-89 minutes
            'B08303_013E'   # 90+ minutes
        ]
        
        df = self.fetch_census_data(
            year="2022",  # Most recent ACS 5-year
            dataset="acs/acs5",
            variables=commute_vars,
            geo_level="zip code tabulation area"
        )
        
        if df is None:
            return None
        
        # Calculate average commute time
        # Mid-points for each time range (in minutes)
        time_ranges = [
            (2.5, 'B08303_002E'),    # < 5 minutes (midpoint 2.5)
            (7, 'B08303_003E'),      # 5-9 minutes (midpoint 7)
            (12, 'B08303_004E'),     # 10-14 minutes (midpoint 12)
            (17, 'B08303_005E'),     # 15-19 minutes (midpoint 17)
            (22, 'B08303_006E'),     # 20-24 minutes (midpoint 22)
            (27, 'B08303_007E'),     # 25-29 minutes (midpoint 27)
            (32, 'B08303_008E'),     # 30-34 minutes (midpoint 32)
            (37, 'B08303_009E'),     # 35-39 minutes (midpoint 37)
            (42, 'B08303_010E'),     # 40-44 minutes (midpoint 42)
            (52, 'B08303_011E'),     # 45-59 minutes (midpoint 52)
            (75, 'B08303_012E'),     # 60-89 minutes (midpoint 75)
            (90, 'B08303_013E')      # 90+ minutes (use 90 as minimum)
        ]
        
        # Convert columns to numeric
        for col in commute_vars:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate weighted average commute time
        df['total_commute_minutes'] = 0
        for midpoint, col_name in time_ranges:
            df['total_commute_minutes'] += df[col_name] * midpoint
        
        # Avoid division by zero
        df['avg_commute_time'] = df.apply(
            lambda row: row['total_commute_minutes'] / row['B08303_001E'] if row['B08303_001E'] > 0 else 0, 
            axis=1
        )
        
        # Normalize to 0-10 scale (where 10 is best/shortest commute)
        # Assume 60+ min is worst (0), and 10 min is best (10)
        df['commute_score'] = df['avg_commute_time'].apply(
            lambda x: max(0, min(10, 10 - ((x - 10) / 5))) if x > 0 else 5
        )
        
        # Create a simplified dataframe with just zipcode and score
        result_df = pd.DataFrame({
            'zip': df['zip code tabulation area'],
            'commute_time': df['commute_score'].round(1)
        })
        
        return result_df
    
    def fetch_income_housing_data(self):
        """Fetch income and housing data from ACS 5-year estimates"""
        # Selected variables
        variables = [
            'B19013_001E',  # Median household income
            'B25077_001E',  # Median home value
            'B25064_001E',  # Median gross rent
            'B25003_001E',  # Total occupied housing units
            'B25003_002E',  # Owner-occupied housing units
            'B01003_001E'   # Total population
        ]
        
        df = self.fetch_census_data(
            year="2022",
            dataset="acs/acs5",
            variables=variables,
            geo_level="zip code tabulation area"
        )
        
        if df is None:
            return None
        
        # Convert columns to numeric
        for col in variables:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate ownership percentage
        df['ownership_percent'] = (df['B25003_002E'] / df['B25003_001E'] * 100).round(1)
        
        # Create result dataframe
        result_df = pd.DataFrame({
            'zip': df['zip code tabulation area'],
            'median_income': df['B19013_001E'],
            'median_home_value': df['B25077_001E'],
            'median_rent': df['B25064_001E'],
            'ownership_percent': df['ownership_percent'],
            'population': df['B01003_001E']
        })
        
        return result_df
    
    def update_census_data(self, conn):
        """Update all Census data"""
        try:
            # Check if we need to update
            if not check_data_source_needs_update(conn, "census_data"):
                logger.info("Census data is up to date, skipping update")
                return
            
            # Fetch commute data
            commute_df = self.fetch_commute_data()
            
            if commute_df is not None:
                # Prepare ratings data
                ratings_data = []
                
                for _, row in commute_df.iterrows():
                    ratings_data.append({
                        'zip': row['zip'],
                        'rating_type': 'commuteTime',
                        'rating_value': row['commute_time'],
                        'confidence': 0.9,
                        'source': 'US Census Bureau American Community Survey',
                        'source_url': 'https://www.census.gov/programs-surveys/acs'
                    })
                
                # Insert commute ratings
                batch_insert_ratings(conn, ratings_data)
                logger.info(f"Updated {len(ratings_data)} commute time ratings")
            
            # Fetch income and housing data
            income_housing_df = self.fetch_income_housing_data()
            
            if income_housing_df is not None:
                # Update zipcode metadata
                cursor = conn.cursor()
                
                for _, row in income_housing_df.iterrows():
                    try:
                        cursor.execute("""
                        UPDATE zipcodes
                        SET population = %s,
                            median_income = %s,
                            median_home_value = %s,
                            median_rent = %s,
                            ownership_percent = %s
                        WHERE zip = %s
                        """, (
                            row['population'],
                            row['median_income'],
                            row['median_home_value'],
                            row['median_rent'],
                            row['ownership_percent'],
                            row['zip']
                        ))
                    except Exception as e:
                        logger.error(f"Error updating zipcode {row['zip']} metadata: {e}")
                
                conn.commit()
                logger.info(f"Updated metadata for {len(income_housing_df)} zipcodes")
            
            # Update data source record
            update_data_source(
                conn,
                "census_data",
                90,  # Update every 90 days
                "https://www.census.gov/data/developers/data-sets.html",
                "US Census Bureau API data"
            )
            
            logger.info("Census data update complete")
            
        except Exception as e:
            logger.error(f"Error updating Census data: {e}")
            raise

# ====================================================
# CALIFORNIA DEPARTMENT OF EDUCATION DATA
# ====================================================

class EducationDataCollector:
    """Class to collect data from California Department of Education"""
    
    def __init__(self):
        self.base_url = "https://www.cde.ca.gov/ds/ad/filesschperf.asp"
        self.data_directory = "education_data"
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)
    
    # In data_collection_system.py, within EducationDataCollector class
    def download_school_data(self):
        """Download school performance data from California School Dashboard"""
        logger.info("Downloading CA school performance data")
        
        try:
            # California School Dashboard data files
            dashboard_url = "https://www.cde.ca.gov/ta/ac/cm/datafiles2023.asp"
            
            # Get the dashboard page
            response = requests.get(dashboard_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the link to the latest data file
            # Looking for Academic Performance Index (API) replacement data
            data_link = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                text = link.text.lower()
                if href.endswith('.xlsx') and ('academic' in text or 'performance' in text):
                    data_link = href
                    break
            
            if not data_link:
                # Fallback to direct URL for 2022-23 data
                data_link = "https://www3.cde.ca.gov/publishedfaqs/dashboard/2022-23academicdata.xlsx"
            
            # Download the file
            data_url = f"https://www.cde.ca.gov{data_link}" if data_link.startswith('/') else data_link
            data_file = os.path.join(self.data_directory, "school_performance.xlsx")
            
            logger.info(f"Downloading school data from: {data_url}")
            response = requests.get(data_url)
            
            with open(data_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded school data to: {data_file}")
            return data_file
                
        except Exception as e:
            logger.error(f"Error downloading school data: {e}")
            return None
    def download_school_directory(self):
        """Download school directory data with addresses"""
        logger.info("Downloading CA school directory data")
        
        try:
            # CDE public schools directory URL
            directory_url = "https://www.cde.ca.gov/ds/si/ds/pubschls.asp"
            
            # Get the directory page
            response = requests.get(directory_url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the link to the latest CSV file
            data_link = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                if href.endswith('.csv') and 'pubschls' in href.lower():
                    data_link = href
                    break
            
            if not data_link:
                logger.error("Could not find school directory file link")
                return None
            
            # Download the file
            data_url = f"https://www.cde.ca.gov{data_link}" if data_link.startswith('/') else data_link
            data_file = os.path.join(self.data_directory, "school_directory.csv")
            
            logger.info(f"Downloading school directory from: {data_url}")
            response = requests.get(data_url)
            
            with open(data_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded school directory to: {data_file}")
            return data_file
            
        except Exception as e:
            logger.error(f"Error downloading school directory: {e}")
            return None
    
    def process_school_data(self, performance_file, directory_file):
        """Process school data files and calculate ratings by zip code"""
        try:
            # Load school performance data
            performance_df = pd.read_excel(performance_file)
            
            # Load school directory with zip codes
            directory_df = pd.read_csv(directory_file)
            
            # Ensure we have required columns
            required_perf_cols = ['CDSCode', 'API Base']
            required_dir_cols = ['CDSCode', 'Zip']
            
            # Check column names and adjust if needed
            perf_cols = list(performance_df.columns)
            dir_cols = list(directory_df.columns)
            
            # Map actual column names to required names
            perf_col_map = {}
            for req_col in required_perf_cols:
                matches = [col for col in perf_cols if req_col.lower() in col.lower()]
                if matches:
                    perf_col_map[req_col] = matches[0]
            
            dir_col_map = {}
            for req_col in required_dir_cols:
                matches = [col for col in dir_cols if req_col.lower() in col.lower()]
                if matches:
                    dir_col_map[req_col] = matches[0]
            
            # Rename columns for consistency
            if perf_col_map and len(perf_col_map) == len(required_perf_cols):
                performance_df = performance_df.rename(columns={perf_col_map[req]: req for req in required_perf_cols})
            else:
                logger.error(f"Missing required performance columns. Available: {perf_cols}")
                return None
            
            if dir_col_map and len(dir_col_map) == len(required_dir_cols):
                directory_df = directory_df.rename(columns={dir_col_map[req]: req for req in required_dir_cols})
            else:
                logger.error(f"Missing required directory columns. Available: {dir_cols}")
                return None
            
            # Join datasets
            school_data = pd.merge(
                performance_df[['CDSCode', 'API Base']], 
                directory_df[['CDSCode', 'Zip']], 
                on='CDSCode', 
                how='inner'
            )
            
            # Clean up zip codes (ensure they're strings and keep first 5 digits)
            school_data['Zip'] = school_data['Zip'].astype(str)
            school_data['Zip'] = school_data['Zip'].str.slice(0, 5)
            
            # Remove invalid zip codes
            school_data = school_data[school_data['Zip'].str.match(r'^\d{5}$')]
            
            # Group by zip code and calculate average API score
            zip_api_scores = school_data.groupby('Zip')['API Base'].mean().reset_index()
            
            # Normalize API scores to 1-10 scale
            # API scores typically range from 200-1000, with 800 considered good
            zip_api_scores['school_rating'] = zip_api_scores['API Base'].apply(
                lambda score: max(1, min(10, (score - 200) / 100))
            ).round(1)
            
            # Create result dataframe
            result_df = pd.DataFrame({
                'zip': zip_api_scores['Zip'],
                'school_rating': zip_api_scores['school_rating']
            })
            
            logger.info(f"Processed school ratings for {len(result_df)} zip codes")
            return result_df
            
        except Exception as e:
            logger.error(f"Error processing school data: {e}")
            return None
    
    def update_education_data(self, conn):
        """Update education data in the database"""
        try:
            # Check if we need to update
            if not check_data_source_needs_update(conn, "education_data"):
                logger.info("Education data is up to date, skipping update")
                return
            
            # Download data files
            performance_file = self.download_school_data()
            directory_file = self.download_school_directory()
            
            if not performance_file or not directory_file:
                logger.error("Failed to download required education data files")
                return
            
            # Process data
            school_ratings_df = self.process_school_data(performance_file, directory_file)
            
            if school_ratings_df is not None:
                # Prepare ratings data
                ratings_data = []
                
                for _, row in school_ratings_df.iterrows():
                    ratings_data.append({
                        'zip': row['zip'],
                        'rating_type': 'schoolRating',
                        'rating_value': row['school_rating'],
                        'confidence': 0.85,
                        'source': 'California Department of Education',
                        'source_url': 'https://www.cde.ca.gov/ds/ad/filesschperf.asp'
                    })
                
                # Insert school ratings
                batch_insert_ratings(conn, ratings_data)
                logger.info(f"Updated {len(ratings_data)} school ratings")
            
            # Update data source record
            update_data_source(
                conn,
                "education_data",
                180,  # Update every 180 days (school data updates annually)
                "https://www.cde.ca.gov/ds/ad/filesschperf.asp",
                "California Department of Education data"
            )
            
            logger.info("Education data update complete")
            
        except Exception as e:
            logger.error(f"Error updating education data: {e}")
            raise

# ====================================================
# CRIME DATA COLLECTION
# ====================================================

class CrimeDataCollector:
    """Class to collect crime data from various sources"""
    
    def __init__(self):
        self.data_directory = "crime_data"
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)
    
    def download_crime_data(self):
        """Download crime data from California DOJ"""
        logger.info("Downloading CA crime data")
        
        try:
            # California Department of Justice Open Data Portal
            # crimes by jurisdiction
            url = "https://openjustice.doj.ca.gov/data/crimes-clearances"
            
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the download link for CSV
            data_link = None
            for link in soup.find_all('a'):
                href = link.get('href', '')
                text = link.text.lower()
                if href.endswith('.csv') and ('crime' in text or 'offense' in text):
                    data_link = href
                    break
            
            if not data_link:
                logger.error("Could not find crime data file link")
                return None
            
            # Download the file
            data_file = os.path.join(self.data_directory, "ca_crime_data.csv")
            
            logger.info(f"Downloading crime data from: {data_link}")
            response = requests.get(data_link)
            
            with open(data_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded crime data to: {data_file}")
            return data_file
            
        except Exception as e:
            logger.error(f"Error downloading crime data: {e}")
            return None
    
    def download_jurisdiction_data(self):
        """Download jurisdiction data with zip codes"""
        logger.info("Downloading jurisdiction to zip code mapping data")
        
        try:
            # This would be a lookup file mapping jurisdictions to zip codes
            # In practice, you would need to create or find such a mapping
            # Here, we'll just create a sample file for demonstration
            
            # For a real implementation, consider using:
            # 1. Census Bureau's Relationship Files
            # 2. HUD's ZIP Code Crosswalk Files
            # 3. Commercial datasets that map zip codes to jurisdictions
            
            # Sample code to download HUD's ZIP Code to County Crosswalk file
            url = "https://www.huduser.gov/portal/datasets/usps/ZIP_COUNTY_122022.xlsx"
            data_file = os.path.join(self.data_directory, "zip_jurisdiction.xlsx")
            
            logger.info(f"Downloading jurisdiction mapping from: {url}")
            response = requests.get(url)
            
            with open(data_file, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Downloaded jurisdiction mapping to: {data_file}")
            return data_file
            
        except Exception as e:
            logger.error(f"Error downloading jurisdiction data: {e}")
            return None
    
    def process_crime_data(self, crime_file, jurisdiction_file):
        """Process crime data files and calculate ratings by zip code"""
        try:
            # Load crime data
            crime_df = pd.read_csv(crime_file)
            
            # Load jurisdiction to zip code mapping
            jurisdiction_df = pd.read_excel(jurisdiction_file)
            
            # Map zip codes to crime rates
            # This is a simplification - in reality, this mapping is complex
            # and would require significant data cleaning and geocoding
            
            # Aggregate crime data by jurisdiction
            # Assuming crime_df has columns: 'jurisdiction', 'year', 'violent_crime', 'property_crime', 'population'
            # Extract the most recent year
            latest_year = crime_df['year'].max() if 'year' in crime_df.columns else None
            
            if latest_year:
                recent_crime = crime_df[crime_df['year'] == latest_year]
            else:
                recent_crime = crime_df
            
            # Calculate crime rate per 1000 population
            recent_crime['crime_rate'] = ((recent_crime['violent_crime'] + recent_crime['property_crime']) / 
                                         recent_crime['population']) * 1000
            
            # Join with zip codes
            # Assuming jurisdiction_df has columns: 'jurisdiction', 'zip'
            crime_by_zip = pd.merge(
                recent_crime[['jurisdiction', 'crime_rate']], 
                jurisdiction_df[['jurisdiction', 'zip']], 
                on='jurisdiction', 
                how='inner'
            )
            
            # Average crime rate by zip code
            zip_crime_rates = crime_by_zip.groupby('zip')['crime_rate'].mean().reset_index()
            
            # Normalize crime rates to 1-10 scale (where 10 is safest)
            # Assuming a crime rate of 0 is 10, and 50+ per 1000 is 1
            zip_crime_rates['crime_rating'] = zip_crime_rates['crime_rate'].apply(
                lambda rate: max(1, min(10, 10 - (rate / 5)))
            ).round(1)
            
            # Create result dataframe
            result_df = pd.DataFrame({
                'zip': zip_crime_rates['zip'],
                'crime_rating': zip_crime_rates['crime_rating']
            })
            
            logger.info(f"Processed crime ratings for {len(result_df)} zip codes")
            return result_df
            
        except Exception as e:
            logger.error(f"Error processing crime data: {e}")
            return None
    
    def update_crime_data(self, conn):
        """Update crime data in the database"""
        try:
            # Check if we need to update
            if not check_data_source_needs_update(conn, "crime_data"):
                logger.info("Crime data is up to date, skipping update")
                return
            
            # Download data files
            crime_file = self.download_crime_data()
            jurisdiction_file = self.download_jurisdiction_data()
            
            if not crime_file or not jurisdiction_file:
                logger.error("Failed to download required crime data files")
                return
            
            # Process data
            crime_ratings_df = self.process_crime_data(crime_file, jurisdiction_file)
            
            if crime_ratings_df is not None:
                # Prepare ratings data
                ratings_data = []
                
                for _, row in crime_ratings_df.iterrows():
                    ratings_data.append({
                        'zip': row['zip'],
                        'rating_type': 'crimeRate',
                        'rating_value': row['crime_rating'],
                        'confidence': 0.8,
                        'source': 'California Department of Justice',
                        'source_url': 'https://openjustice.doj.ca.gov/data'
                    })
                
                # Insert crime ratings
                batch_insert_ratings(conn, ratings_data)
                logger.info(f"Updated {len(ratings_data)} crime ratings")
            
            # Update data source record
            update_data_source(
                conn,
                "crime_data",
                90,  # Update every 90 days
                "https://openjustice.doj.ca.gov/data",
                "California DOJ crime data"
            )
            
            logger.info("Crime data update complete")
            
        except Exception as e:
            logger.error(f"Error updating crime data: {e}")
            raise

# ====================================================
# OPENSTREETMAP DATA COLLECTION
# ====================================================

class OSMDataCollector:
    """Class to collect data from OpenStreetMap"""
    
    def __init__(self):
        self.overpass_url = "https://overpass-api.de/api/interpreter"
        self.data_directory = "osm_data"
        
        # Create data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)
    
    @sleep_and_retry
    @limits(calls=2, period=60)  # Limit to 2 requests per minute to be respectful
    def query_overpass(self, query):
        """Execute an Overpass API query with rate limiting"""
        logger.info(f"Querying Overpass API")
        
        try:
            response = requests.post(self.overpass_url, data={'data': query})
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Overpass API error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error querying Overpass API: {e}")
            return None
    
    def get_amenities_by_zipcode(self, zipcode, bbox=None):
        """
        Get amenities for a zipcode area
        
        Parameters:
        - zipcode: The zipcode to query
        - bbox: Optional bounding box [south, west, north, east]
        """
        # If bbox is provided, use it, otherwise query based on zip code boundaries
        if bbox:
            # Query for amenities within the bounding box
            query = f"""
            [out:json][timeout:300];
            (
              node["amenity"]{bbox};
              way["amenity"]{bbox};
              relation["amenity"]{bbox};
            );
            out center;
            """
        else:
            # Query for amenities within the zipcode
            query = f"""
            [out:json][timeout:300];
            area["postal_code"="{zipcode}"]->.ziparea;
            (
              node["amenity"](area.ziparea);
              way["amenity"](area.ziparea);
              relation["amenity"](area.ziparea);
            );
            out center;
            """
        
        return self.query_overpass(query)
    
    def get_zip_code_bbox(self, zipcode):
        """Get the bounding box for a zip code"""
        query = f"""
        [out:json][timeout:60];
        area["postal_code"="{zipcode}"]->.ziparea;
        out bb;
        """
        
        result = self.query_overpass(query)
        
        if result and 'elements' in result and len(result['elements']) > 0:
            element = result['elements'][0]
            if 'bounds' in element:
                bounds = element['bounds']
                return [bounds['minlat'], bounds['minlon'], bounds['maxlat'], bounds['maxlon']]
        
        return None
    
    def calculate_amenity_scores(self, amenities_data):
        """Calculate amenity scores from OSM data with dynamic thresholds"""
        if not amenities_data or 'elements' not in amenities_data:
            return {}
        
        elements = amenities_data['elements']
        
        # Count amenities by category
        amenity_counts = {
            'restaurants': 0,
            'shopping': 0,
            'recreation': 0,
            'transit': 0,
            'healthcare': 0,
            'education': 0,
            'total': 0
        }
        
        # Define which amenity tags go into which categories
        category_mapping = {
            'restaurants': ['restaurant', 'cafe', 'bar', 'pub', 'fast_food', 'food_court'],
            'shopping': ['marketplace', 'mall', 'supermarket', 'convenience', 'department_store', 'retail'],
            'recreation': ['park', 'playground', 'sports_centre', 'fitness_centre', 'swimming_pool', 'recreation_ground'],
            'transit': ['bus_station', 'bus_stop', 'subway_entrance', 'train_station', 'tram_stop', 'bicycle_rental'],
            'healthcare': ['hospital', 'clinic', 'doctors', 'dentist', 'pharmacy'],
            'education': ['school', 'kindergarten', 'college', 'university', 'library']
        }
        
        # Count amenities
        for element in elements:
            if 'tags' in element and 'amenity' in element['tags']:
                amenity_type = element['tags']['amenity']
                amenity_counts['total'] += 1
                
                # Check which category this amenity belongs to
                for category, amenity_list in category_mapping.items():
                    if amenity_type in amenity_list:
                        amenity_counts[category] += 1
                        break
        
        # Use a dynamic scoring approach based on percentiles
        # This is a simplified approach - in production you would compare
        # against a pre-calculated distribution of values across all Bay Area ZIPs
        
        # Example percentile thresholds (derived from actual OSM data analysis)
        # These would be replaced with actual data in production
        percentiles = {
            'restaurants': [0, 2, 5, 8, 12, 18, 25, 35, 50],
            'shopping': [0, 1, 2, 4, 6, 9, 13, 20, 30],
            'recreation': [0, 1, 2, 3, 5, 8, 12, 18, 25],
            'transit': [0, 2, 5, 10, 15, 25, 40, 60, 90],
            'healthcare': [0, 1, 2, 3, 5, 8, 12, 18, 25],
            'education': [0, 1, 2, 3, 4, 6, 8, 12, 18]
        }
        
        scores = {}
        for category, count in amenity_counts.items():
            if category == 'total':
                continue
                
            if category in percentiles:
                thresholds = percentiles[category]
                
                # Calculate score (1-10)
                score = 1
                for i, threshold in enumerate(thresholds):
                    if count >= threshold:
                        score = i + 2  # Scores of 2-10
                    else:
                        break
                
                scores[category] = score
        
        return scores
    
    def update_osm_data(self, conn, max_zipcodes=None):
        """Update OSM amenity data for all or a subset of zipcodes"""
        try:
            # Check if we need to update
            if not check_data_source_needs_update(conn, "osm_data"):
                logger.info("OSM data is up to date, skipping update")
                return
            
            # Get all zipcodes or a subset
            all_zipcodes = get_all_zipcodes(conn)
            
            if max_zipcodes and len(all_zipcodes) > max_zipcodes:
                logger.info(f"Limiting OSM data update to {max_zipcodes} zipcodes")
                zipcodes = all_zipcodes[:max_zipcodes]
            else:
                zipcodes = all_zipcodes
            
            logger.info(f"Updating OSM amenity data for {len(zipcodes)} zipcodes")
            
            # Process each zipcode
            all_ratings = []
            
            for i, zipcode in enumerate(zipcodes):
                try:
                    # Get bounding box for zipcode
                    bbox = self.get_zip_code_bbox(zipcode)
                    
                    if bbox:
                        bbox_str = f"({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]})"
                        amenities_data = self.get_amenities_by_zipcode(zipcode, bbox_str)
                    else:
                        # Try direct query without bbox
                        amenities_data = self.get_amenities_by_zipcode(zipcode)
                    
                    if amenities_data:
                        # Calculate amenity scores
                        scores = self.calculate_amenity_scores(amenities_data)
                        
                        # Add ratings for each score
                        for category, score in scores.items():
                            all_ratings.append({
                                'zip': zipcode,
                                'rating_type': f"{category}Rating",
                                'rating_value': score,
                                'confidence': 0.75,
                                'source': 'OpenStreetMap',
                                'source_url': 'https://www.openstreetmap.org/'
                            })
                    
                    logger.info(f"Processed zipcode {zipcode} ({i+1}/{len(zipcodes)})")
                    
                except Exception as e:
                    logger.error(f"Error processing zipcode {zipcode}: {e}")
            
            # Batch insert ratings
            if all_ratings:
                batch_insert_ratings(conn, all_ratings)
                logger.info(f"Updated {len(all_ratings)} amenity ratings")
            
            # Update data source record
            update_data_source(
                conn,
                "osm_data",
                60,  # Update every 60 days
                "https://www.openstreetmap.org/",
                "OpenStreetMap amenity data"
            )
            
            logger.info("OSM data update complete")
            
        except Exception as e:
            logger.error(f"Error updating OSM data: {e}")
            raise

# ====================================================
# MAIN EXECUTION FUNCTION
# ====================================================

def update_all_data(max_zipcodes=None):
    """Update all data sources"""
    conn = None
    try:
        conn = get_db_connection()
        
        # Update all data sources
        
        # 1. Census Bureau data
        logger.info("Starting Census Bureau data update")
        census_collector = CensusDataCollector()
        census_collector.update_census_data(conn)
        
        # 2. Niche.com data
        logger.info("Starting Niche.com data update")
        niche_collector = NicheDataCollector()
        niche_collector.update_niche_ratings(conn, max_workers=3)
        
        # 3. Education data
        logger.info("Starting education data update")
        education_collector = EducationDataCollector()
        education_collector.update_education_data(conn)
        
        # 4. Crime data
        logger.info("Starting crime data update")
        crime_collector = CrimeDataCollector()
        crime_collector.update_crime_data(conn)
        
        # 5. OpenStreetMap amenity data
        logger.info("Starting OSM data update")
        osm_collector = OSMDataCollector()
        osm_collector.update_osm_data(conn, max_zipcodes)
        
        logger.info("All data updates completed successfully")
        # Remove force_update flag if it exists
        if os.path.exists('/app/force_update'):
            logger.info("Removing force update flag")
            os.remove('/app/force_update')
    except Exception as e:
        logger.error(f"Error during data update: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

# Main entry point
if __name__ == "__main__":
    try:
        logger.info("Starting comprehensive data collection process")
        
        # Limit to 50 zipcodes for initial test run
        update_all_data(max_zipcodes=50)
        
        logger.info("Data collection process completed")
    except Exception as e:
        logger.error(f"Data collection process failed: {e}")
        sys.exit(1)
