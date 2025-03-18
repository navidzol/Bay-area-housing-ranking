#!/usr/bin/env python3
"""
Crime Data Collector for Bay Area
Collects crime statistics from open data portals for Bay Area counties and cities
"""

import os
from dotenv import load_dotenv
import sys
import time
import logging
import json
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import argparse
import sqlite3
from sodapy import Socrata
import geopandas as gpd
from shapely.geometry import Point

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crime_collector.log")
    ]
)
logger = logging.getLogger('crime_collector')

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

class CrimeDataCollector:
    """Collects crime data from open data portals for Bay Area counties and cities"""
    
    def __init__(self, cache_dir="crime_cache"):
        """
        Initialize the collector
        
        Parameters:
        - cache_dir: Directory to store cached data
        """
        self.cache_dir = cache_dir
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Initialize cache database
        self.init_cache_db()
        
        # Define data sources for each jurisdiction
        self.data_sources = {
            'sf': {
                'name': 'San Francisco',
                'domain': 'data.sfgov.org',
                'dataset_id': 'wg3w-h783',  # SF Police Department Incident Reports
                'limit': 50000
            },
            'oakland': {
                'name': 'Oakland',
                'domain': 'data.oaklandca.gov',
                'dataset_id': '3xav-7geq',  # Oakland Crime Incidents
                'limit': 50000
            },
            'san_jose': {
                'name': 'San Jose',
                'domain': 'data.sanjoseca.gov',
                'dataset_id': 'gqp9-crw5',  # San Jose Police Calls for Service
                'limit': 50000
            },
            'berkeley': {
                'name': 'Berkeley',
                'domain': 'data.cityofberkeley.info',
                'dataset_id': 'k2nh-s5h5',  # Berkeley Police Department Calls for Service
                'limit': 20000
            }
            # Add more jurisdictions as needed
        }
        
        # Mapping from source-specific crime types to standardized categories
        # In crime-data-collector.py, find self.crime_category_map in __init__ method
		self.crime_category_map = {
			# Part I Violent Crimes
			'homicide': 'violent',
			'murder': 'violent',
			'manslaughter': 'violent',
			'robbery': 'violent',
			'assault': 'violent',
			'aggravated assault': 'violent',
			'rape': 'violent',
			'sexual assault': 'violent',
			
			# Part I Property Crimes
			'burglary': 'property',
			'theft': 'property',
			'larceny': 'property',
			'motor vehicle theft': 'property',
			'arson': 'property',
			
			# Part II Offenses
			'drug': 'drugs',
			'narcotics': 'drugs',
			'disorderly conduct': 'quality_of_life',
			'vandalism': 'property',
			'fraud': 'property',
			'dui': 'traffic',
			'prostitution': 'quality_of_life',
			'liquor laws': 'drugs',
			'forgery': 'property',
			'embezzlement': 'property',
			
			# Default for uncategorized
			'other': 'other'
		}
    
    def init_cache_db(self):
        """Initialize SQLite cache database"""
        self.cache_db = os.path.join(self.cache_dir, "crime_cache.db")
        
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS crime_data (
            jurisdiction TEXT,
            year INTEGER,
            month INTEGER,
            data BLOB,
            timestamp INTEGER,
            PRIMARY KEY (jurisdiction, year, month)
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS zipcode_crime_stats (
            zip TEXT,
            year INTEGER,
            month INTEGER,
            violent_count INTEGER,
            property_count INTEGER,
            other_count INTEGER,
            total_count INTEGER,
            timestamp INTEGER,
            PRIMARY KEY (zip, year, month)
        )
        """)
        
        conn.commit()
        conn.close()
    
    # In crime-data-collector.py, add this method to the CrimeDataCollector class
	def download_jurisdiction_data(self):
		"""Download jurisdiction data with zip codes"""
		logger.info("Downloading jurisdiction to zip code mapping data")
		
		try:
			# Use Census Bureau's ZCTA to Place Relationship File (free)
			url = "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_place_rel_10.txt"
			data_file = os.path.join(self.cache_dir, "zip_jurisdiction.csv")
			
			logger.info(f"Downloading jurisdiction mapping from: {url}")
			response = requests.get(url)
			
			with open(data_file, 'wb') as f:
				f.write(response.content)
			
			# Filter to just California places (state FIPS code 06)
			df = pd.read_csv(data_file, sep=",")
			ca_df = df[df['STATE'] == 6]
			ca_df.to_csv(os.path.join(self.cache_dir, "ca_zip_jurisdiction.csv"), index=False)
			
			logger.info(f"Downloaded and filtered jurisdiction mapping")
			return os.path.join(self.cache_dir, "ca_zip_jurisdiction.csv")
				
		except Exception as e:
			logger.error(f"Error downloading jurisdiction data: {e}")
			return None
    
    def get_cached_crime_data(self, jurisdiction, year, month):
        """Get crime data from cache if available and not expired"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Cache expires after 7 days (604800 seconds)
        cursor.execute(
            "SELECT data FROM crime_data WHERE jurisdiction = ? AND year = ? AND month = ? AND timestamp > ?", 
            (jurisdiction, year, month, int(time.time()) - 604800)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            logger.info(f"Retrieved {jurisdiction} {year}-{month} crime data from cache")
            return pd.read_json(result[0])
        
        return None
    
    def save_crime_data_to_cache(self, jurisdiction, year, month, df):
        """Save crime data to cache"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO crime_data (jurisdiction, year, month, data, timestamp) VALUES (?, ?, ?, ?, ?)",
            (jurisdiction, year, month, df.to_json(), int(time.time()))
        )
        
        conn.commit()
        conn.close()
    
    def get_cached_zipcode_stats(self, zip_code, year, month):
        """Get zipcode crime stats from cache if available and not expired"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Cache expires after 30 days (2592000 seconds)
        cursor.execute(
            "SELECT violent_count, property_count, other_count, total_count FROM zipcode_crime_stats WHERE zip = ? AND year = ? AND month = ? AND timestamp > ?", 
            (zip_code, year, month, int(time.time()) - 2592000)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            logger.info(f"Retrieved {zip_code} {year}-{month} crime stats from cache")
            return {
                'violent_count': result[0],
                'property_count': result[1],
                'other_count': result[2],
                'total_count': result[3]
            }
        
        return None
    
    def save_zipcode_stats_to_cache(self, zip_code, year, month, stats):
        """Save zipcode crime stats to cache"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO zipcode_crime_stats (zip, year, month, violent_count, property_count, other_count, total_count, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (zip_code, year, month, stats['violent_count'], stats['property_count'], stats['other_count'], stats['total_count'], int(time.time()))
        )
        
        conn.commit()
        conn.close()
    
    def get_crime_category(self, crime_type):
        """Map crime type to standard category: violent, property, or other"""
        if not crime_type:
            return 'other'
        
        crime_type_lower = crime_type.lower()
        
        for key, category in self.crime_category_map.items():
            if key in crime_type_lower:
                return category
        
        return 'other'
    
    def fetch_crime_data(self, jurisdiction, start_date, end_date=None):
        """
        Fetch crime data for a jurisdiction within a date range
        
        Parameters:
        - jurisdiction: Key for the jurisdiction in self.data_sources
        - start_date: Start date in 'YYYY-MM-DD' format
        - end_date: End date in 'YYYY-MM-DD' format (defaults to current date)
        
        Returns DataFrame with standardized crime data
        """
        if jurisdiction not in self.data_sources:
            logger.error(f"Unknown jurisdiction: {jurisdiction}")
            return None
        
        source = self.data_sources[jurisdiction]
        
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Check cache first (by month)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # If requesting multiple months, handle each month separately
        if start_dt.year != end_dt.year or start_dt.month != end_dt.month:
            all_data = []
            current_dt = start_dt
            
            while current_dt <= end_dt:
                year = current_dt.year
                month = current_dt.month
                
                # Get data for this month
                month_data = self.fetch_crime_data_by_month(jurisdiction, year, month)
                
                if month_data is not None:
                    all_data.append(month_data)
                
                # Move to next month
                if month == 12:
                    next_dt = datetime(year + 1, 1, 1)
                else:
                    next_dt = datetime(year, month + 1, 1)
                
                current_dt = next_dt
            
            if all_data:
                return pd.concat(all_data, ignore_index=True)
            else:
                return None
        
        # If requesting a single month, delegate to the by-month method
        return self.fetch_crime_data_by_month(jurisdiction, start_dt.year, start_dt.month)
    
    def fetch_crime_data_by_month(self, jurisdiction, year, month):
        """
        Fetch crime data for a specific month
        
        Parameters:
        - jurisdiction: Key for the jurisdiction in self.data_sources
        - year: Year (integer)
        - month: Month (integer)
        
        Returns DataFrame with standardized crime data
        """
        # Check cache first
        cached_data = self.get_cached_crime_data(jurisdiction, year, month)
        if cached_data is not None:
            return cached_data
        
        source = self.data_sources[jurisdiction]
        
        # Calculate date range for the month
        start_date = f"{year}-{month:02d}-01"
        
        # Last day of month
        if month == 12:
            end_date = f"{year}-{month:02d}-31"
        else:
            next_month = datetime(year, month, 1) + timedelta(days=32)
            next_month = next_month.replace(day=1) - timedelta(days=1)
            end_date = next_month.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching {source['name']} crime data for {year}-{month:02d}")
        
        try:
            # Initialize Socrata client
            client = Socrata(source['domain'], None)
            
            # Find the date field for this dataset
            date_field = self.get_date_field(client, source['dataset_id'])
            
            if not date_field:
                logger.error(f"Could not determine date field for {source['name']}")
                return None
            
            # Query Socrata API
            query = f"{date_field} >= '{start_date}' AND {date_field} <= '{end_date}'"
            results = client.get(source['dataset_id'], where=query, limit=source['limit'])
            
            if not results:
                logger.warning(f"No data found for {source['name']} in {year}-{month:02d}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame.from_records(results)
            
            # Standardize columns
            standardized_df = self.standardize_crime_data(df, jurisdiction)
            
            # Save to cache
            self.save_crime_data_to_cache(jurisdiction, year, month, standardized_df)
            
            return standardized_df
        
        except Exception as e:
            logger.error(f"Error fetching {source['name']} crime data: {e}")
            return None
    
    def get_date_field(self, client, dataset_id):
        """Determine the date field for a dataset by querying its metadata"""
        try:
            metadata = client.get_metadata(dataset_id)
            columns = metadata.get('columns', [])
            
            # Look for date fields
            date_fields = []
            for column in columns:
                if column.get('dataTypeName') in ['calendar_date', 'date']:
                    field_name = column.get('fieldName')
                    column_name = column.get('name', '').lower()
                    
                    # Prioritize fields with these terms in their name
                    priority_terms = ['date', 'incident', 'occurred', 'report']
                    
                    priority = 0
                    for term in priority_terms:
                        if term in column_name:
                            priority += 1
                    
                    date_fields.append((field_name, priority))
            
            # Sort by priority (highest first) and take the first one
            date_fields.sort(key=lambda x: x[1], reverse=True)
            
            if date_fields:
                return date_fields[0][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting date field for dataset {dataset_id}: {e}")
            return None
    
    def standardize_crime_data(self, df, jurisdiction):
        """
        Standardize crime data DataFrame
        
        Maps source-specific fields to standard fields and adds location data
        """
        source = self.data_sources[jurisdiction]
        standardized = pd.DataFrame()
        
        # Determine column mappings based on the jurisdiction
        if jurisdiction == 'sf':
            # San Francisco-specific mappings
            date_col = next((col for col in df.columns if 'date' in col.lower()), None)
            category_col = next((col for col in df.columns if 'category' in col.lower() or 'incident' in col.lower()), None)
            desc_col = next((col for col in df.columns if 'descript' in col.lower()), None)
            
            standardized['date'] = pd.to_datetime(df[date_col]) if date_col and date_col in df.columns else None
            standardized['category'] = df[category_col] if category_col and category_col in df.columns else None
            standardized['description'] = df[desc_col] if desc_col and desc_col in df.columns else None
            
            # Extract location
            if 'latitude' in df.columns and 'longitude' in df.columns:
                standardized['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                standardized['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            elif 'point' in df.columns:  # Handle GeoJSON point
                try:
                    standardized['latitude'] = df['point'].apply(lambda x: x.get('coordinates', [0, 0])[1] if x else None)
                    standardized['longitude'] = df['point'].apply(lambda x: x.get('coordinates', [0, 0])[0] if x else None)
                except:
                    standardized['latitude'] = None
                    standardized['longitude'] = None
            else:
                standardized['latitude'] = None
                standardized['longitude'] = None
            
        elif jurisdiction == 'oakland':
            # Oakland-specific mappings
            date_col = next((col for col in df.columns if 'date' in col.lower()), None)
            category_col = next((col for col in df.columns if 'crimetype' in col.lower() or 'category' in col.lower()), None)
            desc_col = next((col for col in df.columns if 'desc' in col.lower()), None)
            
            standardized['date'] = pd.to_datetime(df[date_col]) if date_col and date_col in df.columns else None
            standardized['category'] = df[category_col] if category_col and category_col in df.columns else None
            standardized['description'] = df[desc_col] if desc_col and desc_col in df.columns else None
            
            # Extract location
            if 'latitude' in df.columns and 'longitude' in df.columns:
                standardized['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
                standardized['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
            elif 'location_1' in df.columns:  # Handle location object
                try:
                    standardized['latitude'] = df['location_1'].apply(lambda x: x.get('latitude') if x else None)
                    standardized['longitude'] = df['location_1'].apply(lambda x: x.get('longitude') if x else None)
                except:
                    standardized['latitude'] = None
                    standardized['longitude'] = None
            else:
                standardized['latitude'] = None
                standardized['longitude'] = None
            
        else:
            # Generic mappings for other jurisdictions
            date_col = next((col for col in df.columns if 'date' in col.lower()), None)
            category_col = next((col for col in df.columns if 'category' in col.lower() or 'type' in col.lower() or 'crime' in col.lower()), None)
            desc_col = next((col for col in df.columns if 'desc' in col.lower() or 'narrative' in col.lower()), None)
            
            standardized['date'] = pd.to_datetime(df[date_col]) if date_col and date_col in df.columns else None
            standardized['category'] = df[category_col] if category_col and category_col in df.columns else None
            standardized['description'] = df[desc_col] if desc_col and desc_col in df.columns else None
            
            # Extract location
            lat_col = next((col for col in df.columns if 'lat' in col.lower()), None)
            lon_col = next((col for col in df.columns if 'lon' in col.lower() or 'lng' in col.lower()), None)
            
            if lat_col and lon_col and lat_col in df.columns and lon_col in df.columns:
                standardized['latitude'] = pd.to_numeric(df[lat_col], errors='coerce')
                standardized['longitude'] = pd.to_numeric(df[lon_col], errors='coerce')
            else:
                standardized['latitude'] = None
                standardized['longitude'] = None
        
        # Add jurisdiction field
        standardized['jurisdiction'] = source['name']
        
        # Add crime type category
        standardized['crime_type'] = standardized['category'].apply(self.get_crime_category)
        
        # Add year and month
        if 'date' in standardized.columns and standardized['date'].notna().any():
            standardized['year'] = standardized['date'].dt.year
            standardized['month'] = standardized['date'].dt.month
        
        return standardized
    
    def assign_crimes_to_zipcodes(self, crime_df, zip_gdf):
        """
        Assign crimes to ZIP codes based on their coordinates
        
        Parameters:
        - crime_df: DataFrame with standardized crime data
        - zip_gdf: GeoDataFrame with ZIP code boundaries
        
        Returns DataFrame with added ZIP code column
        """
        # Filter to crimes with valid coordinates
        valid_coords = crime_df.dropna(subset=['latitude', 'longitude'])
        
        if len(valid_coords) == 0:
            logger.warning("No valid coordinates in crime data")
            return crime_df
        
        # Create points from coordinates
        geometry = [Point(lon, lat) for lon, lat in zip(valid_coords['longitude'], valid_coords['latitude'])]
        crime_gdf = gpd.GeoDataFrame(valid_coords, geometry=geometry, crs="EPSG:4326")
        
        # Spatial join with ZIP codes
        joined = gpd.sjoin(crime_gdf, zip_gdf, how='left', predicate='within')
        
        # Add zipcode to original DataFrame
        crime_df = crime_df.copy()
        crime_df['zipcode'] = None
        
        # Update with joined zipcodes
        crime_df.loc[valid_coords.index, 'zipcode'] = joined['zip']
        
        return crime_df
    
    def calculate_crime_stats_by_zipcode(self, crime_df, year, month):
        """
        Calculate crime statistics by ZIP code for a specific month
        
        Parameters:
        - crime_df: DataFrame with crime data and ZIP codes
        - year: Year (integer)
        - month: Month (integer)
        
        Returns DataFrame with crime counts by ZIP code
        """
        # Filter to the specified month
        month_data = crime_df[(crime_df['year'] == year) & (crime_df['month'] == month)]
        
        if len(month_data) == 0:
            logger.warning(f"No crime data for {year}-{month:02d}")
            return None
        
        # Group by zipcode and crime type
        grouped = month_data.groupby(['zipcode', 'crime_type']).size().unstack(fill_value=0)
        
        # Ensure all crime type columns exist
        for crime_type in ['violent', 'property', 'other']:
            if crime_type not in grouped.columns:
                grouped[crime_type] = 0
        
        # Calculate total
        grouped['total'] = grouped.sum(axis=1)
        
        # Reset index to get zipcode as a column
        grouped = grouped.reset_index()
        
        # Rename columns
        grouped = grouped.rename(columns={
            'violent': 'violent_count',
            'property': 'property_count',
            'other': 'other_count',
            'total': 'total_count'
        })
        
        # Add year and month columns
        grouped['year'] = year
        grouped['month'] = month
        
        return grouped
    
    def calculate_crime_rates(self, crime_stats_df, population_df):
        """
        Calculate crime rates per 1000 population
        
        Parameters:
        - crime_stats_df: DataFrame with crime counts by ZIP code
        - population_df: DataFrame with population by ZIP code
        
        Returns DataFrame with added crime rate columns
        """
        # Merge crime stats with population data
        merged = pd.merge(crime_stats_df, population_df, on='zipcode', how='left')
        
        # Calculate rates per 1000 population
        merged['violent_rate'] = merged['violent_count'] / merged['population'] * 1000
        merged['property_rate'] = merged['property_count'] / merged['population'] * 1000
        merged['other_rate'] = merged['other_count'] / merged['population'] * 1000
        merged['total_rate'] = merged['total_count'] / merged['population'] * 1000
        
        return merged
    
    def collect_crime_data(self, zip_gdf, start_date, end_date=None):
        """
        Collect crime data for all jurisdictions and calculate stats by ZIP code
        
        Parameters:
        - zip_gdf: GeoDataFrame with ZIP code boundaries
        - start_date: Start date in 'YYYY-MM-DD' format
        - end_date: End date in 'YYYY-MM-DD' format (defaults to current date)
        
        Returns DataFrame with crime stats by ZIP code
        """
        all_crime_data = []
        
        # Fetch crime data for each jurisdiction
        for jurisdiction in self.data_sources.keys():
            crime_df = self.fetch_crime_data(jurisdiction, start_date, end_date)
            
            if crime_df is not None:
                # Assign crimes to ZIP codes
                crime_df = self.assign_crimes_to_zipcodes(crime_df, zip_gdf)
                all_crime_data.append(crime_df)
        
        if not all_crime_data:
            logger.warning("No crime data collected")
            return None
        
        # Combine all crime data
        combined_df = pd.concat(all_crime_data, ignore_index=True)
        
        # Parse date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        
        # Calculate stats for each month in the range
        all_stats = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            year = current_dt.year
            month = current_dt.month
            
            # Calculate stats for this month
            month_stats = self.calculate_crime_stats_by_zipcode(combined_df, year, month)
            
            if month_stats is not None:
                all_stats.append(month_stats)
            
            # Move to next month
            if month == 12:
                next_dt = datetime(year + 1, 1, 1)
            else:
                next_dt = datetime(year, month + 1, 1)
            
            current_dt = next_dt
        
        if not all_stats:
            logger.warning("No crime stats calculated")
            return None
        
        # Combine all stats
        return pd.concat(all_stats, ignore_index=True)
    
    def save_to_database(self, crime_stats_df, db_connection=None):
        """
        Save crime stats to database
        
        Parameters:
        - crime_stats_df: DataFrame with crime stats by ZIP code
        - db_connection: Database connection string or connection object
        """
        if not db_connection and not db_url:
            logger.error("No database connection provided")
            return False
        
        try:
            # Connect to database
            if isinstance(db_connection, str):
                conn = psycopg2.connect(db_connection)
            elif db_connection:
                conn = db_connection
            else:
                conn = psycopg2.connect(db_url)
            
            cursor = conn.cursor()
            
            # Insert crime stats
            for _, row in crime_stats_df.iterrows():
                cursor.execute("""
                INSERT INTO crime_stats (
                    zip, year, violent_crime_count, property_crime_count,
                    violent_crime_rate, property_crime_rate, overall_crime_rate,
                    source, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (zip, year) DO UPDATE SET
                    violent_crime_count = EXCLUDED.violent_crime_count,
                    property_crime_count = EXCLUDED.property_crime_count,
                    violent_crime_rate = EXCLUDED.violent_crime_rate,
                    property_crime_rate = EXCLUDED.property_crime_rate,
                    overall_crime_rate = EXCLUDED.overall_crime_rate,
                    source = EXCLUDED.source,
                    last_updated = EXCLUDED.last_updated
                """, (
                    row['zipcode'],
                    row['year'],
                    row['violent_count'],
                    row['property_count'],
                    row.get('violent_rate', 0),
                    row.get('property_rate', 0),
                    row.get('total_rate', 0),
                    'Open Data Portals (SF, Oakland, Berkeley, San Jose)',
                    datetime.now()
                ))
                
                # Also update zipcode_ratings table for map visualization
                # Calculate crime rating on a 10-point scale
                # 0 crime rate = 10, 50+ crimes per 1000 people = 1
                if 'total_rate' in row:
                    crime_rate = row['total_rate']
                    crime_rating = max(1, min(10, 10 - (crime_rate / 5)))
                    
                    cursor.execute("""
                    INSERT INTO zipcode_ratings (
                        zip, rating_type, rating_value, confidence,
                        source, source_url, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zip, rating_type) DO UPDATE SET
                        rating_value = EXCLUDED.rating_value,
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source,
                        source_url = EXCLUDED.source_url,
                        last_updated = EXCLUDED.last_updated
                    """, (
                        row['zipcode'],
                        'crimeRate',
                        crime_rating,
                        0.8,  # confidence score
                        'Open Data Portals',
                        'https://data.sfgov.org/',
                        datetime.now()
                    ))
            
            # Update data source record
            cursor.execute("""
            INSERT INTO data_sources
            (source_name, last_updated, next_update, update_frequency, url, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_name) DO UPDATE SET
                last_updated = EXCLUDED.last_updated,
                next_update = EXCLUDED.next_update,
                update_frequency = EXCLUDED.update_frequency,
                url = EXCLUDED.url,
                notes = EXCLUDED.notes
            """, (
                "crime_data",
                datetime.now(),
                datetime.now() + timedelta(days=30),
                "30 days",
                "https://data.sfgov.org/",
                "Crime data from Bay Area open data portals"
            ))
            
            conn.commit()
            logger.info("Successfully saved crime stats to database")
            return True
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    
    def save_to_csv(self, crime_stats_df, output_file="crime_stats.csv"):
        """
        Save crime stats to CSV
        
        Parameters:
        - crime_stats_df: DataFrame with crime stats by ZIP code
        - output_file: Output CSV filename
        """
        crime_stats_df.to_csv(output_file, index=False)
        logger.info(f"Saved crime stats to {output_file}")
        return output_file

def load_zipcode_boundaries(zipcode_file=None, db_connection=None):
    """
    Load ZIP code boundaries from file or database
    
    Parameters:
    - zipcode_file: GeoJSON file with ZIP code boundaries (optional)
    - db_connection: Database connection string or connection object (optional)
    
    Returns GeoDataFrame with ZIP code boundaries
    """
    # If file provided, load from file
    if zipcode_file and os.path.exists(zipcode_file):
        try:
            gdf = gpd.read_file(zipcode_file)
            return gdf
        except Exception as e:
            logger.error(f"Error loading ZIP code boundaries from file: {e}")
    
    # If database connection provided, load from database
    if db_connection or db_url:
        try:
            # Connect to database
            if isinstance(db_connection, str):
                conn = psycopg2.connect(db_connection)
            elif db_connection:
                conn = db_connection
            else:
                conn = psycopg2.connect(db_url)
            
            # Query ZIP code boundaries
            query = "SELECT zip, name, county, state, ST_AsGeoJSON(geometry) as geom FROM zipcodes"
            gdf = gpd.read_postgis(query, conn, geom_col='geom')
            
            conn.close()
            return gdf
            
        except Exception as e:
            logger.error(f"Error loading ZIP code boundaries from database: {e}")
    
    # If no boundaries could be loaded, download from Census
    logger.info("Downloading ZIP code boundaries from Census")
    try:
        # Download CA ZIP Code Tabulation Areas
        url = "https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_06_zcta520.zip"
        gdf = gpd.read_file(url)
        
        # Filter to Bay Area counties
        bay_area_zips = get_bay_area_zipcodes()
        gdf = gdf[gdf['ZCTA5CE20'].isin(bay_area_zips)]
        
        # Rename columns
        gdf = gdf.rename(columns={'ZCTA5CE20': 'zip'})
        
        return gdf
        
    except Exception as e:
        logger.error(f"Error downloading ZIP code boundaries: {e}")
        return None

def get_bay_area_zipcodes():
    """Return a list of Bay Area ZIP codes"""
    bay_area_zips = []
    
    # San Francisco
    bay_area_zips.extend(['94102', '94103', '94104', '94105', '94107', '94108', '94109', '94110', 
                          '94111', '94112', '94114', '94115', '94116', '94117', '94118', '94121', 
                          '94122', '94123', '94124', '94127', '94129', '94130', '94131', '94132', 
                          '94133', '94134', '94158'])
    
    # East Bay (Oakland, Berkeley, etc.)
    bay_area_zips.extend(['94501', '94502', '94536', '94537', '94538', '94539', '94540', '94541', 
                          '94542', '94543', '94544', '94545', '94546', '94550', '94551', '94552', 
                          '94555', '94560', '94566', '94568', '94577', '94578', '94579', '94580', 
                          '94586', '94587', '94588', '94601', '94602', '94603', '94605', '94606', 
                          '94607', '94608', '94609', '94610', '94611', '94612', '94618', '94619', 
                          '94621', '94705', '94706', '94707', '94708', '94709', '94710'])
    
    # Add more Bay Area ZIP codes as needed
    
    return bay_area_zips

def main():
    parser = argparse.ArgumentParser(description='Crime Data Collector for Bay Area')
    parser.add_argument('--start-date', default=(datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default=datetime.now().strftime('%Y-%m-%d'),
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', default='crime_stats.csv', help='Output CSV filename')
    parser.add_argument('--zipcode-file', help='GeoJSON file with ZIP code boundaries')
    parser.add_argument('--database', action='store_true', help='Save to database')
    parser.add_argument('--db-url', help='Database connection URL')
    
    args = parser.parse_args()
    
    # If database URL provided, use it instead of environment variable
    global db_url
    if args.db_url:
        db_url = args.db_url
    
    # Load ZIP code boundaries
    zip_gdf = load_zipcode_boundaries(args.zipcode_file, args.db_url)
    
    if zip_gdf is None:
        logger.error("Could not load ZIP code boundaries")
        return 1
    
    # Initialize collector
    collector = CrimeDataCollector()
    
    # Collect crime data
    crime_stats = collector.collect_crime_data(zip_gdf, args.start_date, args.end_date)
    
    if crime_stats is None:
        logger.error("No crime stats collected")
        return 1
    
    # Save to CSV
    collector.save_to_csv(crime_stats, args.output)
    
    # Save to database if requested
    if args.database:
        collector.save_to_database(crime_stats)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
