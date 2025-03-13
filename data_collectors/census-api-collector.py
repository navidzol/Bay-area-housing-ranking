#!/usr/bin/env python3
"""
Census Bureau API Data Collector
Extracts demographic, income, housing, and commute data for Bay Area zip codes
"""

import os
import sys
import time
import logging
import json
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("census_collector.log")
    ]
)
logger = logging.getLogger('census_collector')

# Optional database connection parameters
db_url = os.environ.get('DATABASE_URL', '')

class CensusDataCollector:
    """Collects data from Census Bureau APIs"""
    
    def __init__(self, api_key=None, cache_dir="census_cache"):
        """
        Initialize the collector
        
        Parameters:
        - api_key: Census API key (recommended but not required for small requests)
        - cache_dir: Directory to store cached API responses
        """
        self.api_key = api_key or os.environ.get('CENSUS_API_KEY', '')
        self.cache_dir = cache_dir
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Base URLs for different Census APIs
        self.acs_url = "https://api.census.gov/data"
    
    def get_acs_data(self, year, dataset, variables, geo_level, geo_ids=None, state="06"):
        """
        Fetch data from American Community Survey (ACS)
        
        Parameters:
        - year: Survey year (e.g., "2022")
        - dataset: Dataset name (e.g., "acs/acs5" for 5-year estimates)
        - variables: List of Census variable codes
        - geo_level: Geography level (e.g., "zip code tabulation area")
        - geo_ids: Optional list of geographic IDs to filter
        - state: State FIPS code (default: "06" for California)
        """
        # Check cache first
        cache_key = f"{year}_{dataset}_{geo_level}_{state}_{'-'.join(variables)}"
        if geo_ids:
            cache_key += f"_{'-'.join(geo_ids)}"
        
        cache_file = os.path.join(self.cache_dir, f"{cache_key.replace('/', '_')}.json")
        
        if os.path.exists(cache_file):
            with open(cache_file, 'r') as f:
                logger.info(f"Loading from cache: {cache_file}")
                return pd.DataFrame(json.load(f))
        
        # Construct URL
        url = f"{self.acs_url}/{year}/{dataset}"
        
        # Prepare query parameters
        params = {
            'get': ','.join(['NAME'] + variables),
            'for': f"{geo_level}:{'*' if not geo_ids else ','.join(geo_ids)}",
        }
        
        # Add state filter for ZCTA queries
        if geo_level == 'zip code tabulation area':
            params['in'] = f'state:{state}'
        
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
                
                # Save to cache
                with open(cache_file, 'w') as f:
                    json.dump(data, f)
                
                return df
            else:
                logger.error(f"Census API error: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            logger.error(f"Error fetching Census data: {e}")
            return None

    def collect_demographics(self, year="2022"):
        """
        Collect demographic data from ACS 5-year estimates
        
        Returns DataFrame with demographic data by ZIP code
        """
        # Define demographic variables
        variables = [
            'B01003_001E',  # Total population
            'B01002_001E',  # Median age
            'B02001_002E',  # White population
            'B02001_003E',  # Black/African American population
            'B02001_004E',  # American Indian population
            'B02001_005E',  # Asian population
            'B03003_003E',  # Hispanic or Latino population
            'B08301_001E',  # Total commuters
            'B08301_003E',  # Car, truck, van - drove alone
            'B08301_004E',  # Car, truck, van - carpooled
            'B08301_010E',  # Public transportation
            'B08301_019E',  # Walked
            'B08301_018E',  # Bicycle
            'B08301_021E',  # Worked from home
            'B19013_001E',  # Median household income
            'B25064_001E',  # Median gross rent
            'B25077_001E',  # Median value of owner-occupied housing units
            'B25003_001E',  # Total occupied housing units
            'B25003_002E',  # Owner-occupied housing units
            'B25003_003E',  # Renter-occupied housing units
            'B08303_001E',  # Total commute time
            'B15003_001E',  # Total education population 25 years and over
            'B15003_017E',  # High school graduate
            'B15003_022E',  # Bachelor's degree
            'B15003_023E',  # Master's degree
            'B15003_024E',  # Professional school degree
            'B15003_025E'   # Doctorate degree
        ]
        
        # Fetch data
        df = self.get_acs_data(
            year=year,
            dataset="acs/acs5",
            variables=variables,
            geo_level="zip code tabulation area"
        )
        
        if df is None:
            return None
        
        # Convert data to numeric
        for col in variables:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate additional fields
        results = []
        
        for _, row in df.iterrows():
            zipcode = row['zip code tabulation area']
            name = row['NAME'].replace('ZCTA5 ', '')
            
            # Calculate percentages
            total_pop = row['B01003_001E']
            total_occupied = row['B25003_001E'] if row['B25003_001E'] > 0 else 1  # Avoid div by zero
            total_commuters = row['B08301_001E'] if row['B08301_001E'] > 0 else 1  # Avoid div by zero
            total_education = row['B15003_001E'] if row['B15003_001E'] > 0 else 1  # Avoid div by zero
            
            # Race/ethnicity percentages
            white_pct = row['B02001_002E'] / total_pop * 100 if total_pop > 0 else 0
            black_pct = row['B02001_003E'] / total_pop * 100 if total_pop > 0 else 0
            asian_pct = row['B02001_005E'] / total_pop * 100 if total_pop > 0 else 0
            hispanic_pct = row['B03003_003E'] / total_pop * 100 if total_pop > 0 else 0
            other_pct = 100 - (white_pct + black_pct + asian_pct)
            
            # Housing percentages
            ownership_pct = row['B25003_002E'] / total_occupied * 100
            renter_pct = row['B25003_003E'] / total_occupied * 100
            
            # Commute percentages
            drove_alone_pct = row['B08301_003E'] / total_commuters * 100
            carpool_pct = row['B08301_004E'] / total_commuters * 100
            transit_pct = row['B08301_010E'] / total_commuters * 100
            walk_pct = row['B08301_019E'] / total_commuters * 100
            bike_pct = row['B08301_018E'] / total_commuters * 100
            wfh_pct = row['B08301_021E'] / total_commuters * 100
            
            # Education percentages
            high_school_pct = row['B15003_017E'] / total_education * 100
            bachelors_pct = row['B15003_022E'] / total_education * 100
            masters_pct = row['B15003_023E'] / total_education * 100
            professional_pct = row['B15003_024E'] / total_education * 100
            doctorate_pct = row['B15003_025E'] / total_education * 100
            
            # Combined education percentages
            college_pct = bachelors_pct + masters_pct + professional_pct + doctorate_pct
            
            results.append({
                'zip': zipcode,
                'name': name,
                'population': total_pop,
                'median_age': row['B01002_001E'],
                'median_income': row['B19013_001E'],
                'median_rent': row['B25064_001E'],
                'median_home_value': row['B25077_001E'],
                'ownership_percent': ownership_pct,
                'renter_percent': renter_pct,
                'race_white': white_pct,
                'race_black': black_pct,
                'race_asian': asian_pct,
                'race_hispanic': hispanic_pct,
                'race_other': other_pct,
                'commute_drove_alone': drove_alone_pct,
                'commute_carpool': carpool_pct,
                'commute_transit': transit_pct,
                'commute_walk': walk_pct,
                'commute_bike': bike_pct,
                'commute_wfh': wfh_pct,
                'education_highschool': high_school_pct,
                'education_bachelors': bachelors_pct,
                'education_graduate': college_pct - bachelors_pct
            })
        
        return pd.DataFrame(results)
    
    def collect_commute_times(self, year="2022"):
        """
        Collect commute time data from ACS 5-year estimates
        
        Returns DataFrame with commute times by ZIP code
        """
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
        
        df = self.get_acs_data(
            year=year,
            dataset="acs/acs5",
            variables=commute_vars,
            geo_level="zip code tabulation area"
        )
        
        if df is None:
            return None
        
        # Convert to numeric
        for col in commute_vars:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Calculate weighted average commute time
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
        
        results = []
        
        for _, row in df.iterrows():
            zipcode = row['zip code tabulation area']
            total_workers = row['B08303_001E']
            
            # Calculate weighted average commute time
            total_minutes = 0
            for midpoint, col in time_ranges:
                total_minutes += row[col] * midpoint
            
            # Calculate average and percentages
            avg_commute_minutes = total_minutes / total_workers if total_workers > 0 else 0
            
            # Calculate percentages in each commute time range
            pct_lt_10min = (row['B08303_002E'] + row['B08303_003E']) / total_workers * 100 if total_workers > 0 else 0
            pct_10to20min = (row['B08303_004E'] + row['B08303_005E']) / total_workers * 100 if total_workers > 0 else 0
            pct_20to30min = (row['B08303_006E'] + row['B08303_007E']) / total_workers * 100 if total_workers > 0 else 0
            pct_30to45min = (row['B08303_008E'] + row['B08303_009E'] + row['B08303_010E']) / total_workers * 100 if total_workers > 0 else 0
            pct_45to60min = row['B08303_011E'] / total_workers * 100 if total_workers > 0 else 0
            pct_gt60min = (row['B08303_012E'] + row['B08303_013E']) / total_workers * 100 if total_workers > 0 else 0
            
            # Normalize to 0-10 scale (where 10 is best/shortest commute)
            # Assume 60+ min is worst (0), and 10 min is best (10)
            commute_score = max(0, min(10, 10 - ((avg_commute_minutes - 10) / 5))) if avg_commute_minutes > 0 else 5
            
            results.append({
                'zip': zipcode,
                'avg_commute_minutes': round(avg_commute_minutes, 1),
                'commute_score': round(commute_score, 1),
                'commute_lt_10min_pct': round(pct_lt_10min, 1),
                'commute_10to20min_pct': round(pct_10to20min, 1),
                'commute_20to30min_pct': round(pct_20to30min, 1),
                'commute_30to45min_pct': round(pct_30to45min, 1),
                'commute_45to60min_pct': round(pct_45to60min, 1),
                'commute_gt60min_pct': round(pct_gt60min, 1)
            })
        
        return pd.DataFrame(results)
    
    def collect_housing_market(self, year="2022"):
        """
        Collect housing market data from ACS 5-year estimates
        
        Returns DataFrame with housing market data by ZIP code
        """
        # Define housing variables
        variables = [
            'B25077_001E',  # Median value of owner-occupied housing units
            'B25064_001E',  # Median gross rent
            'B25003_001E',  # Total occupied housing units
            'B25003_002E',  # Owner-occupied housing units
            'B25003_003E',  # Renter-occupied housing units
            'B25004_001E',  # Total vacant housing units
            'B25004_002E',  # For rent
            'B25004_004E',  # For sale only
            'B25024_001E',  # Total housing units
            'B25024_002E',  # 1-unit, detached
            'B25024_003E',  # 1-unit, attached
            'B25024_004E',  # 2 units
            'B25024_005E',  # 3 or 4 units
            'B25024_006E',  # 5 to 9 units
            'B25024_007E',  # 10 to 19 units
            'B25024_008E',  # 20 to 49 units
            'B25024_009E',  # 50 or more units
            'B25024_010E'   # Mobile home, boat, RV, van, etc.
        ]
        
        # Fetch data
        df = self.get_acs_data(
            year=year,
            dataset="acs/acs5",
            variables=variables,
            geo_level="zip code tabulation area"
        )
        
        if df is None:
            return None
        
        # Convert data to numeric
        for col in variables:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        results = []
        
        for _, row in df.iterrows():
            zipcode = row['zip code tabulation area']
            
            # Calculate housing metrics
            total_units = row['B25024_001E']
            total_occupied = row['B25003_001E']
            
            # Avoid division by zero
            if total_units == 0:
                total_units = 1
            if total_occupied == 0:
                total_occupied = 1
            
            # Calculate percentages
            pct_occupied = total_occupied / total_units * 100
            pct_vacant = (total_units - total_occupied) / total_units * 100
            
            # Housing type percentages
            pct_single_family = (row['B25024_002E'] + row['B25024_003E']) / total_units * 100
            pct_small_multi = (row['B25024_004E'] + row['B25024_005E']) / total_units * 100
            pct_medium_multi = (row['B25024_006E'] + row['B25024_007E']) / total_units * 100
            pct_large_multi = row['B25024_008E'] / total_units * 100
            pct_high_rise = row['B25024_009E'] / total_units * 100
            
            # Ownership percentages
            pct_owner_occupied = row['B25003_002E'] / total_occupied * 100
            pct_renter_occupied = row['B25003_003E'] / total_occupied * 100
            
            results.append({
                'zip': zipcode,
                'median_home_value': row['B25077_001E'],
                'median_rent': row['B25064_001E'],
                'total_housing_units': total_units,
                'pct_occupied': pct_occupied,
                'pct_vacant': pct_vacant,
                'pct_owner_occupied': pct_owner_occupied,
                'pct_renter_occupied': pct_renter_occupied,
                'pct_single_family': pct_single_family,
                'pct_small_multi': pct_small_multi,
                'pct_medium_multi': pct_medium_multi,
                'pct_large_multi': pct_large_multi,
                'pct_high_rise': pct_high_rise
            })
        
        return pd.DataFrame(results)
    
    def save_to_database(self, db_connection=None):
        """
        Save all collected data to the database
        
        Parameters:
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
            year = datetime.now().year
            
            # Collect and save demographic data
            logger.info("Collecting demographic data")
            demo_df = self.collect_demographics()
            
            if demo_df is not None:
                logger.info(f"Saving {len(demo_df)} demographic records")
                
                # Update zipcodes table with basic demographic info
                for _, row in demo_df.iterrows():
                    cursor.execute("""
                    UPDATE zipcodes SET
                        population = %s,
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
                
                # Insert into demographics table
                for _, row in demo_df.iterrows():
                    cursor.execute("""
                    INSERT INTO demographics (
                        zip, year, total_population, median_age,
                        race_white, race_black, race_asian, race_hispanic, race_other,
                        education_less_than_highschool, education_highschool, 
                        education_some_college, education_bachelors, education_graduate,
                        source, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zip, year) DO UPDATE SET
                        total_population = EXCLUDED.total_population,
                        median_age = EXCLUDED.median_age,
                        race_white = EXCLUDED.race_white,
                        race_black = EXCLUDED.race_black,
                        race_asian = EXCLUDED.race_asian,
                        race_hispanic = EXCLUDED.race_hispanic,
                        race_other = EXCLUDED.race_other,
                        education_less_than_highschool = EXCLUDED.education_less_than_highschool,
                        education_highschool = EXCLUDED.education_highschool,
                        education_some_college = EXCLUDED.education_some_college,
                        education_bachelors = EXCLUDED.education_bachelors,
                        education_graduate = EXCLUDED.education_graduate,
                        source = EXCLUDED.source,
                        last_updated = EXCLUDED.last_updated
                    """, (
                        row['zip'],
                        year,
                        row['population'],
                        row['median_age'],
                        row['race_white'],
                        row['race_black'],
                        row['race_asian'],
                        row['race_hispanic'],
                        row['race_other'],
                        0,  # No direct data for less than high school
                        row['education_highschool'],
                        0,  # No direct data for some college
                        row['education_bachelors'],
                        row['education_graduate'],
                        'US Census Bureau ACS',
                        datetime.now()
                    ))
            
            # Collect and save commute data
            logger.info("Collecting commute data")
            commute_df = self.collect_commute_times()
            
            if commute_df is not None:
                logger.info(f"Saving {len(commute_df)} commute records")
                
                # Update zipcode_ratings table with commute scores
                for _, row in commute_df.iterrows():
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
                        row['zip'],
                        'commuteTime',
                        row['commute_score'],
                        0.9,  # High confidence for Census data
                        'US Census Bureau ACS',
                        'https://www.census.gov/programs-surveys/acs',
                        datetime.now()
                    ))
                
                # Insert into commute_stats table
                for _, row in commute_df.iterrows():
                    # Get commute mode percentages from demographics
                    demo_row = demo_df[demo_df['zip'] == row['zip']].iloc[0] if len(demo_df[demo_df['zip'] == row['zip']]) > 0 else None
                    
                    cursor.execute("""
                    INSERT INTO commute_stats (
                        zip, year, avg_commute_minutes,
                        drive_alone_pct, carpool_pct, public_transit_pct,
                        walk_pct, bike_pct, work_from_home_pct,
                        commute_lt_10min_pct, commute_10to20min_pct, commute_20to30min_pct,
                        commute_30to45min_pct, commute_45to60min_pct, commute_gt60min_pct,
                        source, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zip, year) DO UPDATE SET
                        avg_commute_minutes = EXCLUDED.avg_commute_minutes,
                        drive_alone_pct = EXCLUDED.drive_alone_pct,
                        carpool_pct = EXCLUDED.carpool_pct,
                        public_transit_pct = EXCLUDED.public_transit_pct,
                        walk_pct = EXCLUDED.walk_pct,
                        bike_pct = EXCLUDED.bike_pct,
                        work_from_home_pct = EXCLUDED.work_from_home_pct,
                        commute_lt_10min_pct = EXCLUDED.commute_lt_10min_pct,
                        commute_10to20min_pct = EXCLUDED.commute_10to20min_pct,
                        commute_20to30min_pct = EXCLUDED.commute_20to30min_pct,
                        commute_30to45min_pct = EXCLUDED.commute_30to45min_pct,
                        commute_45to60min_pct = EXCLUDED.commute_45to60min_pct,
                        commute_gt60min_pct = EXCLUDED.commute_gt60min_pct,
                        source = EXCLUDED.source,
                        last_updated = EXCLUDED.last_updated
                    """, (
                        row['zip'],
                        year,
                        row['avg_commute_minutes'],
                        demo_row['commute_drove_alone'] if demo_row is not None else 0,
                        demo_row['commute_carpool'] if demo_row is not None else 0,
                        demo_row['commute_transit'] if demo_row is not None else 0,
                        demo_row['commute_walk'] if demo_row is not None else 0,
                        demo_row['commute_bike'] if demo_row is not None else 0,
                        demo_row['commute_wfh'] if demo_row is not None else 0,
                        row['commute_lt_10min_pct'],
                        row['commute_10to20min_pct'],
                        row['commute_20to30min_pct'],
                        row['commute_30to45min_pct'],
                        row['commute_45to60min_pct'],
                        row['commute_gt60min_pct'],
                        'US Census Bureau ACS',
                        datetime.now()
                    ))
            
            # Collect and save housing market data
            logger.info("Collecting housing market data")
            housing_df = self.collect_housing_market()
            
            if housing_df is not None:
                logger.info(f"Saving {len(housing_df)} housing market records")
                
                # Insert into housing_market table
                for _, row in housing_df.iterrows():
                    cursor.execute("""
                    INSERT INTO housing_market (
                        zip, year_month, median_list_price, median_sold_price,
                        median_days_on_market, num_homes_sold, inventory_count,
                        price_per_sqft, year_over_year_change, source, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zip, year_month) DO UPDATE SET
                        median_list_price = EXCLUDED.median_list_price,
                        median_sold_price = EXCLUDED.median_sold_price,
                        median_days_on_market = EXCLUDED.median_days_on_market,
                        num_homes_sold = EXCLUDED.num_homes_sold,
                        inventory_count = EXCLUDED.inventory_count,
                        price_per_sqft = EXCLUDED.price_per_sqft,
                        year_over_year_change = EXCLUDED.year_over_year_change,
                        source = EXCLUDED.source,
                        last_updated = EXCLUDED.last_updated
                    """, (
                        row['zip'],
                        f"{year}-{datetime.now().month:02d}",
                        row['median_home_value'],
                        row['median_home_value'],  # No sold price in ACS
                        None,  # No days on market in ACS
                        None,  # No homes sold in ACS
                        None,  # No inventory in ACS
                        None,  # No price per sqft in ACS
                        None,  # No year-over-year in ACS
                        'US Census Bureau ACS',
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
                "census_data",
                datetime.now(),
                datetime.now().replace(year=datetime.now().year + 1),  # Next year
                "365 days",
                "https://www.census.gov/data/developers/data-sets.html",
                "US Census Bureau ACS 5-year estimates"
            ))
            
            conn.commit()
            logger.info("Successfully saved Census data to database")
            return True
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            if 'conn' in locals() and conn:
                conn.rollback()
            return False
        finally:
            if 'conn' in locals() and conn:
                conn.close()
    
    def save_to_csv(self, output_dir="census_output"):
        """
        Save all collected data to CSV files
        
        Parameters:
        - output_dir: Directory to save CSV files
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Collect and save demographic data
        demo_df = self.collect_demographics()
        if demo_df is not None:
            demo_df.to_csv(os.path.join(output_dir, "demographics.csv"), index=False)
            logger.info(f"Saved {len(demo_df)} demographic records to CSV")
        
        # Collect and save commute data
        commute_df = self.collect_commute_times()
        if commute_df is not None:
            commute_df.to_csv(os.path.join(output_dir, "commute_times.csv"), index=False)
            logger.info(f"Saved {len(commute_df)} commute records to CSV")
        
        # Collect and save housing market data
        housing_df = self.collect_housing_market()
        if housing_df is not None:
            housing_df.to_csv(os.path.join(output_dir, "housing_market.csv"), index=False)
            logger.info(f"Saved {len(housing_df)} housing market records to CSV")
        
        return output_dir

def get_bay_area_zipcodes():
    """Return a list of Bay Area ZIP codes"""
    bay_area_zips = []
    
    # San Francisco
    bay_area_zips.extend(['94102', '94103', '94104', '94105', '94107', '94108', '94109', '94110', 
                          '94111', '94112', '94114', '94115', '94116', '94117', '94118', '94121', 
                          '94122', '94123', '94124', '94127', '94129', '94130', '94131', '94132', 
                          '94133', '94134', '94158'])
    
    # Add more Bay Area ZIP codes as needed
    
    return bay_area_zips

def main():
    parser = argparse.ArgumentParser(description='Census Bureau Data Collector')
    parser.add_argument('--api-key', help='Census API key')
    parser.add_argument('--output-dir', default='census_output', help='Output directory for CSV files')
    parser.add_argument('--database', action='store_true', help='Save to database')
    parser.add_argument('--db-url', help='Database connection URL')
    
    args = parser.parse_args()
    
    # If database URL provided, use it instead of environment variable
    global db_url
    if args.db_url:
        db_url = args.db_url
    
    # Initialize collector
    collector = CensusDataCollector(api_key=args.api_key)
    
    # Save to CSV
    collector.save_to_csv(output_dir=args.output_dir)
    
    # Save to database if requested
    if args.database:
        collector.save_to_database()

if __name__ == "__main__":
    main()
