#!/usr/bin/env python3
"""
Data loader script for Bay Area Housing Criteria Map
This script loads zipcode boundaries and real commute data from Census
"""

import os
from dotenv import load_dotenv
import sys
import logging
import requests
import geopandas as gpd
import pandas as pd
import psycopg2
import io
import zipfile
from shapely.geometry import MultiPolygon

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('load_zipcode_data')

# Database connection parameters
load_dotenv()

# Construct DATABASE_URL from components
db_user = os.environ.get('POSTGRES_USER')
db_password = os.environ.get('POSTGRES_PASSWORD')
db_name = os.environ.get('POSTGRES_DB_NAME')
db_host = os.environ.get('POSTGIS_HOST', 'postgis_db')
db_port = os.environ.get('POSTGRES_PORT', '5433')


# Build the connection string
db_url = f"postgres://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

census_zcta_url = os.environ.get('CENSUS_ZCTA_URL', 'https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_us_zcta520.zip')

if not db_url:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def download_zcta_data():
    """Download ZCTA (ZIP Code Tabulation Areas) data from Census"""
    logger.info("Downloading ZCTA data from Census")
    
    # URL for 2020 ZCTA shapefile
    url = census_zcta_url
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to download ZCTA data: HTTP {response.status_code}")
            sys.exit(1)
        
        # Extract the zipfile contents
        z = zipfile.ZipFile(io.BytesIO(response.content))
        z.extractall("temp_zcta")
        
        logger.info("ZCTA data downloaded and extracted successfully")
        return "temp_zcta/tl_2020_us_zcta520.shp"
    
    except Exception as e:
        logger.error(f"Error downloading ZCTA data: {e}")
        raise

def download_commute_data():
    """Download real commute time data from Census ACS 5-year estimates"""
    logger.info("Downloading commute time data from Census")
    
    # Census API URL for commute time by ZIP Code Tabulation Area
    # Using ACS 5-year estimates for 2019 (most recent available)
    # Table B08303 - Travel Time to Work
    url = "https://api.census.gov/data/2019/acs/acs5?get=NAME,B08303_001E,B08303_002E,B08303_003E,B08303_004E,B08303_005E,B08303_006E,B08303_007E,B08303_008E,B08303_009E,B08303_010E,B08303_011E,B08303_012E,B08303_013E&for=zip%20code%20tabulation%20area:*&in=state:06"
    
    try:
        response = requests.get(url)
        if response.status_code != 200:
            logger.error(f"Failed to download commute data: HTTP {response.status_code}")
            logger.warning("Using sample commute data instead")
            return None
        
        # Convert JSON to DataFrame
        data = response.json()
        columns = data[0]
        rows = data[1:]
        
        df = pd.DataFrame(rows, columns=columns)
        logger.info(f"Downloaded commute data for {len(df)} ZCTAs")
        
        # Calculate average commute time
        # B08303_001E = Total workers
        # B08303_002E = Less than 5 minutes
        # B08303_003E = 5 to 9 minutes
        # B08303_004E = 10 to 14 minutes 
        # ...and so on
        
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
            (47, 'B08303_011E'),     # 45-59 minutes (midpoint 47)
            (67, 'B08303_012E'),     # 60-89 minutes (midpoint 67)
            (90, 'B08303_013E')      # 90+ minutes (use 90 as minimum)
        ]
        
        # Convert columns to numeric
        for _, col_name in time_ranges:
            df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        
        df['B08303_001E'] = pd.to_numeric(df['B08303_001E'], errors='coerce')
        
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
    
    except Exception as e:
        logger.error(f"Error downloading commute data: {e}")
        logger.warning("Using sample commute data instead")
        return None

def load_bay_area_zipcodes(shapefile_path):
    """
    Load Bay Area zipcodes from shapefile
    Filter for just the Bay Area counties
    """
    logger.info("Loading Bay Area zipcodes")
    
    try:
        # Read the shapefile
        gdf = gpd.read_file(shapefile_path)
        logger.info(f"Loaded {len(gdf)} ZCTAs from shapefile")
        
        # Print column names to debug
        logger.info(f"Available columns: {list(gdf.columns)}")
        
        # Check for state column
        state_col = None
        for col in ['STATEFP20', 'STATEFP', 'STATEFP10', 'STATE']:
            if col in gdf.columns:
                state_col = col
                break
        
        if not state_col:
            logger.warning("No state column found - using ZCTA prefixes only")
        else:
            logger.info(f"Using state column: {state_col}")
            # Filter for California
            ca_gdf = gdf[gdf[state_col] == '06']
            logger.info(f"Filtered to {len(ca_gdf)} California ZCTAs")
        
        # If we couldn't filter by state, use all data
        if not state_col:
            ca_gdf = gdf
        
        # Get ZCTA column name
        zcta_col = None
        for col in ['ZCTA5CE20', 'ZCTA5CE10', 'ZCTA5', 'ZCTA']:
            if col in gdf.columns:
                zcta_col = col
                break
                
        if not zcta_col:
            logger.error("No ZCTA column found in shapefile")
            raise ValueError("Could not identify ZCTA column in shapefile")
            
        logger.info(f"Using ZCTA column: {zcta_col}")
        
        # Define Bay Area counties
        bay_area_counties = ['San Francisco', 'Alameda', 'Contra Costa', 'Marin', 
                          'Napa', 'San Mateo', 'Santa Clara', 'Solano', 'Sonoma']

        # Attempt to filter by county if available in the data
        county_col = None
        for col in ['COUNTY', 'COUNTYFP', 'COUNTYFP20', 'county']:
            if col in ca_gdf.columns:
                county_col = col
                break

        if county_col:
            # If we have county info, use that to filter
            # Note: You would need a mapping from county FIPS to names
            # This is a simplified version assuming county names are available
            bay_area_gdf = ca_gdf[ca_gdf[county_col].isin(bay_area_counties)]
            logger.info(f"Filtered to {len(bay_area_gdf)} ZIP codes in Bay Area counties")
        else:
            # Fallback to filtering by ZIP code prefix
            # This is less accurate but better than nothing
            bay_area_zips = []
            
            # San Francisco prefixes
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('941')])
            
            # Alameda County (Oakland, Berkeley, etc.)
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('945') or 
                                 code.startswith('946') or code.startswith('947')])
            
            # Contra Costa County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('945') or code.startswith('944')])
            
            # San Mateo County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('940') or code.startswith('944')])
            
            # Santa Clara County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('95') and not code.startswith('959')])
            
            # Marin County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('949')])
            
            # Napa County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('945') and not code in bay_area_zips])
            
            # Sonoma County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('954') or code.startswith('955')])
            
            # Solano County
            bay_area_zips.extend([code for code in ca_gdf[zcta_col] if code.startswith('945') and not code in bay_area_zips])
            
            # Filter to the Bay Area zip codes
            bay_area_gdf = ca_gdf[ca_gdf[zcta_col].isin(bay_area_zips)]
            logger.info(f"Filtered to {len(bay_area_gdf)} Bay Area ZIP codes by code pattern")
        
        logger.info(f"Filtered to {len(bay_area_gdf)} Bay Area ZCTAs")
        
        # Ensure geometries are valid MultiPolygons
        bay_area_gdf['geometry'] = bay_area_gdf['geometry'].apply(
            lambda x: MultiPolygon([x]) if x.geom_type == 'Polygon' else x
        )
        
        # Create a county lookup dictionary for Bay Area ZIP codes
        county_lookup = {
            '941': 'San Francisco',  # San Francisco
            '940': 'San Mateo',      # San Mateo
            '944': 'San Mateo',      # San Mateo/Contra Costa
            '945': 'Contra Costa',   # Contra Costa/Alameda/Napa/Solano
            '946': 'Alameda',        # Alameda
            '947': 'Alameda',        # Alameda
            '948': 'Alameda',        # Alameda
            '949': 'Marin',          # Marin
            '950': 'Santa Clara',    # Santa Clara
            '951': 'Santa Clara',    # Santa Clara
            '952': 'Santa Clara',    # Santa Clara
            '953': 'Santa Clara',    # Santa Clara
            '954': 'Sonoma',         # Sonoma
            '955': 'Sonoma',         # Sonoma
            '956': 'Napa',           # Napa
            '957': 'Solano'          # Solano
        }

        # Assign counties based on ZIP code prefix
        bay_area_gdf['county'] = bay_area_gdf[zcta_col].apply(
            lambda z: county_lookup.get(z[:3], 'Bay Area')
        )
        bay_area_gdf['state'] = 'CA'
        bay_area_gdf['name'] = bay_area_gdf[zcta_col] + ' Area'
        
        # Create a simplified GeoDataFrame with just the columns we need
        result_gdf = gpd.GeoDataFrame({
            'zip': bay_area_gdf[zcta_col],
            'name': bay_area_gdf['name'],
            'county': bay_area_gdf['county'],
            'state': bay_area_gdf['state'],
            'geometry': bay_area_gdf['geometry']
        }, geometry='geometry')
        
        # Ensure proper CRS
        if result_gdf.crs is None or result_gdf.crs.to_epsg() != 4326:
            result_gdf = result_gdf.to_crs(epsg=4326)
        
        return result_gdf
    
    except Exception as e:
        logger.error(f"Error loading Bay Area zipcodes: {e}")
        raise

def insert_zipcodes_into_db(gdf, commute_df, conn):
    """Insert zipcodes into database with real commute data"""
    logger.info("Inserting zipcodes into database")
    
    cursor = conn.cursor()
    
    try:
        # First, clear existing data
        cursor.execute("TRUNCATE TABLE zipcode_ratings CASCADE")
        cursor.execute("TRUNCATE TABLE zipcodes CASCADE")
        
        # Insert each zipcode
        for idx, row in gdf.iterrows():
            # Convert geometry to WKT format
            wkt = row['geometry'].wkt
            
            cursor.execute("""
            INSERT INTO zipcodes (zip, name, county, state, geometry)
            VALUES (%s, %s, %s, %s, ST_Multi(ST_GeomFromText(%s, 4326)))
            """, (row['zip'], row['name'], row['county'], row['state'], wkt))
            
            # Log progress every 50 records
            if idx % 50 == 0:
                logger.info(f"Inserted {idx} zipcodes")
        
        # Commit the zipcodes first
        conn.commit()
        
        # Now insert commute time ratings if available
        if commute_df is not None:
            logger.info("Inserting real commute time data")
            for idx, row in commute_df.iterrows():
                # Check if zipcode exists before inserting ratings
                cursor.execute("SELECT 1 FROM zipcodes WHERE zip = %s", (row['zip'],))
                if cursor.fetchone():  # Only insert if zipcode exists
                    cursor.execute("""
                    INSERT INTO zipcode_ratings (zip, rating_type, rating_value, confidence, source, source_url)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (zip, rating_type) DO UPDATE
                    SET rating_value = EXCLUDED.rating_value,
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source,
                        source_url = EXCLUDED.source_url
                    """, (
                        row['zip'], 
                        'commuteTime', 
                        row['commute_time'],
                        0.9, 
                        'US Census Bureau American Community Survey', 
                        'https://www.census.gov/programs-surveys/acs'
                    ))
                else:
                    logger.warning(f"Skipping commute rating for non-existent zipcode: {row['zip']}")
                
                # Log progress occasionally
                if idx % 100 == 0:
                    logger.info(f"Inserted {idx} commute ratings")
        else:
            logger.warning("No real commute data available, skipping commute rating insertion")
        
        conn.commit()
        logger.info("Successfully inserted all zipcodes and ratings")
    
    except Exception as e:
        logger.error(f"Error inserting zipcodes: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    """Main execution function"""
    conn = None
    try:
        # Download ZCTA data
        shapefile_path = download_zcta_data()
        
        # Load Bay Area zipcodes
        gdf = load_bay_area_zipcodes(shapefile_path)
        
        # Download real commute time data
        commute_df = download_commute_data()
        
        # Connect to database
        conn = get_db_connection()
        
        # Insert zipcodes and real data into database
        insert_zipcodes_into_db(gdf, commute_df, conn)
        
        logger.info("Bay Area zipcode data loaded successfully")
    
    except Exception as e:
        logger.error(f"Error loading zipcode data: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
        
        # Clean up temporary files
        import shutil
        if os.path.exists("temp_zcta"):
            shutil.rmtree("temp_zcta")

if __name__ == "__main__":
    main()
