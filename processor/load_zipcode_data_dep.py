#!/usr/bin/env python3
"""
Data loader script for Bay Area Housing Criteria Map
This script loads zipcode boundaries from Census data
"""

import os
import sys
import logging
import requests
import geopandas as gpd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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
db_url = os.environ.get('DATABASE_URL')
if not db_url:
    logger.error("DATABASE_URL environment variable not set")
    sys.exit(1)

# Bay Area counties FIPS codes
BAY_AREA_COUNTIES = {
    '001': 'Alameda',
    '013': 'Contra Costa',
    '041': 'Marin',
    '055': 'Napa',
    '075': 'San Francisco',
    '081': 'San Mateo',
    '085': 'Santa Clara',
    '095': 'Solano',
    '097': 'Sonoma'
}

def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(db_url)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

def download_zcta_data():
    """Download ZCTA (ZIP Code Tabulation Areas) data from Census"""
    logger.info("Downloading ZCTA data from Census")
    
    # URL for 2020 ZCTA shapefile
    url = "https://www2.census.gov/geo/tiger/TIGER2020/ZCTA520/tl_2020_us_zcta520.zip"
    
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
        
        # For this simplified example, we'll filter by the first 2 digits of the ZCTA
        # Bay Area zipcodes generally start with 94, 95, or 93
        bay_area_gdf = ca_gdf[
            (ca_gdf[zcta_col].str[:2] == '94') | 
            (ca_gdf[zcta_col].str[:2] == '95') |
            (ca_gdf[zcta_col].str[:2] == '93')
        ]
        
        logger.info(f"Filtered to {len(bay_area_gdf)} Bay Area ZCTAs")
        
        # Ensure geometries are valid MultiPolygons
        bay_area_gdf['geometry'] = bay_area_gdf['geometry'].apply(
            lambda x: MultiPolygon([x]) if x.geom_type == 'Polygon' else x
        )
        
        # Add county placeholder - in production you'd do a proper spatial join
        bay_area_gdf['county'] = 'Bay Area'
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
def insert_zipcodes_into_db(gdf, conn):
    """Insert zipcodes into database"""
    logger.info("Inserting zipcodes into database")
    
    cursor = conn.cursor()
    
    try:
        # First, clear existing data
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
        
        # Generate sample ratings for testing
        cursor.execute("""
        INSERT INTO zipcode_ratings (zip, rating_type, rating_value, confidence, source, source_url)
        SELECT 
            zip, 
            'schoolRating', 
            (random() * 5 + 5)::numeric(10,1), 
            0.8, 
            'Sample Data', 
            'https://example.com'
        FROM zipcodes
        """)
        
        cursor.execute("""
        INSERT INTO zipcode_ratings (zip, rating_type, rating_value, confidence, source, source_url)
        SELECT 
            zip, 
            'nicheRating', 
            (random() * 5 + 5)::numeric(10,1), 
            0.7, 
            'Sample Data', 
            'https://example.com'
        FROM zipcodes
        """)
        
        cursor.execute("""
        INSERT INTO zipcode_ratings (zip, rating_type, rating_value, confidence, source, source_url)
        SELECT 
            zip, 
            'crimeRate', 
            (random() * 5 + 5)::numeric(10,1), 
            0.6, 
            'Sample Data', 
            'https://example.com'
        FROM zipcodes
        """)
        
        cursor.execute("""
        INSERT INTO zipcode_ratings (zip, rating_type, rating_value, confidence, source, source_url)
        SELECT 
            zip, 
            'commuteTime', 
            (random() * 5 + 5)::numeric(10,1), 
            0.5, 
            'Sample Data', 
            'https://example.com'
        FROM zipcodes
        """)
        
        conn.commit()
        logger.info("Successfully inserted all zipcodes and sample ratings")
    
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
        
        # Connect to database
        conn = get_db_connection()
        
        # Insert zipcodes into database
        insert_zipcodes_into_db(gdf, conn)
        
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