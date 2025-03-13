-- Enhanced database schema for Bay Area Housing Criteria Map
-- This schema adds tables for storing detailed neighborhood data

-- Enable PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Main zipcodes table with geometries and metadata
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
);

-- Zipcode ratings table (used for map visualization)
CREATE TABLE IF NOT EXISTS zipcode_ratings (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    rating_type TEXT NOT NULL,
    rating_value REAL, 
    confidence REAL,  -- Confidence score (0-1) indicating data quality
    source TEXT,      -- Source of the data
    source_url TEXT,  -- URL to the data source
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data source tracking for scheduling updates
CREATE TABLE IF NOT EXISTS data_sources (
    source_name TEXT PRIMARY KEY,
    last_updated TIMESTAMP,
    next_update TIMESTAMP,
    update_frequency INTERVAL,
    url TEXT,
    notes TEXT
);

-- School data by zipcode
CREATE TABLE IF NOT EXISTS schools (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    school_name TEXT,
    school_type TEXT, -- public, private, charter
    grade_range TEXT, -- e.g., K-5, 6-8, 9-12
    api_score INTEGER,
    graduation_rate NUMERIC(5,2),
    college_going_rate NUMERIC(5,2),
    rating NUMERIC(3,1),
    lat REAL,
    lon REAL,
    address TEXT,
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Crime data by zipcode
CREATE TABLE IF NOT EXISTS crime_stats (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    year INTEGER,
    violent_crime_count INTEGER,
    property_crime_count INTEGER,
    violent_crime_rate NUMERIC(7,2), -- per 100k population
    property_crime_rate NUMERIC(7,2), -- per 100k population
    overall_crime_rate NUMERIC(7,2), -- per 100k population
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Housing market data by zipcode
CREATE TABLE IF NOT EXISTS housing_market (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    year_month TEXT, -- YYYY-MM format
    median_list_price NUMERIC,
    median_sold_price NUMERIC,
    median_days_on_market INTEGER,
    num_homes_sold INTEGER,
    inventory_count INTEGER,
    price_per_sqft NUMERIC(7,2),
    year_over_year_change NUMERIC(5,2), -- percentage
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Amenities by zipcode
CREATE TABLE IF NOT EXISTS amenities (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    amenity_type TEXT, -- restaurant, park, grocery, etc.
    amenity_name TEXT,
    amenity_count INTEGER,
    lat REAL,
    lon REAL,
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Demographic data by zipcode
CREATE TABLE IF NOT EXISTS demographics (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    year INTEGER,
    total_population INTEGER,
    median_age NUMERIC(4,1),
    household_size NUMERIC(3,1),
    race_white NUMERIC(5,2), -- percentages
    race_black NUMERIC(5,2),
    race_asian NUMERIC(5,2),
    race_hispanic NUMERIC(5,2),
    race_other NUMERIC(5,2),
    education_less_than_highschool NUMERIC(5,2), -- percentages
    education_highschool NUMERIC(5,2),
    education_some_college NUMERIC(5,2),
    education_bachelors NUMERIC(5,2),
    education_graduate NUMERIC(5,2),
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Commute data by zipcode
CREATE TABLE IF NOT EXISTS commute_stats (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    year INTEGER,
    avg_commute_minutes NUMERIC(5,1),
    drive_alone_pct NUMERIC(5,2),
    carpool_pct NUMERIC(5,2),
    public_transit_pct NUMERIC(5,2),
    walk_pct NUMERIC(5,2),
    bike_pct NUMERIC(5,2),
    work_from_home_pct NUMERIC(5,2),
    commute_lt_10min_pct NUMERIC(5,2),
    commute_10to20min_pct NUMERIC(5,2),
    commute_20to30min_pct NUMERIC(5,2),
    commute_30to45min_pct NUMERIC(5,2),
    commute_45to60min_pct NUMERIC(5,2),
    commute_gt60min_pct NUMERIC(5,2),
    source TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Niche.com detailed ratings by zipcode
CREATE TABLE IF NOT EXISTS niche_ratings (
    id SERIAL PRIMARY KEY,
    zip TEXT REFERENCES zipcodes(zip),
    overall_grade TEXT, -- A+ to F scale
    public_schools_grade TEXT,
    housing_grade TEXT,
    crime_safety_grade TEXT,
    nightlife_grade TEXT,
    family_friendly_grade TEXT,
    diversity_grade TEXT,
    jobs_grade TEXT,
    cost_of_living_grade TEXT,
    outdoor_activities_grade TEXT,
    commute_grade TEXT,
    health_fitness_grade TEXT,
    weather_grade TEXT,
    overall_rank INTEGER,
    source_url TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indices for better query performance
CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_zip ON zipcode_ratings(zip);
CREATE INDEX IF NOT EXISTS idx_zipcode_ratings_type ON zipcode_ratings(rating_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_zipcode_ratings_zip_type ON zipcode_ratings(zip, rating_type);

CREATE INDEX IF NOT EXISTS idx_schools_zip ON schools(zip);
CREATE INDEX IF NOT EXISTS idx_crime_stats_zip ON crime_stats(zip);
CREATE INDEX IF NOT EXISTS idx_housing_market_zip ON housing_market(zip);
CREATE INDEX IF NOT EXISTS idx_amenities_zip ON amenities(zip);
CREATE INDEX IF NOT EXISTS idx_demographics_zip ON demographics(zip);
CREATE INDEX IF NOT EXISTS idx_commute_stats_zip ON commute_stats(zip);
CREATE INDEX IF NOT EXISTS idx_niche_ratings_zip ON niche_ratings(zip);

-- Spatial index on zipcode geometries
CREATE INDEX IF NOT EXISTS idx_zipcodes_geometry ON zipcodes USING GIST(geometry);
