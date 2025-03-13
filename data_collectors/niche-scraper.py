#!/usr/bin/env python3
"""
Comprehensive Niche.com Data Scraper for Bay Area ZIP Codes
This script responsibly collects data from Niche.com with appropriate rate limiting
"""

import os
import sys
import time
import logging
import json
import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from datetime import datetime
import random
import re
from concurrent.futures import ThreadPoolExecutor
import sqlite3
import argparse

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("niche_scraper.log")
    ]
)
logger = logging.getLogger('niche_scraper')

# Optional database connection parameters
db_url = os.environ.get('DATABASE_URL', '')

class NicheDataScraper:
    """Comprehensive scraper for Niche.com zipcode data"""
    
    def __init__(self, cache_dir="niche_cache", use_cache=True, delay_min=3, delay_max=7):
        """
        Initialize the scraper
        
        Parameters:
        - cache_dir: Directory to store cached pages
        - use_cache: Whether to use cached pages
        - delay_min: Minimum delay between requests in seconds
        - delay_max: Maximum delay between requests in seconds
        """
        self.cache_dir = cache_dir
        self.use_cache = use_cache
        self.delay_min = delay_min
        self.delay_max = delay_max
        
        # Create cache directory
        os.makedirs(cache_dir, exist_ok=True)
        
        # Initialize SQLite cache database
        self.init_cache_db()
        
        # Initialize session
        self.session = requests.Session()
        
        # Rotate user agents to be less detectable
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.55',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
        ]
        
        # Update the user agent for the session
        self.update_user_agent()
    
    def init_cache_db(self):
        """Initialize SQLite cache database"""
        self.cache_db = os.path.join(self.cache_dir, "niche_cache.db")
        
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS page_cache (
            url TEXT PRIMARY KEY,
            content TEXT,
            timestamp INTEGER
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS zipcode_data (
            zipcode TEXT PRIMARY KEY,
            data TEXT,
            timestamp INTEGER
        )
        """)
        
        conn.commit()
        conn.close()
    
    def update_user_agent(self):
        """Randomly select a user agent"""
        user_agent = random.choice(self.user_agents)
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        })
        return user_agent
    
    def get_page_from_cache(self, url):
        """Get page content from cache if available and not expired"""
        if not self.use_cache:
            return None
            
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Cache expires after 30 days (2592000 seconds)
        cursor.execute(
            "SELECT content FROM page_cache WHERE url = ? AND timestamp > ?", 
            (url, int(time.time()) - 2592000)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            logger.info(f"Retrieved {url} from cache")
            return result[0]
        
        return None
    
    def save_page_to_cache(self, url, content):
        """Save page content to cache"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO page_cache (url, content, timestamp) VALUES (?, ?, ?)",
            (url, content, int(time.time()))
        )
        
        conn.commit()
        conn.close()
    
    def save_zipcode_data(self, zipcode, data):
        """Save processed zipcode data to cache"""
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        cursor.execute(
            "INSERT OR REPLACE INTO zipcode_data (zipcode, data, timestamp) VALUES (?, ?, ?)",
            (zipcode, json.dumps(data), int(time.time()))
        )
        
        conn.commit()
        conn.close()
    
    def get_zipcode_data_from_cache(self, zipcode):
        """Get processed zipcode data from cache if available and not expired"""
        if not self.use_cache:
            return None
            
        conn = sqlite3.connect(self.cache_db)
        cursor = conn.cursor()
        
        # Cache expires after 30 days (2592000 seconds)
        cursor.execute(
            "SELECT data FROM zipcode_data WHERE zipcode = ? AND timestamp > ?", 
            (zipcode, int(time.time()) - 2592000)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            logger.info(f"Retrieved zipcode {zipcode} data from cache")
            return json.loads(result[0])
        
        return None
    
    def fetch_page(self, url):
        """
        Fetch a page with proper delays and caching
        
        Returns the page content as string
        """
        # Check cache first
        cached_content = self.get_page_from_cache(url)
        if cached_content:
            return cached_content
        
        # If not in cache, fetch the page
        logger.info(f"Fetching {url}")
        
        # Update user agent for each request
        self.update_user_agent()
        
        # Random delay between requests
        delay = random.uniform(self.delay_min, self.delay_max)
        logger.info(f"Waiting {delay:.2f} seconds before request")
        time.sleep(delay)
        
        try:
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                # Save page to cache
                self.save_page_to_cache(url, response.text)
                return response.text
            else:
                logger.error(f"Failed to fetch {url}: HTTP {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def scrape_zipcode(self, zipcode):
        """
        Scrape all available data for a zipcode
        
        Returns a dictionary with all extracted data
        """
        # Check if we already have cached data
        cached_data = self.get_zipcode_data_from_cache(zipcode)
        if cached_data:
            return cached_data
        
        # Base URL for the zipcode
        base_url = f"https://www.niche.com/places-to-live/z/{zipcode}/"
        
        # URLs for different sections
        urls = {
            'main': base_url,
            'real_estate': f"{base_url}real-estate/",
            'residents': f"{base_url}residents/",
            'reviews': f"{base_url}reviews/",
            'schools': f"{base_url}schools/"
        }
        
        # Aggregate data from all pages
        data = {
            'zipcode': zipcode,
            'timestamp': datetime.now().isoformat(),
            'url': base_url
        }
        
        # Fetch and process each page
        for page_name, url in urls.items():
            page_content = self.fetch_page(url)
            
            if page_content:
                # Process the page based on its type
                if page_name == 'main':
                    data.update(self.extract_main_page_data(page_content))
                elif page_name == 'real_estate':
                    data.update(self.extract_real_estate_data(page_content))
                elif page_name == 'residents':
                    data.update(self.extract_resident_data(page_content))
                elif page_name == 'reviews':
                    data.update(self.extract_review_data(page_content))
                elif page_name == 'schools':
                    data.update(self.extract_school_data(page_content))
        
        # Save processed data to cache
        self.save_zipcode_data(zipcode, data)
        
        return data
    
    def extract_main_page_data(self, html_content):
        """Extract data from the main zipcode page"""
        if not html_content:
            return {}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {}
        
        # Extract area name
        name_elem = soup.select_one("h1.profile-name")
        if name_elem:
            data['area_name'] = name_elem.text.strip()
        
        # Extract overall grade
        grade_elem = soup.select_one("div.overall-grade span.niche__grade")
        if grade_elem:
            data['overall_grade'] = grade_elem.text.strip()
        
        # Extract rankings
        rankings = []
        ranking_elements = soup.select("div.rankings-list div.ranking")
        
        for elem in ranking_elements:
            rank_text = elem.select_one("div.rank")
            title_text = elem.select_one("div.title")
            
            if rank_text and title_text:
                rankings.append({
                    'rank': rank_text.text.strip(),
                    'title': title_text.text.strip()
                })
        
        data['rankings'] = rankings
        
        # Extract category grades
        category_grades = {}
        grade_elems = soup.select("li.report-card-list__item")
        
        for elem in grade_elems:
            category = elem.select_one("h4.report-card-list__category")
            grade = elem.select_one("span.niche__grade")
            
            if category and grade:
                category_name = category.text.strip()
                grade_value = grade.text.strip()
                category_grades[category_name] = grade_value
        
        data['category_grades'] = category_grades
        
        # Extract summary/about text
        about_elem = soup.select_one("div.profile-section--about")
        if about_elem:
            summary_elem = about_elem.select_one("div.profile-section__content p")
            if summary_elem:
                data['summary'] = summary_elem.text.strip()
        
        return data
    
    def extract_real_estate_data(self, html_content):
        """Extract real estate data"""
        if not html_content:
            return {}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {'real_estate': {}}
        
        # Extract median home values
        home_value_card = soup.select_one("div.profile-card:contains('Median Home Value')")
        if home_value_card:
            value_elem = home_value_card.select_one("div.scalar__value")
            if value_elem:
                # Extract numeric value (remove $ and commas)
                value_text = value_elem.text.strip()
                data['real_estate']['median_home_value'] = self.parse_currency(value_text)
        
        # Extract median rent
        rent_card = soup.select_one("div.profile-card:contains('Median Rent')")
        if rent_card:
            value_elem = rent_card.select_one("div.scalar__value")
            if value_elem:
                value_text = value_elem.text.strip()
                data['real_estate']['median_rent'] = self.parse_currency(value_text)
        
        # Extract ownership percentages
        ownership_section = soup.select_one("div:contains('% Own')")
        if ownership_section:
            own_elem = ownership_section.select_one("div.fact__figure")
            rent_elem = ownership_section.find_next_sibling().select_one("div.fact__figure")
            
            if own_elem:
                own_text = own_elem.text.strip()
                data['real_estate']['percent_own'] = self.parse_percentage(own_text)
            
            if rent_elem:
                rent_text = rent_elem.text.strip()
                data['real_estate']['percent_rent'] = self.parse_percentage(rent_text)
        
        # Extract housing types
        housing_types = {}
        housing_type_section = soup.select_one("div.profile-section:contains('Housing Types')")
        if housing_type_section:
            type_items = housing_type_section.select("li.profile-histogram__list-item")
            for item in type_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    housing_types[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['real_estate']['housing_types'] = housing_types
        
        # Extract year built distribution
        year_built = {}
        year_section = soup.select_one("div.profile-section:contains('Year Built')")
        if year_section:
            year_items = year_section.select("li.profile-histogram__list-item")
            for item in year_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    year_built[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['real_estate']['year_built'] = year_built
        
        return data
    
    def extract_resident_data(self, html_content):
        """Extract demographic and resident data"""
        if not html_content:
            return {}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {'demographics': {}}
        
        # Extract population
        pop_section = soup.select_one("div.profile-card:contains('Population')")
        if pop_section:
            value_elem = pop_section.select_one("div.scalar__value")
            if value_elem:
                pop_text = value_elem.text.strip()
                data['demographics']['population'] = self.parse_number(pop_text)
        
        # Extract density
        density_section = soup.select_one("div.profile-card:contains('Density')")
        if density_section:
            value_elem = density_section.select_one("div.scalar__value")
            if value_elem:
                density_text = value_elem.text.strip()
                data['demographics']['population_density'] = self.parse_number(density_text.split(' ')[0])
        
        # Extract median age
        age_section = soup.select_one("div.profile-card:contains('Median Age')")
        if age_section:
            value_elem = age_section.select_one("div.scalar__value")
            if value_elem:
                age_text = value_elem.text.strip()
                data['demographics']['median_age'] = float(age_text)
        
        # Extract race/ethnicity
        race_data = {}
        race_section = soup.select_one("div.profile-section:contains('Race & Ethnicity')")
        if race_section:
            race_items = race_section.select("li.profile-histogram__list-item")
            for item in race_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    race_data[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['demographics']['race_ethnicity'] = race_data
        
        # Extract education levels
        education_data = {}
        education_section = soup.select_one("div.profile-section:contains('Educational Attainment')")
        if education_section:
            education_items = education_section.select("li.profile-histogram__list-item")
            for item in education_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    education_data[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['demographics']['education'] = education_data
        
        # Extract household income
        income_data = {}
        income_section = soup.select_one("div.profile-section:contains('Household Income')")
        if income_section:
            income_items = income_section.select("li.profile-histogram__list-item")
            for item in income_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    income_data[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['demographics']['household_income'] = income_data
        
        # Extract employment info
        employment_data = {}
        employment_section = soup.select_one("div.profile-section:contains('Employment')")
        if employment_section:
            employment_items = employment_section.select("li.profile-histogram__list-item")
            for item in employment_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    employment_data[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['demographics']['employment'] = employment_data
        
        # Extract commute times
        commute_data = {}
        commute_section = soup.select_one("div.profile-section:contains('Commute Time')")
        if commute_section:
            commute_items = commute_section.select("li.profile-histogram__list-item")
            for item in commute_items:
                label = item.select_one("span.label")
                value = item.select_one("div.fact__figure")
                
                if label and value:
                    commute_data[label.text.strip()] = self.parse_percentage(value.text.strip())
        
        data['demographics']['commute_times'] = commute_data
        
        return data
    
    def extract_review_data(self, html_content):
        """Extract review data"""
        if not html_content:
            return {}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {'reviews': []}
        
        # Extract rating distribution
        rating_distribution = {}
        rating_section = soup.select_one("div.rating-distribution")
        if rating_section:
            rating_items = rating_section.select("span.rating-label")
            
            for item in rating_items:
                stars = item.select_one("span.stars")
                if stars:
                    star_count = len(stars.select("i.icon-full"))
                    count_elem = item.select_one("span.count")
                    
                    if count_elem:
                        count = self.parse_number(count_elem.text.strip())
                        rating_distribution[f"{star_count}_star"] = count
        
        data['rating_distribution'] = rating_distribution
        
        # Extract individual reviews
        review_elems = soup.select("div.review-card")
        reviews = []
        
        for elem in review_elems[:10]:  # Limit to 10 reviews
            review = {}
            
            # Extract rating
            rating_elem = elem.select_one("div.review-star-rating")
            if rating_elem:
                stars = rating_elem.select("i.icon-full")
                review['rating'] = len(stars)
            
            # Extract title
            title_elem = elem.select_one("h3.review-card-title")
            if title_elem:
                review['title'] = title_elem.text.strip()
            
            # Extract content
            content_elem = elem.select_one("div.review-card-content")
            if content_elem:
                review['content'] = content_elem.text.strip()
            
            # Extract date
            date_elem = elem.select_one("div.review-card-date")
            if date_elem:
                review['date'] = date_elem.text.strip()
            
            reviews.append(review)
        
        data['reviews'] = reviews
        
        return data
    
    def extract_school_data(self, html_content):
        """Extract school data"""
        if not html_content:
            return {}
            
        soup = BeautifulSoup(html_content, 'html.parser')
        data = {'schools': []}
        
        # Extract schools
        school_elems = soup.select("li.search-results__list-item")
        schools = []
        
        for elem in school_elems:
            school = {}
            
            # Extract name
            name_elem = elem.select_one("h2.search-result__title")
            if name_elem:
                school['name'] = name_elem.text.strip()
            
            # Extract grade
            grade_elem = elem.select_one("div.niche__grade")
            if grade_elem:
                school['grade'] = grade_elem.text.strip()
            
            # Extract type and grade range
            type_elem = elem.select_one("span.search-result-fact:first-child")
            if type_elem:
                school['type'] = type_elem.text.strip()
            
            grades_elem = elem.select_one("span.search-result-fact:nth-child(2)")
            if grades_elem:
                school['grades'] = grades_elem.text.strip()
            
            # Extract rating
            rating_elem = elem.select_one("div.search-result-rating")
            if rating_elem:
                stars = rating_elem.select("i.icon-full")
                school['rating'] = len(stars)
            
            schools.append(school)
        
        data['schools'] = schools
        
        return data
    
    def parse_currency(self, text):
        """Parse currency values (e.g., $1,234)"""
        if not text:
            return None
            
        # Remove $ and commas, then convert to float
        number_text = re.sub(r'[^\d.]', '', text)
        if number_text:
            return float(number_text)
        
        return None
    
    def parse_percentage(self, text):
        """Parse percentage values (e.g., 12.3%)"""
        if not text:
            return None
            
        # Remove % symbol and convert to float
        number_text = re.sub(r'[^\d.]', '', text)
        if number_text:
            return float(number_text)
        
        return None
    
    def parse_number(self, text):
        """Parse numeric values (handles commas and K/M suffixes)"""
        if not text:
            return None
            
        text = text.strip()
        
        # Handle K/M suffixes
        multiplier = 1
        if text.endswith('K'):
            multiplier = 1000
            text = text[:-1]
        elif text.endswith('M'):
            multiplier = 1000000
            text = text[:-1]
        
        # Remove commas and convert to number
        number_text = re.sub(r'[^\d.]', '', text)
        if number_text:
            return float(number_text) * multiplier
        
        return None
    
    def scrape_zipcodes(self, zipcodes, max_workers=5):
        """
        Scrape multiple zipcodes in parallel
        
        Parameters:
        - zipcodes: List of zipcodes to scrape
        - max_workers: Maximum number of parallel workers
        
        Returns a dictionary mapping zipcodes to their data
        """
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scraping tasks
            future_to_zipcode = {executor.submit(self.scrape_zipcode, zipcode): zipcode for zipcode in zipcodes}
            
            # Process results as they complete
            for i, future in enumerate(future_to_zipcode):
                zipcode = future_to_zipcode[future]
                try:
                    data = future.result()
                    results[zipcode] = data
                    logger.info(f"Completed {i+1}/{len(zipcodes)}: {zipcode}")
                except Exception as e:
                    logger.error(f"Error scraping {zipcode}: {e}")
        
        return results
    
    def save_to_csv(self, results, filename="niche_data.csv"):
        """
        Save results to CSV
        
        Parameters:
        - results: Dictionary mapping zipcodes to their data
        - filename: Output CSV filename
        """
        # Flatten nested structure for CSV
        rows = []
        
        for zipcode, data in results.items():
            row = {'zipcode': zipcode}
            
            # Add main data
            if 'area_name' in data:
                row['area_name'] = data['area_name']
            
            if 'overall_grade' in data:
                row['overall_grade'] = data['overall_grade']
            
            if 'summary' in data:
                row['summary'] = data['summary']
            
            # Add category grades
            if 'category_grades' in data:
                for category, grade in data['category_grades'].items():
                    safe_category = category.replace(' ', '_').lower()
                    row[f'grade_{safe_category}'] = grade
            
            # Add real estate data
            if 'real_estate' in data:
                re_data = data['real_estate']
                for key, value in re_data.items():
                    if key not in ['housing_types', 'year_built']:
                        row[f're_{key}'] = value
            
            # Add demographic data
            if 'demographics' in data and 'population' in data['demographics']:
                row['population'] = data['demographics']['population']
            
            if 'demographics' in data and 'median_age' in data['demographics']:
                row['median_age'] = data['demographics']['median_age']
            
            rows.append(row)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(rows)
        df.to_csv(filename, index=False)
        logger.info(f"Saved {len(rows)} zipcodes to {filename}")
        
        return df
    
    def save_to_database(self, results, db_connection=None):
        """
        Save results to PostgreSQL database
        
        Parameters:
        - results: Dictionary mapping zipcodes to their data
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
            
            # Process each zipcode
            for zipcode, data in results.items():
                # Update niche_ratings table
                if 'category_grades' in data and 'overall_grade' in data:
                    try:
                        # Convert letter grades to values
                        grade_map = {
                            'A+': 10.0, 'A': 9.5, 'A-': 9.0,
                            'B+': 8.5, 'B': 8.0, 'B-': 7.5,
                            'C+': 7.0, 'C': 6.5, 'C-': 6.0,
                            'D+': 5.5, 'D': 5.0, 'D-': 4.5,
                            'F+': 4.0, 'F': 3.5, 'F-': 3.0
                        }
                        
                        grades = {
                            'overall_grade': data['overall_grade']
                        }
                        
                        for category, grade in data.get('category_grades', {}).items():
                            # Normalize category names
                            key = category.replace(' ', '_').lower()
                            if 'public schools' in category.lower():
                                key = 'public_schools_grade'
                            elif 'crime & safety' in category.lower():
                                key = 'crime_safety_grade'
                            elif 'good for families' in category.lower():
                                key = 'family_friendly_grade'
                            elif 'cost of living' in category.lower():
                                key = 'cost_of_living_grade'
                            elif 'jobs' in category.lower():
                                key = 'jobs_grade'
                            elif 'nightlife' in category.lower():
                                key = 'nightlife_grade'
                            elif 'diversity' in category.lower():
                                key = 'diversity_grade'
                            
                            grades[key] = grade
                        
                        # Insert into niche_ratings table
                        cursor.execute("""
                        INSERT INTO niche_ratings (
                            zip, overall_grade, public_schools_grade, housing_grade, 
                            crime_safety_grade, nightlife_grade, family_friendly_grade,
                            diversity_grade, jobs_grade, cost_of_living_grade,
                            source_url, last_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (zip) DO UPDATE SET
                            overall_grade = EXCLUDED.overall_grade,
                            public_schools_grade = EXCLUDED.public_schools_grade,
                            housing_grade = EXCLUDED.housing_grade,
                            crime_safety_grade = EXCLUDED.crime_safety_grade,
                            nightlife_grade = EXCLUDED.nightlife_grade,
                            family_friendly_grade = EXCLUDED.family_friendly_grade,
                            diversity_grade = EXCLUDED.diversity_grade,
                            jobs_grade = EXCLUDED.jobs_grade,
                            cost_of_living_grade = EXCLUDED.cost_of_living_grade,
                            source_url = EXCLUDED.source_url,
                            last_updated = EXCLUDED.last_updated
                        """, (
                            zipcode,
                            grades.get('overall_grade'),
                            grades.get('public_schools_grade'),
                            grades.get('housing_grade'),
                            grades.get('crime_safety_grade'),
                            grades.get('nightlife_grade'),
                            grades.get('family_friendly_grade'),
                            grades.get('diversity_grade'),
                            grades.get('jobs_grade'),
                            grades.get('cost_of_living_grade'),
                            data.get('url'),
                            datetime.now()
                        ))
                        
                        # Also update zipcode_ratings table for map visualization
                        for category, grade in grades.items():
                            if grade and category != 'overall_grade':
                                # Convert grade to numeric value
                                rating_value = grade_map.get(grade, 5.0)
                                
                                # Normalize rating type
                                rating_type = category.replace('_grade', 'Rating')
                                
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
                                    zipcode,
                                    rating_type,
                                    rating_value,
                                    0.85,  # confidence score
                                    'Niche.com',
                                    data.get('url'),
                                    datetime.now()
                                ))
                    except Exception as e:
                        logger.error(f"Error inserting niche ratings for {zipcode}: {e}")
                
                # Update housing_market table
                if 'real_estate' in data:
                    try:
                        re_data = data['real_estate']
                        cursor.execute("""
                        INSERT INTO housing_market (
                            zip, year_month, median_list_price, median_sold_price,
                            source, last_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (zip, year_month) DO UPDATE SET
                            median_list_price = EXCLUDED.median_list_price,
                            median_sold_price = EXCLUDED.median_sold_price,
                            source = EXCLUDED.source,
                            last_updated = EXCLUDED.last_updated
                        """, (
                            zipcode,
                            datetime.now().strftime('%Y-%m'),
                            re_data.get('median_home_value'),
                            re_data.get('median_home_value'),  # Using list price as sold price since not available
                            'Niche.com',
                            datetime.now()
                        ))
                        
                        # Also update the zipcodes table
                        cursor.execute("""
                        UPDATE zipcodes SET
                            median_home_value = %s,
                            median_rent = %s,
                            ownership_percent = %s
                        WHERE zip = %s
                        """, (
                            re_data.get('median_home_value'),
                            re_data.get('median_rent'),
                            re_data.get('percent_own'),
                            zipcode
                        ))
                    except Exception as e:
                        logger.error(f"Error inserting housing data for {zipcode}: {e}")
                
                # Update demographics table
                if 'demographics' in data:
                    try:
                        demo_data = data['demographics']
                        cursor.execute("""
                        INSERT INTO demographics (
                            zip, year, total_population, median_age,
                            source, last_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (zip, year) DO UPDATE SET
                            total_population = EXCLUDED.total_population,
                            median_age = EXCLUDED.median_age,
                            source = EXCLUDED.source,
                            last_updated = EXCLUDED.last_updated
                        """, (
                            zipcode,
                            datetime.now().year,
                            demo_data.get('population'),
                            demo_data.get('median_age'),
                            'Niche.com',
                            datetime.now()
                        ))
                        
                        # Also update the zipcodes table
                        cursor.execute("""
                        UPDATE zipcodes SET
                            population = %s
                        WHERE zip = %s
                        """, (
                            demo_data.get('population'),
                            zipcode
                        ))
                        
                        # Process race/ethnicity data
                        if 'race_ethnicity' in demo_data:
                            race_data = demo_data['race_ethnicity']
                            
                            # Map Niche categories to database columns
                            race_mapping = {
                                'White': 'race_white',
                                'Hispanic': 'race_hispanic',
                                'Black': 'race_black',
                                'Asian': 'race_asian',
                                'Two or More Races': 'race_other',
                                'Other': 'race_other'
                            }
                            
                            race_values = {}
                            for race, percentage in race_data.items():
                                for key, column in race_mapping.items():
                                    if key.lower() in race.lower():
                                        race_values[column] = percentage
                                        break
                            
                            # Update demographics with race data
                            update_columns = []
                            update_values = []
                            
                            for column, value in race_values.items():
                                update_columns.append(f"{column} = %s")
                                update_values.append(value)
                            
                            if update_columns and update_values:
                                update_query = f"""
                                UPDATE demographics SET
                                    {', '.join(update_columns)}
                                WHERE zip = %s AND year = %s
                                """
                                
                                cursor.execute(update_query, update_values + [zipcode, datetime.now().year])
                    except Exception as e:
                        logger.error(f"Error inserting demographic data for {zipcode}: {e}")
            
            conn.commit()
            logger.info(f"Successfully saved {len(results)} zipcodes to database")
            return True
            
        except Exception as e:
            logger.error(f"Database error: {e}")
            return False

def get_bay_area_zipcodes():
    """Get list of Bay Area zipcodes"""
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
    
    # South Bay (San Jose, etc.)
    bay_area_zips.extend(['94022', '94024', '94035', '94039', '94040', '94041', '94043', '94085', 
                          '94086', '94087', '94089', '94301', '94303', '94304', '94305', '94306', 
                          '95002', '95008', '95014', '95030', '95032', '95035', '95050', '95051', 
                          '95054', '95070', '95110', '95111', '95112', '95113', '95116', '95117', 
                          '95118', '95119', '95120', '95121', '95122', '95123', '95124', '95125', 
                          '95126', '95127', '95128', '95129', '95130', '95131', '95132', '95133', 
                          '95134', '95135', '95136', '95138', '95139', '95148'])
    
    # Peninsula (San Mateo, etc.)
    bay_area_zips.extend(['94002', '94005', '94010', '94014', '94015', '94025', '94027', '94028', 
                          '94030', '94044', '94061', '94062', '94063', '94065', '94066', '94070', 
                          '94080', '94401', '94402', '94403', '94404'])
    
    # North Bay (Marin, Sonoma)
    bay_area_zips.extend(['94901', '94903', '94904', '94920', '94925', '94930', '94939', '94941', 
                          '94945', '94947', '94949', '94952', '94954', '94965', '94973', '95401', 
                          '95403', '95404', '95405', '95407', '95409'])
    
    return bay_area_zips

def main():
    parser = argparse.ArgumentParser(description='Niche.com Zipcode Data Scraper')
    parser.add_argument('--zipcodes', nargs='+', help='List of zipcodes to scrape')
    parser.add_argument('--max-workers', type=int, default=3, help='Maximum number of parallel workers')
    parser.add_argument('--output', default='niche_data.csv', help='Output CSV filename')
    parser.add_argument('--cache-dir', default='niche_cache', help='Cache directory')
    parser.add_argument('--no-cache', action='store_true', help='Disable caching')
    parser.add_argument('--delay-min', type=float, default=3, help='Minimum delay between requests in seconds')
    parser.add_argument('--delay-max', type=float, default=7, help='Maximum delay between requests in seconds')
    parser.add_argument('--database', action='store_true', help='Save results to database')
    parser.add_argument('--bay-area', action='store_true', help='Scrape all Bay Area zipcodes')
    
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = NicheDataScraper(
        cache_dir=args.cache_dir,
        use_cache=not args.no_cache,
        delay_min=args.delay_min,
        delay_max=args.delay_max
    )
    
    # Determine which zipcodes to scrape
    if args.bay_area:
        zipcodes = get_bay_area_zipcodes()
    elif args.zipcodes:
        zipcodes = args.zipcodes
    else:
        # Default to a few sample zipcodes
        zipcodes = ['94110', '94117', '94103', '94122', '94105']
    
    logger.info(f"Starting scrape of {len(zipcodes)} zipcodes")
    
    # Scrape zipcodes
    results = scraper.scrape_zipcodes(zipcodes, max_workers=args.max_workers)
    
    # Save results
    scraper.save_to_csv(results, filename=args.output)
    
    # Save to database if requested
    if args.database and db_url:
        scraper.save_to_database(results)
    
    logger.info("Scraping complete")

if __name__ == "__main__":
    main()
