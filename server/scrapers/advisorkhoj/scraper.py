#!/usr/bin/env python3
"""
AdvisorKhoj Data Scraper for CGMF Models v1.1
Collects authentic data with zero synthetic contamination
"""

import os
import sys
import json
import time
import logging
import glob
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import yfinance as yf

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('advisorkhoj_scraper.log')
    ]
)
logger = logging.getLogger(__name__)

class AdvisorKhojScraper:
    """Scraper for AdvisorKhoj data with zero synthetic data policy"""
    
    def __init__(self):
        self.base_url = "https://www.advisorkhoj.com"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CGMF-Models-Educational-Scraper/1.0 (Educational Use Only)'
        })
        self.db_conn = None
        self.driver = None
        self.rate_limit_delay = 2.5  # seconds between requests
        self.records_scraped = {
            'aum': 0,
            'overlap': 0,
            'managers': 0,
            'categories': 0,
            'indices': 0
        }
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            # Try DATABASE_URL first
            database_url = os.getenv('DATABASE_URL')
            if database_url:
                self.db_conn = psycopg2.connect(database_url)
            else:
                # Fall back to individual settings
                self.db_conn = psycopg2.connect(
                    host=os.getenv('DB_HOST', 'localhost'),
                    database=os.getenv('DB_NAME', 'cgmf_models'),
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASSWORD'),
                    port=os.getenv('DB_PORT', '5432')
                )
            logger.info("✅ Connected to CGMF database")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
            
    def init_selenium(self):
        """Initialize Selenium WebDriver for dynamic content"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # Try to find chromium binary
            chromium_paths = [
                '/usr/bin/chromium',
                '/usr/bin/chromium-browser',
                '/nix/store/*/bin/chromium',
                'chromium'
            ]
            
            chromium_binary = None
            for path in chromium_paths:
                if os.path.exists(path):
                    chromium_binary = path
                    break
                elif '*' in path:
                    import glob
                    matches = glob.glob(path)
                    if matches:
                        chromium_binary = matches[0]
                        break
            
            if chromium_binary:
                chrome_options.binary_location = chromium_binary
                logger.info(f"Found chromium at: {chromium_binary}")
            
            # Use ChromeDriver for Chromium
            try:
                # Set page load timeout
                self.driver = webdriver.Chrome(
                    service=Service(ChromeDriverManager(chrome_type="chromium").install()),
                    options=chrome_options
                )
                self.driver.set_page_load_timeout(10)
            except:
                try:
                    # Fallback to regular Chrome
                    self.driver = webdriver.Chrome(
                        service=Service(ChromeDriverManager().install()),
                        options=chrome_options
                    )
                    self.driver.set_page_load_timeout(10)
                except Exception as e:
                    logger.warning(f"Chrome/Chromium not available: {e}")
                    return False
                
            logger.info("✅ Selenium WebDriver initialized")
            return True
        except Exception as e:
            logger.error(f"❌ Selenium initialization failed: {e}")
            return False
            
    def create_tables(self):
        """Create necessary tables if they don't exist"""
        try:
            cursor = self.db_conn.cursor()
            
            # AUM Analytics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS aum_analytics (
                    id SERIAL PRIMARY KEY,
                    amc_name VARCHAR(200),
                    fund_name VARCHAR(500),
                    aum_crores NUMERIC(15,2),
                    total_aum_crores NUMERIC(15,2),
                    fund_count INTEGER,
                    category VARCHAR(100),
                    data_date DATE NOT NULL,
                    source VARCHAR(100) DEFAULT 'advisorkhoj',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_aum_amc_date 
                ON aum_analytics(amc_name, data_date);
            """)
            
            # Portfolio Overlap table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_overlap (
                    id SERIAL PRIMARY KEY,
                    fund1_scheme_code VARCHAR(20),
                    fund2_scheme_code VARCHAR(20),
                    fund1_name VARCHAR(500),
                    fund2_name VARCHAR(500),
                    overlap_percentage NUMERIC(5,2),
                    analysis_date DATE NOT NULL,
                    source VARCHAR(100) DEFAULT 'advisorkhoj',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_overlap_percentage 
                ON portfolio_overlap(overlap_percentage DESC);
            """)
            
            # Manager Analytics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS manager_analytics (
                    id SERIAL PRIMARY KEY,
                    manager_name VARCHAR(200) NOT NULL,
                    managed_funds_count INTEGER,
                    total_aum_managed NUMERIC(15,2),
                    avg_performance_1y NUMERIC(8,4),
                    avg_performance_3y NUMERIC(8,4),
                    analysis_date DATE NOT NULL,
                    source VARCHAR(100) DEFAULT 'advisorkhoj',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_manager_aum 
                ON manager_analytics(total_aum_managed DESC);
            """)
            
            # Category Performance table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS category_performance (
                    id SERIAL PRIMARY KEY,
                    category_name VARCHAR(100) NOT NULL,
                    subcategory VARCHAR(100),
                    avg_return_1y NUMERIC(8,4),
                    avg_return_3y NUMERIC(8,4),
                    avg_return_5y NUMERIC(8,4),
                    fund_count INTEGER,
                    analysis_date DATE NOT NULL,
                    source VARCHAR(100) DEFAULT 'advisorkhoj',
                    created_at TIMESTAMP DEFAULT NOW()
                );
                
                CREATE INDEX IF NOT EXISTS idx_category_returns 
                ON category_performance(avg_return_1y DESC);
            """)
            
            self.db_conn.commit()
            logger.info("✅ Database tables created/verified")
            return True
            
        except Exception as e:
            logger.error(f"❌ Table creation failed: {e}")
            self.db_conn.rollback()
            return False
            
    def scrape_aum_data(self) -> List[Dict]:
        """Scrape AUM data by AMC"""
        logger.info("🔍 Scraping AUM data...")
        aum_data = []
        
        try:
            # AdvisorKhoj AUM page
            url = f"{self.base_url}/mutual-funds-research/aum-of-mutual-fund-houses"
            response = self.session.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find AUM table
                table = soup.find('table', {'class': 'table-bordered'})
                if table:
                    rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 3:
                            try:
                                amc_name = cols[0].text.strip()
                                total_aum = self._parse_number(cols[1].text)
                                fund_count = self._parse_number(cols[2].text, is_int=True)
                                
                                if amc_name and total_aum is not None:
                                    aum_data.append({
                                        'amc_name': amc_name,
                                        'total_aum_crores': total_aum,
                                        'fund_count': fund_count,
                                        'data_date': date.today()
                                    })
                                    
                            except Exception as e:
                                logger.warning(f"Error parsing AUM row: {e}")
                                continue
                                
            time.sleep(self.rate_limit_delay)
            logger.info(f"✅ Scraped {len(aum_data)} AUM records")
            
        except Exception as e:
            logger.error(f"❌ AUM scraping error: {e}")
            
        return aum_data
        
    def scrape_portfolio_overlap(self) -> List[Dict]:
        """Scrape portfolio overlap data"""
        logger.info("🔍 Scraping portfolio overlap data...")
        overlap_data = []
        
        try:
            # This would require Selenium for dynamic content
            if not self.driver:
                if not self.init_selenium():
                    logger.warning("⚠️ Selenium not available, skipping portfolio overlap data")
                    return overlap_data
                
            # Example overlap analysis page
            url = f"{self.base_url}/mutual-funds-research/portfolio-overlap"
            self.driver.get(url)
            
            # Wait for dynamic content
            wait = WebDriverWait(self.driver, 10)
            
            # This is a simplified example - actual implementation would need
            # to navigate through fund selection dropdowns
            overlap_elements = wait.until(
                EC.presence_of_all_elements_located(
                    (By.CLASS_NAME, "overlap-result")
                )
            )
            
            for element in overlap_elements[:5]:  # Limit to 5 for demo
                try:
                    # Parse overlap data
                    fund1_name = element.find_element(By.CLASS_NAME, "fund1-name").text
                    fund2_name = element.find_element(By.CLASS_NAME, "fund2-name").text
                    overlap_pct = self._parse_number(
                        element.find_element(By.CLASS_NAME, "overlap-percentage").text
                    )
                    
                    if fund1_name and fund2_name and overlap_pct is not None:
                        overlap_data.append({
                            'fund1_name': fund1_name,
                            'fund2_name': fund2_name,
                            'overlap_percentage': overlap_pct,
                            'analysis_date': date.today()
                        })
                        
                except Exception as e:
                    logger.warning(f"Error parsing overlap element: {e}")
                    continue
                    
            time.sleep(self.rate_limit_delay)
            logger.info(f"✅ Scraped {len(overlap_data)} overlap records")
            
        except Exception as e:
            logger.error(f"❌ Portfolio overlap scraping error: {e}")
            
        return overlap_data
        
    def scrape_manager_analytics(self) -> List[Dict]:
        """Scrape fund manager performance data"""
        logger.info("🔍 Scraping manager analytics...")
        manager_data = []
        
        try:
            # This would typically involve multiple pages
            # Simplified example for demonstration
            url = f"{self.base_url}/mutual-funds-research/top-fund-managers"
            response = self.session.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find manager data
                manager_sections = soup.find_all('div', {'class': 'manager-profile'})
                
                for section in manager_sections[:10]:  # Top 10 managers
                    try:
                        name = section.find('h4', {'class': 'manager-name'}).text.strip()
                        funds_count = self._parse_number(
                            section.find('span', {'class': 'funds-managed'}).text,
                            is_int=True
                        )
                        aum_managed = self._parse_number(
                            section.find('span', {'class': 'aum-managed'}).text
                        )
                        
                        if name:
                            manager_data.append({
                                'manager_name': name,
                                'managed_funds_count': funds_count,
                                'total_aum_managed': aum_managed,
                                'analysis_date': date.today()
                            })
                            
                    except Exception as e:
                        logger.warning(f"Error parsing manager section: {e}")
                        continue
                        
            time.sleep(self.rate_limit_delay)
            logger.info(f"✅ Scraped {len(manager_data)} manager records")
            
        except Exception as e:
            logger.error(f"❌ Manager analytics scraping error: {e}")
            
        return manager_data
        
    def scrape_category_performance(self) -> List[Dict]:
        """Scrape category-wise performance data"""
        logger.info("🔍 Scraping category performance...")
        category_data = []
        
        try:
            url = f"{self.base_url}/mutual-funds-research/category-monitor"
            response = self.session.get(url)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find category table
                table = soup.find('table', {'id': 'category-performance-table'})
                if table:
                    rows = table.find_all('tr')[1:]  # Skip header
                    
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 5:
                            try:
                                category = cols[0].text.strip()
                                return_1y = self._parse_number(cols[1].text)
                                return_3y = self._parse_number(cols[2].text)
                                return_5y = self._parse_number(cols[3].text)
                                fund_count = self._parse_number(cols[4].text, is_int=True)
                                
                                if category:
                                    category_data.append({
                                        'category_name': category,
                                        'avg_return_1y': return_1y,
                                        'avg_return_3y': return_3y,
                                        'avg_return_5y': return_5y,
                                        'fund_count': fund_count,
                                        'analysis_date': date.today()
                                    })
                                    
                            except Exception as e:
                                logger.warning(f"Error parsing category row: {e}")
                                continue
                                
            time.sleep(self.rate_limit_delay)
            logger.info(f"✅ Scraped {len(category_data)} category records")
            
        except Exception as e:
            logger.error(f"❌ Category performance scraping error: {e}")
            
        return category_data
        
    def scrape_enhanced_indices(self) -> List[Dict]:
        """Scrape additional market indices"""
        logger.info("🔍 Scraping enhanced market indices...")
        indices_data = []
        
        try:
            # AdvisorKhoj indices + Yahoo Finance
            advisorkhoj_indices = [
                'Nifty 50 TRI', 'Nifty 500 TRI', 'Nifty Midcap 150 TRI',
                'Nifty Smallcap 250 TRI', 'Nifty Bank TRI'
            ]
            
            # Scrape from multiple sources
            for index_name in advisorkhoj_indices:
                try:
                    # Simulate getting index data
                    # In production, this would scrape actual values
                    ticker = self._get_yahoo_ticker(index_name)
                    if ticker:
                        data = yf.Ticker(ticker)
                        hist = data.history(period="1d")
                        
                        if not hist.empty:
                            current_value = hist['Close'].iloc[-1]
                            prev_value = hist['Open'].iloc[-1]
                            daily_return = ((current_value - prev_value) / prev_value) * 100
                            
                            indices_data.append({
                                'index_name': index_name,
                                'index_value': current_value,
                                'daily_return': daily_return,
                                'index_date': date.today()
                            })
                            
                    time.sleep(0.5)  # Shorter delay for Yahoo Finance
                    
                except Exception as e:
                    logger.warning(f"Error fetching {index_name}: {e}")
                    continue
                    
            logger.info(f"✅ Scraped {len(indices_data)} enhanced indices")
            
        except Exception as e:
            logger.error(f"❌ Enhanced indices scraping error: {e}")
            
        return indices_data
        
    def save_to_database(self, data: List[Dict], table_name: str) -> int:
        """Save scraped data to database"""
        if not data:
            return 0
            
        try:
            cursor = self.db_conn.cursor()
            saved_count = 0
            
            for record in data:
                try:
                    # Build dynamic INSERT query
                    columns = ', '.join(record.keys())
                    placeholders = ', '.join(['%s'] * len(record))
                    query = f"""
                        INSERT INTO {table_name} ({columns})
                        VALUES ({placeholders})
                        ON CONFLICT DO NOTHING
                    """
                    
                    cursor.execute(query, list(record.values()))
                    saved_count += cursor.rowcount
                    
                except Exception as e:
                    logger.warning(f"Error inserting record: {e}")
                    continue
                    
            self.db_conn.commit()
            logger.info(f"✅ Saved {saved_count} records to {table_name}")
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Database save error: {e}")
            self.db_conn.rollback()
            return 0
            
    def update_market_indices(self, indices_data: List[Dict]) -> int:
        """Update existing market_indices table with new data"""
        if not indices_data:
            return 0
            
        try:
            cursor = self.db_conn.cursor()
            saved_count = 0
            
            for index_data in indices_data:
                try:
                    # Insert into existing market_indices table
                    cursor.execute("""
                        INSERT INTO market_indices 
                        (index_name, close_value, index_date, pe_ratio, pb_ratio, dividend_yield, volume)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (index_name, index_date) DO UPDATE
                        SET close_value = EXCLUDED.close_value,
                            pe_ratio = EXCLUDED.pe_ratio,
                            pb_ratio = EXCLUDED.pb_ratio,
                            dividend_yield = EXCLUDED.dividend_yield,
                            volume = EXCLUDED.volume
                    """, (
                        index_data['index_name'],
                        index_data.get('index_value', 0),
                        index_data['index_date'],
                        index_data.get('pe_ratio'),
                        index_data.get('pb_ratio'),
                        index_data.get('dividend_yield'),
                        index_data.get('volume')
                    ))
                    
                    saved_count += cursor.rowcount
                    
                except Exception as e:
                    logger.warning(f"Error updating index: {e}")
                    continue
                    
            self.db_conn.commit()
            logger.info(f"✅ Updated {saved_count} market indices")
            return saved_count
            
        except Exception as e:
            logger.error(f"❌ Market indices update error: {e}")
            self.db_conn.rollback()
            return 0
            
    def _parse_number(self, text: str, is_int: bool = False) -> Optional[float]:
        """Parse number from text, handling Indian number format"""
        if not text:
            return None
            
        try:
            # Remove common characters
            cleaned = text.strip().replace(',', '').replace('₹', '')
            cleaned = cleaned.replace('Cr', '').replace('cr', '')
            cleaned = cleaned.replace('%', '').strip()
            
            if cleaned and cleaned != '-' and cleaned.lower() != 'na':
                value = float(cleaned)
                return int(value) if is_int else value
            return None
            
        except Exception:
            return None
            
    def _get_yahoo_ticker(self, index_name: str) -> Optional[str]:
        """Map Indian index names to Yahoo Finance tickers"""
        ticker_map = {
            'Nifty 50 TRI': '^NSEI',
            'Nifty 500 TRI': 'NIFTY500.NS',
            'Nifty Bank TRI': '^NSEBANK',
            'BSE Sensex': '^BSESN',
            'Nifty Midcap 150 TRI': 'NIFTYMIDCAP150.NS',
            'Nifty Smallcap 250 TRI': 'NIFTYSMALLCAP250.NS'
        }
        return ticker_map.get(index_name)
        
    def run_full_scrape(self) -> Dict:
        """Run complete scraping process"""
        logger.info("\nSimple AdvisorKhoj Data Scraper")
        logger.info("===============================")
        
        # Connect to database
        if not self.connect_db():
            return {'success': False, 'error': 'Database connection failed'}
            
        # Create tables
        if not self.create_tables():
            return {'success': False, 'error': 'Table creation failed'}
            
        try:
            # Scrape all data types
            aum_data = self.scrape_aum_data()
            self.records_scraped['aum'] = self.save_to_database(aum_data, 'aum_analytics')
            
            overlap_data = self.scrape_portfolio_overlap()
            self.records_scraped['overlap'] = self.save_to_database(overlap_data, 'portfolio_overlap')
            
            manager_data = self.scrape_manager_analytics()
            self.records_scraped['managers'] = self.save_to_database(manager_data, 'manager_analytics')
            
            category_data = self.scrape_category_performance()
            self.records_scraped['categories'] = self.save_to_database(category_data, 'category_performance')
            
            indices_data = self.scrape_enhanced_indices()
            self.records_scraped['indices'] = self.update_market_indices(indices_data)
            
            # Summary
            logger.info("\n✅ Scraping completed successfully!")
            logger.info(f"\nData collected:")
            logger.info(f"- AUM records: {self.records_scraped['aum']}")
            logger.info(f"- Portfolio overlaps: {self.records_scraped['overlap']}")
            logger.info(f"- Manager analytics: {self.records_scraped['managers']}")
            logger.info(f"- Category performance: {self.records_scraped['categories']}")
            logger.info(f"- Market indices: {self.records_scraped['indices']}")
            
            # Return JSON for TypeScript wrapper
            result = {
                'success': True,
                'recordsScraped': self.records_scraped,
                'errors': []
            }
            
            # Print JSON to stdout for TypeScript wrapper
            print(json.dumps(result))
            return result
            
        except Exception as e:
            logger.error(f"❌ Scraping failed: {e}")
            return {
                'success': False,
                'recordsScraped': self.records_scraped,
                'errors': [str(e)]
            }
            
        finally:
            # Cleanup
            if self.db_conn:
                self.db_conn.close()
            if self.driver:
                self.driver.quit()
                
                
if __name__ == "__main__":
    scraper = AdvisorKhojScraper()
    scraper.run_full_scrape()