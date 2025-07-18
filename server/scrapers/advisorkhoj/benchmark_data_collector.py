#!/usr/bin/env python3
"""
Benchmark Data Collector
Collects and assigns benchmark indices to all mutual funds
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
import requests
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv
import yfinance as yf
import random

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BenchmarkDataCollector:
    """Collector for benchmark data and fund-benchmark mappings"""
    
    def __init__(self):
        self.db_conn = None
        self.session = requests.Session()
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                logger.error("DATABASE_URL not found")
                return False
                
            parsed = urlparse(db_url)
            
            self.db_conn = psycopg2.connect(
                host=parsed.hostname,
                port=parsed.port,
                database=parsed.path[1:],
                user=parsed.username,
                password=parsed.password,
                sslmode='require'
            )
            
            logger.info("✅ Connected to database")
            return True
            
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
            
    def collect_all_benchmarks(self):
        """Collect comprehensive benchmark data from Yahoo Finance"""
        logger.info("📈 Collecting all benchmark indices...")
        
        # Comprehensive list of Indian market indices
        benchmarks = {
            # Main indices
            'NIFTY 50': '^NSEI',
            'SENSEX': '^BSESN',
            'NIFTY NEXT 50': '^NIFTYJR',
            'NIFTY 100': '^CNX100',
            'NIFTY 200': '^CNX200',
            'NIFTY 500': '^CNX500',
            'BSE 100': 'BSE-100.BO',
            'BSE 200': 'BSE-200.BO',
            'BSE 500': 'BSE-500.BO',
            
            # Sector indices
            'NIFTY BANK': '^NSEBANK',
            'NIFTY IT': '^CNXIT',
            'NIFTY PHARMA': '^CNXPHARMA',
            'NIFTY AUTO': '^CNXAUTO',
            'NIFTY FMCG': '^CNXFMCG',
            'NIFTY METAL': '^CNXMETAL',
            'NIFTY REALTY': '^CNXREALTY',
            'NIFTY ENERGY': '^CNXENERGY',
            'NIFTY INFRA': '^CNXINFRA',
            'NIFTY HEALTHCARE': '^CNXHEALTH',
            'NIFTY MEDIA': '^CNXMEDIA',
            'NIFTY FINANCIAL SERVICES': '^CNXFINANCE',
            'NIFTY COMMODITIES': '^CNXCOMMODITY',
            'NIFTY CONSUMER DURABLES': '^CNXCONSUMDUR',
            'NIFTY OIL & GAS': '^CNXOILGAS',
            'NIFTY PSU BANK': '^CNXPSUBANK',
            'NIFTY PRIVATE BANK': '^CNXPVTBANK',
            'NIFTY SERVICES': '^CNXSERVICE',
            
            # Cap-based indices
            'NIFTY MIDCAP 50': '^NSEMDCP50',
            'NIFTY MIDCAP 100': '^NSEMDCP100',
            'NIFTY MIDCAP 150': '^NSEMDCP150',
            'NIFTY SMALLCAP 50': '^NSESMCP50',
            'NIFTY SMALLCAP 100': '^NSESMCP100',
            'NIFTY SMALLCAP 250': '^NSESMCP250',
            'NIFTY LARGECAP 100': '^CNXLARGECAP',
            'NIFTY MIDSMALLCAP 400': '^CNXMIDSMALL',
            
            # Strategy indices
            'NIFTY ALPHA 50': '^CNXALPHA50',
            'NIFTY HIGH BETA 50': '^CNXHIGHBETA',
            'NIFTY LOW VOLATILITY 50': '^CNXLOWVOL50',
            'NIFTY DIVIDEND OPPORTUNITIES 50': '^CNXDIVIDEND',
            'NIFTY GROWTH SECTORS 15': '^CNXGROWTH',
            'NIFTY VALUE 20': '^CNXVALUE20',
            'NIFTY QUALITY 30': '^CNXQUALITY30',
            
            # Thematic indices
            'NIFTY CPSE': '^CNXCPSE',
            'NIFTY INFRASTRUCTURE': '^CNXINFRA',
            'NIFTY MNC': '^CNXMNC',
            'NIFTY PSE': '^CNXPSE',
            'NIFTY SME EMERGE': '^CNXSMEMERGE',
            
            # Debt indices
            'NIFTY 10 YR BENCHMARK G-SEC': '^INBMKGSP10Y',
            'NIFTY 5 YR BENCHMARK G-SEC': '^INBMKGSP5Y',
            'NIFTY AAA CORPORATE BOND': '^INBMKAAA',
            'NIFTY COMPOSITE DEBT': '^INBMKCOMPDEBT',
            
            # International
            'NASDAQ 100': '^NDX',
            'S&P 500': '^GSPC',
            'MSCI EMERGING MARKETS': 'EEM',
            'HANG SENG': '^HSI'
        }
        
        cursor = self.db_conn.cursor()
        count = 0
        
        for index_name, ticker in benchmarks.items():
            try:
                logger.info(f"Fetching {index_name}...")
                stock = yf.Ticker(ticker)
                
                # Get historical data
                hist = stock.history(period="3mo")
                
                if not hist.empty:
                    batch_data = []
                    for idx, row in hist.iterrows():
                        batch_data.append((
                            index_name,
                            float(row['Close']),
                            float(row['Open']),
                            float(row['High']),
                            float(row['Low']),
                            int(row.get('Volume', 0)) if row.get('Volume') else 0,
                            None, None, None,  # PE, PB, Dividend Yield
                            idx.date()
                        ))
                    
                    # Insert data
                    cursor.executemany("""
                        INSERT INTO market_indices 
                        (index_name, close_value, open_value, high_value, low_value, 
                         volume, pe_ratio, pb_ratio, dividend_yield, index_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (index_name, index_date) DO UPDATE
                        SET close_value = EXCLUDED.close_value,
                            open_value = EXCLUDED.open_value,
                            high_value = EXCLUDED.high_value,
                            low_value = EXCLUDED.low_value,
                            volume = EXCLUDED.volume
                    """, batch_data)
                    
                    count += cursor.rowcount
                    self.db_conn.commit()
                    logger.info(f"✅ Added {len(batch_data)} records for {index_name}")
                    
            except Exception as e:
                logger.warning(f"Failed to get {index_name}: {e}")
                continue
                
            time.sleep(0.5)  # Rate limiting
        
        logger.info(f"✅ Collected {count} benchmark records")
        return count
        
    def assign_benchmarks_to_funds(self):
        """Assign appropriate benchmarks to all funds"""
        logger.info("🎯 Assigning benchmarks to all funds...")
        cursor = self.db_conn.cursor()
        
        # Get funds without benchmarks
        cursor.execute("""
            SELECT id, fund_name, category, subcategory, benchmark_name
            FROM funds
            WHERE benchmark_name IS NULL OR benchmark_name = ''
            ORDER BY id
        """)
        
        funds = cursor.fetchall()
        logger.info(f"Found {len(funds)} funds without benchmarks")
        
        # Benchmark mapping rules
        benchmark_rules = {
            # Equity funds
            ('Equity', 'Large Cap'): 'NIFTY 50',
            ('Equity', 'Large & Mid Cap'): 'NIFTY LARGECAP 100',
            ('Equity', 'Multi Cap'): 'NIFTY 500',
            ('Equity', 'Flexi Cap'): 'NIFTY 500',
            ('Equity', 'Mid Cap'): 'NIFTY MIDCAP 100',
            ('Equity', 'Small Cap'): 'NIFTY SMALLCAP 100',
            ('Equity', 'Value'): 'NIFTY VALUE 20',
            ('Equity', 'Dividend Yield'): 'NIFTY DIVIDEND OPPORTUNITIES 50',
            ('Equity', 'ELSS'): 'NIFTY 500',
            ('Equity', 'Sectoral/Thematic'): 'NIFTY 500',
            ('Equity', 'Index'): 'NIFTY 50',
            ('Equity', 'Focused'): 'NIFTY 50',
            ('Equity', 'Contra'): 'NIFTY 500',
            
            # Sector specific
            ('Equity', 'Banking'): 'NIFTY BANK',
            ('Equity', 'IT'): 'NIFTY IT',
            ('Equity', 'Pharma'): 'NIFTY PHARMA',
            ('Equity', 'Infrastructure'): 'NIFTY INFRASTRUCTURE',
            ('Equity', 'FMCG'): 'NIFTY FMCG',
            ('Equity', 'Healthcare'): 'NIFTY HEALTHCARE',
            ('Equity', 'Financial Services'): 'NIFTY FINANCIAL SERVICES',
            
            # Debt funds
            ('Debt', 'Liquid'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Ultra Short Duration'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Low Duration'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Short Duration'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Medium Duration'): 'NIFTY COMPOSITE DEBT',
            ('Debt', 'Long Duration'): 'NIFTY 10 YR BENCHMARK G-SEC',
            ('Debt', 'Gilt'): 'NIFTY 10 YR BENCHMARK G-SEC',
            ('Debt', 'Corporate Bond'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Banking & PSU'): 'NIFTY AAA CORPORATE BOND',
            ('Debt', 'Credit Risk'): 'NIFTY COMPOSITE DEBT',
            
            # Hybrid funds
            ('Hybrid', 'Aggressive Hybrid'): 'NIFTY 50',
            ('Hybrid', 'Conservative Hybrid'): 'NIFTY AAA CORPORATE BOND',
            ('Hybrid', 'Balanced Hybrid'): 'NIFTY 50',
            ('Hybrid', 'Dynamic Asset Allocation'): 'NIFTY 50',
            ('Hybrid', 'Equity Savings'): 'NIFTY 50',
            ('Hybrid', 'Arbitrage'): 'NIFTY 50',
            
            # International
            ('Equity', 'International'): 'NASDAQ 100',
            ('Equity', 'Global'): 'S&P 500'
        }
        
        # Default benchmarks by category
        default_benchmarks = {
            'Equity': 'NIFTY 500',
            'Debt': 'NIFTY COMPOSITE DEBT',
            'Hybrid': 'NIFTY 50',
            'Solution Oriented': 'NIFTY 500',
            'Other': 'NIFTY 50'
        }
        
        batch_updates = []
        
        for fund_id, fund_name, category, subcategory, current_benchmark in funds:
            # Determine appropriate benchmark
            benchmark = None
            
            # First try category + subcategory combination
            if category and subcategory:
                benchmark = benchmark_rules.get((category, subcategory))
            
            # Check if fund name contains sector keywords
            if not benchmark and fund_name:
                fund_name_lower = fund_name.lower()
                if 'banking' in fund_name_lower or 'bank' in fund_name_lower:
                    benchmark = 'NIFTY BANK'
                elif 'it' in fund_name_lower or 'technology' in fund_name_lower:
                    benchmark = 'NIFTY IT'
                elif 'pharma' in fund_name_lower:
                    benchmark = 'NIFTY PHARMA'
                elif 'infrastructure' in fund_name_lower or 'infra' in fund_name_lower:
                    benchmark = 'NIFTY INFRASTRUCTURE'
                elif 'fmcg' in fund_name_lower or 'consumer' in fund_name_lower:
                    benchmark = 'NIFTY FMCG'
                elif 'midcap' in fund_name_lower:
                    benchmark = 'NIFTY MIDCAP 100'
                elif 'smallcap' in fund_name_lower or 'small cap' in fund_name_lower:
                    benchmark = 'NIFTY SMALLCAP 100'
                elif 'largecap' in fund_name_lower or 'large cap' in fund_name_lower:
                    benchmark = 'NIFTY 50'
            
            # Fall back to default by category
            if not benchmark:
                benchmark = default_benchmarks.get(category, 'NIFTY 50')
            
            batch_updates.append((benchmark, fund_id))
            
            # Update in batches
            if len(batch_updates) >= 500:
                cursor.executemany("""
                    UPDATE funds
                    SET benchmark_name = %s
                    WHERE id = %s
                """, batch_updates)
                self.db_conn.commit()
                logger.info(f"Updated {len(batch_updates)} fund benchmarks")
                batch_updates = []
        
        # Update remaining
        if batch_updates:
            cursor.executemany("""
                UPDATE funds
                SET benchmark_name = %s
                WHERE id = %s
            """, batch_updates)
            self.db_conn.commit()
            logger.info(f"Updated {len(batch_updates)} fund benchmarks")
        
        logger.info("✅ Assigned benchmarks to all funds")
        return len(funds)
        
    def run(self):
        """Run the benchmark data collector"""
        logger.info("\n📈 Benchmark Data Collector Started")
        logger.info("===================================")
        
        # Connect to database
        if not self.connect_db():
            return {'success': False, 'error': 'Database connection failed'}
            
        try:
            results = {
                'benchmark_records': 0,
                'funds_updated': 0
            }
            
            # 1. Collect all benchmark data
            logger.info("\n📊 Phase 1: Collecting Benchmark Data")
            results['benchmark_records'] = self.collect_all_benchmarks()
            
            # 2. Assign benchmarks to all funds
            logger.info("\n🎯 Phase 2: Assigning Benchmarks to Funds")
            results['funds_updated'] = self.assign_benchmarks_to_funds()
            
            # Get final stats
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT COUNT(DISTINCT index_name) FROM market_indices")
            unique_benchmarks = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM funds WHERE benchmark_name IS NOT NULL")
            funds_with_benchmarks = cursor.fetchone()[0]
            
            # Summary
            logger.info("\n✅ Benchmark data collection completed!")
            logger.info(f"\nResults:")
            logger.info(f"- Unique benchmarks: {unique_benchmarks}")
            logger.info(f"- Benchmark records: {results['benchmark_records']}")
            logger.info(f"- Funds with benchmarks: {funds_with_benchmarks}")
            
            # Print JSON result
            result = {
                'success': True,
                'results': results,
                'stats': {
                    'unique_benchmarks': unique_benchmarks,
                    'funds_with_benchmarks': funds_with_benchmarks
                },
                'message': 'Successfully collected benchmark data and assigned to all funds'
            }
            print(json.dumps(result))
            return result
            
        except Exception as e:
            logger.error(f"❌ Benchmark collector failed: {e}")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
            
        finally:
            if self.db_conn:
                self.db_conn.close()
                

if __name__ == "__main__":
    collector = BenchmarkDataCollector()
    collector.run()