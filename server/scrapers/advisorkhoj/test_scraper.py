#!/usr/bin/env python3
"""
Test scraper to verify database connections and insert sample data
"""

import os
import sys
import json
import psycopg2
from datetime import date, datetime
from urllib.parse import urlparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TestAdvisorKhojScraper:
    def __init__(self):
        self.db_conn = None
        
    def connect_db(self):
        """Connect to PostgreSQL database"""
        try:
            # Parse DATABASE_URL
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                print("❌ DATABASE_URL not found in environment")
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
            
            print("✅ Connected to CGMF database")
            return True
            
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return False
            
    def test_tables(self):
        """Test inserting sample data into tables"""
        try:
            cursor = self.db_conn.cursor()
            
            # Test AUM Analytics table
            print("\n📊 Testing AUM Analytics table...")
            cursor.execute("""
                INSERT INTO aum_analytics 
                (amc_name, fund_name, aum_crores, total_aum_crores, fund_count, category, data_date)
                VALUES 
                ('HDFC Mutual Fund', 'HDFC Top 100 Fund', 25000.50, 450000.00, 150, 'Equity', %s),
                ('ICICI Prudential', 'ICICI Pru Value Discovery', 18000.75, 380000.00, 125, 'Equity', %s),
                ('SBI Mutual Fund', 'SBI Blue Chip Fund', 22000.25, 420000.00, 140, 'Equity', %s)
                ON CONFLICT DO NOTHING
            """, (date.today(), date.today(), date.today()))
            
            aum_count = cursor.rowcount
            print(f"✅ Inserted {aum_count} AUM records")
            
            # Test Manager Analytics table
            print("\n👤 Testing Manager Analytics table...")
            cursor.execute("""
                INSERT INTO manager_analytics 
                (manager_name, managed_funds_count, total_aum_managed, avg_performance_1y, avg_performance_3y, analysis_date)
                VALUES 
                ('Prashant Jain', 5, 85000.00, 12.5, 15.8, %s),
                ('R. Srinivasan', 4, 65000.00, 11.2, 14.5, %s),
                ('Navneet Munot', 6, 92000.00, 13.8, 16.2, %s)
                ON CONFLICT DO NOTHING
            """, (date.today(), date.today(), date.today()))
            
            manager_count = cursor.rowcount
            print(f"✅ Inserted {manager_count} manager records")
            
            # Test Category Performance table
            print("\n📈 Testing Category Performance table...")
            cursor.execute("""
                INSERT INTO category_performance 
                (category_name, subcategory, avg_return_1y, avg_return_3y, avg_return_5y, fund_count, analysis_date)
                VALUES 
                ('Equity', 'Large Cap', 12.5, 14.2, 13.8, 45, %s),
                ('Equity', 'Mid Cap', 18.3, 16.5, 15.2, 38, %s),
                ('Debt', 'Corporate Bond', 7.2, 8.1, 7.9, 52, %s)
                ON CONFLICT DO NOTHING
            """, (date.today(), date.today(), date.today()))
            
            category_count = cursor.rowcount
            print(f"✅ Inserted {category_count} category records")
            
            # Test Market Indices update
            print("\n📊 Testing Market Indices update...")
            cursor.execute("""
                INSERT INTO market_indices 
                (index_name, close_value, index_date, pe_ratio, pb_ratio, dividend_yield, volume)
                VALUES 
                ('NIFTY IT', 32500.50, %s, 28.5, 5.2, 0.8, 125000000),
                ('NIFTY PHARMA', 14800.25, %s, 24.3, 4.1, 1.2, 85000000),
                ('NIFTY AUTO', 18200.75, %s, 22.8, 3.9, 1.5, 95000000)
                ON CONFLICT (index_name, index_date) DO UPDATE
                SET close_value = EXCLUDED.close_value,
                    pe_ratio = EXCLUDED.pe_ratio,
                    pb_ratio = EXCLUDED.pb_ratio
            """, (date.today(), date.today(), date.today()))
            
            index_count = cursor.rowcount
            print(f"✅ Updated {index_count} market indices")
            
            # Commit all changes
            self.db_conn.commit()
            
            # Verify data
            print("\n🔍 Verifying inserted data...")
            
            cursor.execute("SELECT COUNT(*) FROM aum_analytics")
            print(f"Total AUM records: {cursor.fetchone()[0]}")
            
            cursor.execute("SELECT COUNT(*) FROM manager_analytics")
            print(f"Total manager records: {cursor.fetchone()[0]}")
            
            cursor.execute("SELECT COUNT(*) FROM category_performance")
            print(f"Total category records: {cursor.fetchone()[0]}")
            
            cursor.execute("SELECT COUNT(*) FROM market_indices WHERE index_name IN ('NIFTY IT', 'NIFTY PHARMA', 'NIFTY AUTO')")
            print(f"Updated index records: {cursor.fetchone()[0]}")
            
            print("\n✅ All tests passed! Database tables are working correctly.")
            
            return {
                'success': True,
                'records': {
                    'aum': aum_count,
                    'managers': manager_count,
                    'categories': category_count,
                    'indices': index_count
                }
            }
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            self.db_conn.rollback()
            return {'success': False, 'error': str(e)}
            
        finally:
            if self.db_conn:
                self.db_conn.close()
                

if __name__ == "__main__":
    print("🧪 AdvisorKhoj Database Test")
    print("============================")
    
    tester = TestAdvisorKhojScraper()
    
    if tester.connect_db():
        result = tester.test_tables()
        print(json.dumps(result, indent=2))
    else:
        print("Failed to connect to database")