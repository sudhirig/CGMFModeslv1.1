#!/usr/bin/env python3
"""Quick summary of data collection progress"""
import os
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

db_url = os.getenv('DATABASE_URL')
parsed = urlparse(db_url)

conn = psycopg2.connect(
    host=parsed.hostname,
    port=parsed.port,
    database=parsed.path[1:],
    user=parsed.username,
    password=parsed.password,
    sslmode='require'
)

cursor = conn.cursor()

# Get counts
tables = {
    'aum_analytics': 'SELECT COUNT(DISTINCT fund_name) FROM aum_analytics',
    'category_performance': 'SELECT COUNT(*) FROM category_performance',
    'manager_analytics': 'SELECT COUNT(*) FROM manager_analytics',
    'portfolio_overlap': 'SELECT COUNT(*) FROM portfolio_overlap',
    'portfolio_holdings': 'SELECT COUNT(DISTINCT fund_id) FROM portfolio_holdings',
    'market_indices': 'SELECT COUNT(DISTINCT index_name) FROM market_indices'
}

print("📊 Data Collection Summary")
print("=" * 40)

for table, query in tables.items():
    cursor.execute(query)
    count = cursor.fetchone()[0]
    print(f"{table}: {count:,} records")

conn.close()
