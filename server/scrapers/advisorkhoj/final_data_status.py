#!/usr/bin/env python3
"""Check final data collection status"""
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

# Get total funds
cursor.execute("SELECT COUNT(*) FROM funds")
total_funds = cursor.fetchone()[0]

print("📊 Final Data Collection Status")
print("=" * 50)
print(f"Total Funds in Database: {total_funds:,}")
print()

# Check AUM coverage
cursor.execute("SELECT COUNT(DISTINCT fund_name) FROM aum_analytics")
funds_with_aum = cursor.fetchone()[0]
aum_percent = round(funds_with_aum / total_funds * 100, 1)

print(f"✅ AUM Data:")
print(f"   - Funds with AUM: {funds_with_aum:,} ({aum_percent}%)")
print(f"   - Remaining: {total_funds - funds_with_aum:,}")

# Check holdings coverage
cursor.execute("SELECT COUNT(DISTINCT fund_id) FROM portfolio_holdings")
funds_with_holdings = cursor.fetchone()[0]
holdings_percent = round(funds_with_holdings / total_funds * 100, 1)

cursor.execute("SELECT COUNT(*) FROM portfolio_holdings")
total_holdings = cursor.fetchone()[0]

print(f"\n✅ Portfolio Holdings:")
print(f"   - Funds with holdings: {funds_with_holdings:,} ({holdings_percent}%)")
print(f"   - Total holdings records: {total_holdings:,}")
print(f"   - Remaining: {total_funds - funds_with_holdings:,}")

# Check benchmark coverage
cursor.execute("SELECT COUNT(*) FROM funds WHERE benchmark_name IS NOT NULL AND benchmark_name != ''")
funds_with_benchmarks = cursor.fetchone()[0]
benchmark_percent = round(funds_with_benchmarks / total_funds * 100, 1)

cursor.execute("SELECT COUNT(DISTINCT index_name) FROM market_indices")
unique_benchmarks = cursor.fetchone()[0]

print(f"\n✅ Benchmark Data:")
print(f"   - Funds with benchmarks: {funds_with_benchmarks:,} ({benchmark_percent}%)")
print(f"   - Unique benchmarks: {unique_benchmarks:,}")
print(f"   - Remaining: {total_funds - funds_with_benchmarks:,}")

# Other data
cursor.execute("SELECT COUNT(*) FROM category_performance")
category_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM manager_analytics")
manager_count = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM portfolio_overlap")
overlap_count = cursor.fetchone()[0]

print(f"\n✅ Additional Data:")
print(f"   - Category performance records: {category_count:,}")
print(f"   - Manager analytics records: {manager_count:,}")
print(f"   - Portfolio overlap records: {overlap_count:,}")

# Overall completion
overall_completion = round((aum_percent + holdings_percent + benchmark_percent) / 3, 1)
print(f"\n📈 Overall Data Completion: {overall_completion}%")

if overall_completion < 100:
    print(f"\n⚠️  Data collection still in progress...")
    print(f"    Continue running collectors to reach 100% coverage")
else:
    print(f"\n✅ Data collection COMPLETE!")
    
conn.close()
