#!/usr/bin/env python3
"""
Database Integrity Check - Comprehensive validation
"""

import os
import psycopg2
from urllib.parse import urlparse
from dotenv import load_dotenv

load_dotenv()

# Database connection
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

print("\n🔍 DATABASE INTEGRITY CHECK")
print("=" * 60)

# 1. Check for duplicate holdings
print("\n1. Portfolio Holdings Integrity:")
cursor.execute("""
SELECT fund_id, stock_name, COUNT(*) as duplicates
FROM portfolio_holdings
GROUP BY fund_id, stock_name
HAVING COUNT(*) > 1
LIMIT 5
""")
duplicates = cursor.fetchall()
if duplicates:
    print("❌ Found duplicate holdings:")
    for fund_id, stock, count in duplicates:
        print(f"   Fund {fund_id}: {stock} appears {count} times")
else:
    print("✅ No duplicate holdings found")

# Check holding percentages
cursor.execute("""
SELECT fund_id, SUM(holding_percent) as total_percent
FROM portfolio_holdings
GROUP BY fund_id
HAVING SUM(holding_percent) < 95 OR SUM(holding_percent) > 105
LIMIT 5
""")
bad_percentages = cursor.fetchall()
if bad_percentages:
    print("❌ Holdings with incorrect total percentages:")
    for fund_id, total in bad_percentages:
        print(f"   Fund {fund_id}: {total}%")
else:
    print("✅ All holdings sum to ~100%")

# 2. Check AUM data integrity
print("\n2. AUM Data Integrity:")
cursor.execute("""
SELECT amc_name, fund_name, COUNT(*) as duplicates
FROM aum_analytics
GROUP BY amc_name, fund_name
HAVING COUNT(*) > 1
LIMIT 5
""")
aum_dupes = cursor.fetchall()
if aum_dupes:
    print("❌ Found duplicate AUM records:")
    for amc, fund, count in aum_dupes:
        print(f"   {amc} - {fund}: {count} records")
else:
    print("✅ No duplicate AUM records")

# Check for mismatched fund names
cursor.execute("""
SELECT COUNT(DISTINCT a.fund_name) 
FROM aum_analytics a
WHERE NOT EXISTS (
    SELECT 1 FROM funds f WHERE f.fund_name = a.fund_name
)
""")
orphan_aum = cursor.fetchone()[0]
if orphan_aum > 0:
    print(f"⚠️  {orphan_aum} AUM records with no matching fund")
else:
    print("✅ All AUM records match existing funds")

# 3. Check fund scores integrity
print("\n3. Fund Scores Integrity:")
cursor.execute("""
SELECT 
    COUNT(*) as total_scores,
    COUNT(DISTINCT fund_id) as unique_funds,
    MIN(total_score) as min_score,
    MAX(total_score) as max_score,
    AVG(total_score) as avg_score
FROM fund_scores_corrected
""")
scores_stats = cursor.fetchone()
print(f"   Total scores: {scores_stats[0]:,}")
print(f"   Unique funds: {scores_stats[1]:,}")
print(f"   Score range: {scores_stats[2]:.1f} - {scores_stats[3]:.1f}")
print(f"   Average score: {scores_stats[4]:.1f}")

# Check for invalid scores
cursor.execute("""
SELECT COUNT(*) FROM fund_scores_corrected
WHERE total_score < 0 OR total_score > 100
   OR historical_returns_total < 0 OR historical_returns_total > 40
   OR risk_grade_total < 0 OR risk_grade_total > 30
   OR fundamentals_total < 0 OR fundamentals_total > 20
   OR other_metrics_total < 0 OR other_metrics_total > 10
""")
invalid_scores = cursor.fetchone()[0]
if invalid_scores > 0:
    print(f"❌ {invalid_scores} funds with invalid score components")
else:
    print("✅ All score components within valid ranges")

# 4. Check NAV data integrity
print("\n4. NAV Data Integrity:")
cursor.execute("""
SELECT 
    COUNT(*) as total_nav_records,
    COUNT(DISTINCT fund_id) as funds_with_nav,
    MIN(nav_date) as oldest_nav,
    MAX(nav_date) as newest_nav
FROM nav_data
""")
nav_stats = cursor.fetchone()
print(f"   Total NAV records: {nav_stats[0]:,}")
print(f"   Funds with NAV: {nav_stats[1]:,}")
print(f"   Date range: {nav_stats[2]} to {nav_stats[3]}")

# Check for zero or negative NAVs
cursor.execute("""
SELECT COUNT(*) FROM nav_data
WHERE nav <= 0
LIMIT 1
""")
bad_navs = cursor.fetchone()[0]
if bad_navs > 0:
    print(f"❌ {bad_navs} NAV records with zero or negative values")
else:
    print("✅ All NAV values are positive")

# 5. Check benchmark assignments
print("\n5. Benchmark Integrity:")
cursor.execute("""
SELECT benchmark_name, COUNT(*) as fund_count
FROM funds
WHERE benchmark_name IS NULL OR benchmark_name = ''
GROUP BY benchmark_name
""")
null_benchmarks = cursor.fetchall()
if null_benchmarks:
    print(f"❌ {sum(r[1] for r in null_benchmarks)} funds without benchmarks")
else:
    print("✅ All funds have benchmark assignments")

# 6. Check fund metadata completeness
print("\n6. Fund Metadata Completeness:")
cursor.execute("""
SELECT 
    COUNT(*) as total,
    SUM(CASE WHEN expense_ratio IS NULL THEN 1 ELSE 0 END) as no_expense,
    SUM(CASE WHEN category IS NULL OR category = '' THEN 1 ELSE 0 END) as no_category,
    SUM(CASE WHEN amc_name IS NULL OR amc_name = '' THEN 1 ELSE 0 END) as no_amc
FROM funds
""")
metadata = cursor.fetchone()
print(f"   Total funds: {metadata[0]:,}")
if metadata[1] > 0:
    print(f"   ⚠️  Missing expense ratio: {metadata[1]}")
if metadata[2] > 0:
    print(f"   ⚠️  Missing category: {metadata[2]}")
if metadata[3] > 0:
    print(f"   ⚠️  Missing AMC name: {metadata[3]}")
if metadata[1] == 0 and metadata[2] == 0 and metadata[3] == 0:
    print("   ✅ All funds have complete metadata")

# 7. Data consistency check
print("\n7. Cross-Table Consistency:")
cursor.execute("""
SELECT 
    f.id, f.fund_name
FROM funds f
LEFT JOIN fund_scores_corrected s ON f.id = s.fund_id
LEFT JOIN portfolio_holdings h ON f.id = h.fund_id
LEFT JOIN aum_analytics a ON f.fund_name = a.fund_name
WHERE s.fund_id IS NULL 
   OR h.fund_id IS NULL 
   OR a.fund_name IS NULL
LIMIT 5
""")
incomplete = cursor.fetchall()
if incomplete:
    print(f"❌ {len(incomplete)} funds with incomplete data across tables")
    for fund_id, fund_name in incomplete:
        print(f"   Fund {fund_id}: {fund_name[:50]}...")
else:
    print("✅ All funds have data in all major tables")

print("\n" + "="*60)
print("✅ DATABASE INTEGRITY CHECK COMPLETE")

conn.close()
