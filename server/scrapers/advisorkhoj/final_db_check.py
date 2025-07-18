#!/usr/bin/env python3
"""
Final Database Check and Frontend Data Test
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

print("\n🔍 FINAL DATABASE CHECK")
print("=" * 60)

# 1. NAV Data Check
print("\n1. NAV Data Integrity:")
cursor.execute("""
SELECT COUNT(*) FROM nav_data
WHERE nav_value <= 0
""")
bad_navs = cursor.fetchone()[0]
if bad_navs > 0:
    print(f"   ⚠️  {bad_navs} NAV records with zero or negative values")
else:
    print("   ✅ All NAV values are positive")

# 2. Overall completeness summary
print("\n2. Data Completeness Summary:")
cursor.execute("""
SELECT 
    COUNT(*) as total_funds,
    COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM portfolio_holdings WHERE fund_id = f.id) THEN f.id END) as with_holdings,
    COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM aum_analytics WHERE fund_name = f.fund_name) THEN f.id END) as with_aum,
    COUNT(CASE WHEN benchmark_name IS NOT NULL AND benchmark_name != '' THEN 1 END) as with_benchmarks,
    COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM fund_scores_corrected WHERE fund_id = f.id) THEN f.id END) as with_scores,
    COUNT(DISTINCT CASE WHEN EXISTS (SELECT 1 FROM nav_data WHERE fund_id = f.id) THEN f.id END) as with_nav
FROM funds f
""")
stats = cursor.fetchone()
total = stats[0]

print(f"   Total funds: {total:,}")
print(f"   ✅ With holdings: {stats[1]:,} ({round(stats[1]/total*100,1)}%)")
print(f"   ✅ With AUM: {stats[2]:,} ({round(stats[2]/total*100,1)}%)")
print(f"   ✅ With benchmarks: {stats[3]:,} ({round(stats[3]/total*100,1)}%)")
print(f"   ✅ With scores: {stats[4]:,} ({round(stats[4]/total*100,1)}%)")
print(f"   ✅ With NAV data: {stats[5]:,} ({round(stats[5]/total*100,1)}%)")

# 3. Check holding percentages after fix
print("\n3. Holdings Percentage Check (After Fix):")
cursor.execute("""
SELECT fund_id, SUM(holding_percent) as total_percent
FROM portfolio_holdings
GROUP BY fund_id
HAVING SUM(holding_percent) < 95 OR SUM(holding_percent) > 105
LIMIT 5
""")
bad_holdings = cursor.fetchall()
if bad_holdings:
    print(f"   ⚠️  Still {len(bad_holdings)} funds with incorrect percentages")
else:
    print("   ✅ All holdings now sum to ~100%")

# 4. Check AUM duplicates after fix
print("\n4. AUM Duplicates Check (After Fix):")
cursor.execute("""
SELECT fund_name, COUNT(*) as count
FROM aum_analytics
GROUP BY fund_name
HAVING COUNT(*) > 1
LIMIT 5
""")
aum_dupes = cursor.fetchall()
if aum_dupes:
    print(f"   ⚠️  Still have duplicate AUM records")
else:
    print("   ✅ No duplicate AUM records")

# 5. Sample data for frontend testing
print("\n5. Sample Data for Frontend Testing:")
print("-" * 60)

# Get funds with all data for testing
cursor.execute("""
SELECT 
    f.id,
    f.scheme_code,
    f.fund_name,
    f.category,
    f.expense_ratio,
    f.benchmark_name,
    a.aum_crores,
    s.total_score,
    s.quartile,
    s.recommendation,
    COUNT(DISTINCT h.id) as holdings_count,
    COUNT(DISTINCT n.nav_date) as nav_days
FROM funds f
LEFT JOIN aum_analytics a ON f.fund_name = a.fund_name
LEFT JOIN fund_scores_corrected s ON f.id = s.fund_id
LEFT JOIN portfolio_holdings h ON f.id = h.fund_id
LEFT JOIN nav_data n ON f.id = n.fund_id
WHERE f.id IN (100, 500, 1000, 5000, 10000, 15000)
GROUP BY f.id, f.scheme_code, f.fund_name, f.category, f.expense_ratio, 
         f.benchmark_name, a.aum_crores, s.total_score, s.quartile, s.recommendation
ORDER BY f.id
""")

samples = cursor.fetchall()
for sample in samples:
    fund_id, scheme_code, fund_name, category, expense_ratio, benchmark, aum, score, quartile, recommendation, holdings, nav_days = sample
    print(f"\n🔸 Fund ID {fund_id}: {fund_name[:60]}...")
    print(f"   Scheme Code: {scheme_code}")
    print(f"   Category: {category}")
    print(f"   Expense Ratio: {expense_ratio}%")
    print(f"   Benchmark: {benchmark}")
    print(f"   AUM: ₹{aum:,.0f} Cr" if aum else "   AUM: N/A")
    print(f"   Score: {score} (Quartile {quartile})" if score else "   Score: N/A")
    print(f"   Recommendation: {recommendation}" if recommendation else "   Recommendation: N/A")
    print(f"   Holdings: {holdings} stocks")
    print(f"   NAV History: {nav_days} days")

# 6. Check specific API endpoints data
print("\n\n6. API Endpoint Data Check:")
print("-" * 60)

# Check top-rated funds
cursor.execute("""
SELECT COUNT(*) 
FROM fund_scores_corrected 
WHERE recommendation = 'STRONG_BUY'
""")
strong_buy_count = cursor.fetchone()[0]
print(f"   STRONG_BUY funds: {strong_buy_count}")

# Check market indices
cursor.execute("""
SELECT index_name, COUNT(*) as records, MAX(index_date) as latest
FROM market_indices
GROUP BY index_name
ORDER BY records DESC
LIMIT 5
""")
print("\n   Market Indices:")
for index_name, count, latest in cursor.fetchall():
    print(f"   - {index_name}: {count} records (latest: {latest})")

# Check ELIVATE score
cursor.execute("""
SELECT score_date, total_elivate_score, market_stance
FROM elivate_scores
ORDER BY score_date DESC
LIMIT 1
""")
elivate = cursor.fetchone()
if elivate:
    print(f"\n   ELIVATE Score: {elivate[1]}/100 ({elivate[2]}) as of {elivate[0]}")
else:
    print("\n   ELIVATE Score: No data")

print("\n" + "="*60)
print("✅ DATABASE CHECK COMPLETE - Ready for Frontend Testing")
print("\nRecommended Frontend Tests:")
print("1. Dashboard - Check ELIVATE gauge and top funds display")
print("2. Fund Search - Verify pagination and fund details")
print("3. Fund Analysis - Check individual fund data display")
print("4. Portfolio Holdings - Verify holdings percentage display")
print("5. Benchmark Rolling Returns - Check benchmark data")

conn.close()