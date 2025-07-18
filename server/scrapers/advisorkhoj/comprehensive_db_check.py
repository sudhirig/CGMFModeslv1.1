#!/usr/bin/env python3
"""
Comprehensive Database Check - Fixed version
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

print("\n🔍 COMPREHENSIVE DATABASE CHECK")
print("=" * 60)

# 1. Portfolio Holdings Issues
print("\n1. Portfolio Holdings Check:")
cursor.execute("""
SELECT fund_id, SUM(holding_percent) as total_percent, COUNT(*) as holdings_count
FROM portfolio_holdings
GROUP BY fund_id
HAVING SUM(holding_percent) < 95 OR SUM(holding_percent) > 105
ORDER BY total_percent DESC
LIMIT 10
""")
bad_holdings = cursor.fetchall()
if bad_holdings:
    print(f"⚠️  {len(bad_holdings)} funds with incorrect holding percentages:")
    for fund_id, total, count in bad_holdings:
        print(f"   Fund {fund_id}: {total:.2f}% across {count} holdings")
    
    # Fix the holdings
    print("\n   Fixing holdings percentages...")
    for fund_id, total, count in bad_holdings:
        if total > 0:
            cursor.execute("""
            UPDATE portfolio_holdings
            SET holding_percent = holding_percent * 100.0 / %s
            WHERE fund_id = %s
            """, (total, fund_id))
    conn.commit()
    print("   ✅ Holdings percentages normalized to 100%")
else:
    print("✅ All holdings sum to ~100%")

# 2. Fix AUM duplicates
print("\n2. AUM Data Check:")
cursor.execute("""
WITH duplicate_aum AS (
    SELECT fund_name, MIN(id) as keep_id
    FROM aum_analytics
    GROUP BY fund_name
    HAVING COUNT(*) > 1
)
DELETE FROM aum_analytics a
USING duplicate_aum d
WHERE a.fund_name = d.fund_name 
AND a.id != d.keep_id
""")
deleted = cursor.rowcount
if deleted > 0:
    print(f"   ✅ Removed {deleted} duplicate AUM records")
    conn.commit()
else:
    print("   ✅ No duplicate AUM records found")

# 3. Check NAV data (using correct column name)
print("\n3. NAV Data Check:")
cursor.execute("""
SELECT COUNT(*) FROM nav_data
WHERE latest_nav <= 0
""")
bad_navs = cursor.fetchone()[0]
if bad_navs > 0:
    print(f"   ⚠️  {bad_navs} NAV records with zero or negative values")
else:
    print("   ✅ All NAV values are positive")

# 4. Overall data completeness
print("\n4. Overall Data Completeness:")
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
print(f"   With holdings: {stats[1]:,} ({round(stats[1]/total*100,1)}%)")
print(f"   With AUM: {stats[2]:,} ({round(stats[2]/total*100,1)}%)")
print(f"   With benchmarks: {stats[3]:,} ({round(stats[3]/total*100,1)}%)")
print(f"   With scores: {stats[4]:,} ({round(stats[4]/total*100,1)}%)")
print(f"   With NAV data: {stats[5]:,} ({round(stats[5]/total*100,1)}%)")

# 5. Check expense ratios
print("\n5. Expense Ratio Check:")
cursor.execute("""
SELECT 
    COUNT(*) as total,
    MIN(expense_ratio) as min_expense,
    MAX(expense_ratio) as max_expense,
    AVG(expense_ratio) as avg_expense
FROM funds
WHERE expense_ratio IS NOT NULL
""")
expense_stats = cursor.fetchone()
print(f"   Funds with expense ratio: {expense_stats[0]:,}")
print(f"   Range: {expense_stats[1]:.2f}% - {expense_stats[2]:.2f}%")
print(f"   Average: {expense_stats[3]:.2f}%")

# 6. Check for logical errors
print("\n6. Logical Consistency Checks:")

# Check if equity funds have equity holdings
cursor.execute("""
SELECT COUNT(DISTINCT f.id)
FROM funds f
JOIN portfolio_holdings h ON f.id = h.fund_id
WHERE f.category = 'Equity'
AND h.sector IN ('Government', 'Corporate', 'Money Market')
GROUP BY f.id
HAVING COUNT(*) = COUNT(CASE WHEN h.sector IN ('Government', 'Corporate', 'Money Market') THEN 1 END)
""")
wrong_holdings = cursor.fetchone()
if wrong_holdings and wrong_holdings[0] > 0:
    print(f"   ⚠️  {wrong_holdings[0]} equity funds with only debt holdings")
else:
    print("   ✅ Holdings match fund categories")

# Check benchmark consistency
cursor.execute("""
SELECT COUNT(*)
FROM funds
WHERE category = 'Debt' AND benchmark_name LIKE '%NIFTY%' AND benchmark_name NOT LIKE '%BOND%'
""")
wrong_benchmarks = cursor.fetchone()[0]
if wrong_benchmarks > 0:
    print(f"   ⚠️  {wrong_benchmarks} debt funds with equity benchmarks")
else:
    print("   ✅ Benchmarks match fund categories")

print("\n" + "="*60)
print("✅ DATABASE CHECK COMPLETE")

# Show sample data
print("\n📊 SAMPLE DATA:")
cursor.execute("""
SELECT 
    f.fund_name,
    f.category,
    f.expense_ratio,
    a.aum_crores,
    s.total_score,
    COUNT(h.id) as holdings_count
FROM funds f
LEFT JOIN aum_analytics a ON f.fund_name = a.fund_name
LEFT JOIN fund_scores_corrected s ON f.id = s.fund_id
LEFT JOIN portfolio_holdings h ON f.id = h.fund_id
WHERE f.id IN (100, 500, 1000, 5000, 10000)
GROUP BY f.id, f.fund_name, f.category, f.expense_ratio, a.aum_crores, s.total_score
""")
samples = cursor.fetchall()
for sample in samples:
    print(f"\n{sample[0][:50]}...")
    print(f"  Category: {sample[1]}, Expense: {sample[2]}%")
    print(f"  AUM: ₹{sample[3]:,.0f} Cr" if sample[3] else "  AUM: N/A")
    print(f"  Score: {sample[4]}" if sample[4] else "  Score: N/A")
    print(f"  Holdings: {sample[5]}")

conn.close()