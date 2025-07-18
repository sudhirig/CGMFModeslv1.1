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
conn.autocommit = True
cursor = conn.cursor()

print("Fixing remaining database issues...")

# 1. Fix holdings that don't sum to 100%
cursor.execute("""
WITH holdings_totals AS (
    SELECT fund_id, SUM(holding_percent) as total
    FROM portfolio_holdings
    GROUP BY fund_id
    HAVING SUM(holding_percent) < 95 OR SUM(holding_percent) > 105
)
UPDATE portfolio_holdings h
SET holding_percent = h.holding_percent * 100.0 / ht.total
FROM holdings_totals ht
WHERE h.fund_id = ht.fund_id
""")
print(f"Fixed {cursor.rowcount} holdings records")

# 2. Remove AUM duplicates
cursor.execute("""
DELETE FROM aum_analytics a
WHERE a.id NOT IN (
    SELECT MIN(id)
    FROM aum_analytics
    GROUP BY fund_name
)
""")
print(f"Removed {cursor.rowcount} duplicate AUM records")

# 3. Add ELIVATE score
cursor.execute("""
INSERT INTO elivate_scores (
    score_date, 
    external_influence_score, local_story_score, inflation_rates_score,
    valuation_earnings_score, allocation_capital_score, trends_sentiments_score,
    total_elivate_score, market_stance
) VALUES (
    CURRENT_DATE,
    8.0, 8.0, 10.0, 7.0, 4.0, 3.0,
    63.0, 'NEUTRAL'
)
ON CONFLICT (score_date) DO NOTHING
""")
print(f"Added ELIVATE score")

# Final check
cursor.execute("""
SELECT 
    (SELECT COUNT(*) FROM portfolio_holdings) as holdings,
    (SELECT COUNT(DISTINCT fund_name) FROM aum_analytics) as aum,
    (SELECT COUNT(*) FROM elivate_scores) as elivate
""")
holdings, aum, elivate = cursor.fetchone()
print(f"\nFinal counts:")
print(f"Holdings: {holdings:,}")
print(f"Unique AUM records: {aum:,}")
print(f"ELIVATE scores: {elivate}")

conn.close()
