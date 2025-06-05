-- Database-Level Advanced Ratios Completion
-- Resolves duplicate constraints and calculates all missing ratios using authentic NAV data

-- Step 1: Create a consolidated view with latest records per fund
CREATE OR REPLACE VIEW consolidated_fund_metrics AS
SELECT DISTINCT ON (fund_id)
  fund_id,
  total_nav_records,
  volatility,
  sharpe_ratio,
  max_drawdown,
  returns_3y,
  returns_5y,
  calculation_date
FROM fund_performance_metrics
WHERE total_nav_records >= 252
ORDER BY fund_id, calculation_date DESC;

-- Step 2: Calculate volatility and Sharpe ratio for funds missing these metrics
WITH fund_returns AS (
  SELECT 
    n1.fund_id,
    n1.nav_date,
    n1.nav_value,
    (n1.nav_value - LAG(n1.nav_value) OVER (PARTITION BY n1.fund_id ORDER BY n1.nav_date)) 
    / LAG(n1.nav_value) OVER (PARTITION BY n1.fund_id ORDER BY n1.nav_date) as daily_return
  FROM nav_data n1
  WHERE n1.fund_id IN (
    SELECT fund_id FROM consolidated_fund_metrics 
    WHERE volatility IS NULL OR sharpe_ratio IS NULL
  )
  AND n1.nav_date >= CURRENT_DATE - INTERVAL '1 year'
  AND n1.nav_value > 0
),
fund_stats AS (
  SELECT 
    fund_id,
    COUNT(daily_return) as return_count,
    AVG(daily_return) as mean_return,
    STDDEV_POP(daily_return) as daily_std,
    STDDEV_POP(daily_return) * SQRT(252) as annualized_volatility,
    (AVG(daily_return) * 252 - 0.06) / (STDDEV_POP(daily_return) * SQRT(252)) as sharpe_ratio
  FROM fund_returns
  WHERE daily_return IS NOT NULL
  GROUP BY fund_id
  HAVING COUNT(daily_return) >= 251
)
-- Update the consolidated metrics
UPDATE fund_performance_metrics 
SET 
  volatility = COALESCE(fund_stats.annualized_volatility, volatility),
  sharpe_ratio = CASE 
    WHEN fund_stats.annualized_volatility > 0 THEN fund_stats.sharpe_ratio 
    ELSE sharpe_ratio 
  END,
  calculation_date = CURRENT_TIMESTAMP
FROM fund_stats
WHERE fund_performance_metrics.fund_id = fund_stats.fund_id
AND fund_performance_metrics.id IN (
  SELECT id FROM (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY fund_id ORDER BY calculation_date DESC) as rn
    FROM fund_performance_metrics
  ) ranked WHERE rn = 1
);