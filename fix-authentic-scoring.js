/**
 * Fix the scoring system to preserve actual percentage returns
 * instead of converting them to artificial quartile buckets
 */

import pkg from 'pg';
const { Pool } = pkg;

async function fixAuthenticScoring() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  
  try {
    console.log('Fixing scoring system to preserve actual percentage returns...');
    
    // Stop all background scoring processes
    await pool.query(`
      UPDATE etl_pipeline_runs 
      SET status = 'STOPPED', end_time = NOW()
      WHERE status = 'RUNNING'
    `);
    
    // Clear existing artificial scores
    await pool.query(`DELETE FROM fund_scores WHERE score_date = CURRENT_DATE`);
    
    console.log('Calculating and storing actual percentage returns...');
    
    // Calculate real percentage returns and store them directly
    const result = await pool.query(`
      WITH real_nav_returns AS (
        SELECT 
          f.id as fund_id,
          f.category,
          f.subcategory,
          f.expense_ratio,
          
          -- Calculate actual percentage returns from your NAV database
          CASE 
            WHEN nav_3m.nav_value > 0 AND current_nav.nav_value > 0 THEN
              ((current_nav.nav_value - nav_3m.nav_value) / nav_3m.nav_value * 100)
            ELSE NULL
          END as real_3month_return_percent,
          
          CASE 
            WHEN nav_1y.nav_value > 0 AND current_nav.nav_value > 0 THEN
              ((current_nav.nav_value - nav_1y.nav_value) / nav_1y.nav_value * 100)
            ELSE NULL
          END as real_1year_return_percent,
          
          -- Calculate actual volatility
          (
            SELECT STDDEV(daily_return) * SQRT(252) * 100
            FROM (
              SELECT 
                CASE 
                  WHEN prev_nav > 0 THEN (curr_nav - prev_nav) / prev_nav
                  ELSE NULL
                END as daily_return
              FROM (
                SELECT 
                  nav_value as curr_nav,
                  LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
                FROM nav_data
                WHERE fund_id = f.id
                  AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
                  AND nav_value > 0
                ORDER BY nav_date
              ) daily_navs
            ) daily_returns
            WHERE daily_return IS NOT NULL
          ) as real_volatility_percent
          
        FROM funds f
        
        LEFT JOIN LATERAL (
          SELECT nav_value
          FROM nav_data 
          WHERE fund_id = f.id
          ORDER BY nav_date DESC 
          LIMIT 1
        ) current_nav ON true
        
        LEFT JOIN LATERAL (
          SELECT nav_value
          FROM nav_data 
          WHERE fund_id = f.id
            AND nav_date <= (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id) - INTERVAL '90 days'
          ORDER BY nav_date DESC 
          LIMIT 1
        ) nav_3m ON true
        
        LEFT JOIN LATERAL (
          SELECT nav_value
          FROM nav_data 
          WHERE fund_id = f.id
            AND nav_date <= (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id) - INTERVAL '365 days'
          ORDER BY nav_date DESC 
          LIMIT 1
        ) nav_1y ON true
        
        WHERE f.category IS NOT NULL
          AND current_nav.nav_value IS NOT NULL
      )
      INSERT INTO fund_scores (
        fund_id,
        score_date,
        return_3m_score,
        return_1y_score,
        volatility_1y_percent,
        total_score,
        recommendation,
        expense_ratio_score,
        subcategory,
        created_at
      )
      SELECT 
        fund_id,
        CURRENT_DATE,
        real_3month_return_percent,  -- Store actual percentage, not converted
        real_1year_return_percent,   -- Store actual percentage, not converted
        real_volatility_percent,     -- Store actual volatility percentage
        COALESCE(real_3month_return_percent, 0) + COALESCE(real_1year_return_percent, 0),
        CASE 
          WHEN COALESCE(real_3month_return_percent, 0) + COALESCE(real_1year_return_percent, 0) >= 25 THEN 'STRONG_BUY'
          WHEN COALESCE(real_3month_return_percent, 0) + COALESCE(real_1year_return_percent, 0) >= 15 THEN 'BUY'
          WHEN COALESCE(real_3month_return_percent, 0) + COALESCE(real_1year_return_percent, 0) >= 5 THEN 'HOLD'
          WHEN COALESCE(real_3month_return_percent, 0) + COALESCE(real_1year_return_percent, 0) >= -5 THEN 'SELL'
          ELSE 'STRONG_SELL'
        END,
        GREATEST(0, 10 - expense_ratio * 4),
        subcategory,
        NOW()
      FROM real_nav_returns
      WHERE real_3month_return_percent IS NOT NULL 
        AND real_1year_return_percent IS NOT NULL
    `);
    
    console.log(`Successfully stored authentic returns for ${result.rowCount} funds`);
    
    // Verify the authentic data
    const verification = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(DISTINCT ROUND(return_3m_score::numeric, 4)) as unique_3m_returns,
        COUNT(DISTINCT ROUND(return_1y_score::numeric, 4)) as unique_1y_returns,
        COUNT(DISTINCT ROUND(volatility_1y_percent::numeric, 4)) as unique_volatilities,
        ROUND(MIN(return_3m_score), 4) as min_3m_return,
        ROUND(MAX(return_3m_score), 4) as max_3m_return,
        ROUND(MIN(return_1y_score), 4) as min_1y_return,
        ROUND(MAX(return_1y_score), 4) as max_1y_return
      FROM fund_scores 
      WHERE score_date = CURRENT_DATE
    `);
    
    const stats = verification.rows[0];
    console.log('\n=== AUTHENTIC PERCENTAGE RETURNS PRESERVED ===');
    console.log(`Total funds with authentic data: ${stats.total_funds}`);
    console.log(`Unique 3-month returns: ${stats.unique_3m_returns}`);
    console.log(`Unique 1-year returns: ${stats.unique_1y_returns}`);
    console.log(`Unique volatilities: ${stats.unique_volatilities}`);
    console.log(`3-month return range: ${stats.min_3m_return}% to ${stats.max_3m_return}%`);
    console.log(`1-year return range: ${stats.min_1y_return}% to ${stats.max_1y_return}%`);
    
    // Show sample of actual returns (not artificial buckets)
    const sample = await pool.query(`
      SELECT 
        fund_id,
        ROUND(return_3m_score, 4) as actual_3m_return_percent,
        ROUND(return_1y_score, 4) as actual_1y_return_percent,
        ROUND(volatility_1y_percent, 4) as actual_volatility_percent,
        recommendation
      FROM fund_scores 
      WHERE score_date = CURRENT_DATE
      ORDER BY return_1y_score DESC
      LIMIT 15
    `);
    
    console.log('\n=== SAMPLE AUTHENTIC RETURNS (NOT ARTIFICIAL BUCKETS) ===');
    sample.rows.forEach(row => {
      console.log(`Fund ${row.fund_id}: 3M=${row.actual_3m_return_percent}%, 1Y=${row.actual_1y_return_percent}%, Vol=${row.actual_volatility_percent}% - ${row.recommendation}`);
    });
    
  } catch (error) {
    console.error('Error fixing authentic scoring:', error);
  } finally {
    await pool.end();
  }
}

fixAuthenticScoring();