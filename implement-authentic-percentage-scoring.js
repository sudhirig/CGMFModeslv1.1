/**
 * Direct implementation of authentic percentage-based scoring
 * Uses actual NAV returns as scores without artificial conversion
 */

import pkg from 'pg';
const { Pool } = pkg;

async function implementAuthenticPercentageScoring() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  
  try {
    console.log('Starting authentic percentage-based scoring implementation...');
    
    // Stop all competing background processes
    await pool.query(`
      UPDATE etl_pipeline_runs 
      SET status = 'STOPPED', end_time = NOW()
      WHERE status = 'RUNNING' 
        AND (pipeline_name LIKE '%Scoring%' OR pipeline_name LIKE '%Quartile%')
    `);
    
    // Clear artificial scores
    await pool.query(`DELETE FROM fund_scores WHERE score_date = CURRENT_DATE`);
    
    console.log('Calculating authentic returns from NAV database...');
    
    // Calculate authentic percentage returns directly from NAV movements
    const result = await pool.query(`
      WITH authentic_nav_returns AS (
        SELECT 
          f.id as fund_id,
          f.fund_name,
          f.category,
          f.subcategory,
          
          -- Get exact NAV values
          latest_nav.nav_value as current_nav,
          nav_3m.nav_value as nav_3months_ago,
          nav_1y.nav_value as nav_1year_ago,
          
          -- Calculate exact percentage returns (not converted to scores)
          CASE 
            WHEN nav_3m.nav_value > 0 AND latest_nav.nav_value > 0 THEN
              ROUND(((latest_nav.nav_value - nav_3m.nav_value) / nav_3m.nav_value * 100)::numeric, 6)
            ELSE NULL
          END as authentic_3m_return_percentage,
          
          CASE 
            WHEN nav_1y.nav_value > 0 AND latest_nav.nav_value > 0 THEN
              ROUND(((latest_nav.nav_value - nav_1y.nav_value) / nav_1y.nav_value * 100)::numeric, 6)
            ELSE NULL
          END as authentic_1y_return_percentage,
          
          -- Calculate actual volatility
          (
            SELECT ROUND((STDDEV(daily_return) * SQRT(252) * 100)::numeric, 6)
            FROM (
              SELECT 
                CASE 
                  WHEN lag_nav > 0 THEN (curr_nav - lag_nav) / lag_nav
                  ELSE NULL
                END as daily_return
              FROM (
                SELECT 
                  nav_value as curr_nav,
                  LAG(nav_value) OVER (ORDER BY nav_date) as lag_nav
                FROM nav_data
                WHERE fund_id = f.id
                  AND nav_date >= (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id) - INTERVAL '252 days'
                  AND nav_value > 0
                ORDER BY nav_date
              ) nav_sequence
            ) daily_returns
            WHERE daily_return IS NOT NULL
          ) as authentic_volatility_percentage
          
        FROM funds f
        
        -- Get latest NAV
        LEFT JOIN LATERAL (
          SELECT nav_value, nav_date
          FROM nav_data 
          WHERE fund_id = f.id
          ORDER BY nav_date DESC 
          LIMIT 1
        ) latest_nav ON true
        
        -- Get 3-month NAV
        LEFT JOIN LATERAL (
          SELECT nav_value
          FROM nav_data 
          WHERE fund_id = f.id
            AND nav_date <= latest_nav.nav_date - INTERVAL '90 days'
          ORDER BY nav_date DESC 
          LIMIT 1
        ) nav_3m ON true
        
        -- Get 1-year NAV
        LEFT JOIN LATERAL (
          SELECT nav_value
          FROM nav_data 
          WHERE fund_id = f.id
            AND nav_date <= latest_nav.nav_date - INTERVAL '365 days'
          ORDER BY nav_date DESC 
          LIMIT 1
        ) nav_1y ON true
        
        WHERE f.category IS NOT NULL
          AND latest_nav.nav_value IS NOT NULL
          AND latest_nav.nav_value > 0
      )
      INSERT INTO fund_scores (
        fund_id,
        score_date,
        return_3m_score,
        return_1y_score,
        volatility_1y_percent,
        total_score,
        recommendation,
        subcategory,
        created_at
      )
      SELECT 
        fund_id,
        CURRENT_DATE,
        authentic_3m_return_percentage,  -- Store actual percentage as score
        authentic_1y_return_percentage,  -- Store actual percentage as score
        authentic_volatility_percentage,
        (COALESCE(authentic_3m_return_percentage, 0) + COALESCE(authentic_1y_return_percentage, 0)),
        CASE 
          WHEN (COALESCE(authentic_3m_return_percentage, 0) + COALESCE(authentic_1y_return_percentage, 0)) >= 25 THEN 'STRONG_BUY'
          WHEN (COALESCE(authentic_3m_return_percentage, 0) + COALESCE(authentic_1y_return_percentage, 0)) >= 15 THEN 'BUY'
          WHEN (COALESCE(authentic_3m_return_percentage, 0) + COALESCE(authentic_1y_return_percentage, 0)) >= 5 THEN 'HOLD'
          WHEN (COALESCE(authentic_3m_return_percentage, 0) + COALESCE(authentic_1y_return_percentage, 0)) >= -5 THEN 'SELL'
          ELSE 'STRONG_SELL'
        END,
        subcategory,
        NOW()
      FROM authentic_nav_returns
      WHERE authentic_3m_return_percentage IS NOT NULL 
        AND authentic_1y_return_percentage IS NOT NULL
      RETURNING fund_id
    `);
    
    console.log(`Processed ${result.rowCount} funds with authentic percentage scoring`);
    
    // Verify authentic results
    const verification = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(DISTINCT ROUND(return_3m_score::numeric, 4)) as unique_3m_percentages,
        COUNT(DISTINCT ROUND(return_1y_score::numeric, 4)) as unique_1y_percentages,
        COUNT(DISTINCT ROUND(volatility_1y_percent::numeric, 4)) as unique_volatilities,
        ROUND(MIN(return_3m_score), 4) as min_3m_percentage,
        ROUND(MAX(return_3m_score), 4) as max_3m_percentage,
        ROUND(MIN(return_1y_score), 4) as min_1y_percentage,
        ROUND(MAX(return_1y_score), 4) as max_1y_percentage
      FROM fund_scores 
      WHERE score_date = CURRENT_DATE
    `);
    
    const stats = verification.rows[0];
    console.log('\n=== AUTHENTIC PERCENTAGE SCORING RESULTS ===');
    console.log(`Total funds scored: ${stats.total_funds}`);
    console.log(`Unique 3-month percentages: ${stats.unique_3m_percentages}`);
    console.log(`Unique 1-year percentages: ${stats.unique_1y_percentages}`);
    console.log(`Unique volatilities: ${stats.unique_volatilities}`);
    console.log(`3-month range: ${stats.min_3m_percentage}% to ${stats.max_3m_percentage}%`);
    console.log(`1-year range: ${stats.min_1y_percentage}% to ${stats.max_1y_percentage}%`);
    
    // Sample results
    const sample = await pool.query(`
      SELECT 
        fund_id,
        ROUND(return_3m_score, 4) as actual_3m_percentage,
        ROUND(return_1y_score, 4) as actual_1y_percentage,
        ROUND(volatility_1y_percent, 4) as actual_volatility_percentage,
        recommendation
      FROM fund_scores 
      WHERE score_date = CURRENT_DATE
        AND return_3m_score IS NOT NULL
      ORDER BY return_1y_score DESC
      LIMIT 10
    `);
    
    console.log('\n=== SAMPLE AUTHENTIC PERCENTAGE RESULTS ===');
    sample.rows.forEach(row => {
      console.log(`Fund ${row.fund_id}: 3M=${row.actual_3m_percentage}%, 1Y=${row.actual_1y_percentage}%, Vol=${row.actual_volatility_percentage}% - ${row.recommendation}`);
    });
    
  } catch (error) {
    console.error('Error implementing authentic percentage scoring:', error);
  } finally {
    await pool.end();
  }
}

// Run the implementation
implementAuthenticPercentageScoring().catch(console.error);