/**
 * Streamlined Advanced Ratios Completion Engine
 * Focused on completing all 17,797 eligible funds efficiently
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const db = drizzle(pool);

async function calculateAndUpdateFund(fundId) {
  try {
    // Get 1 year of NAV data
    const navResult = await db.execute(sql`
      SELECT nav_date, nav_value
      FROM nav_data 
      WHERE fund_id = ${fundId}
      AND nav_date >= CURRENT_DATE - INTERVAL '1 year'
      AND nav_value > 0
      ORDER BY nav_date ASC
    `);
    
    if (navResult.rows.length < 252) return false;
    
    const navValues = navResult.rows.map(row => parseFloat(row.nav_value));
    const returns = [];
    
    // Calculate daily returns
    for (let i = 1; i < navValues.length; i++) {
      returns.push((navValues[i] - navValues[i-1]) / navValues[i-1]);
    }
    
    // Calculate volatility
    const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
    const volatility = Math.sqrt(variance) * Math.sqrt(252);
    
    // Calculate Sharpe ratio
    const annualizedReturn = meanReturn * 252;
    const sharpeRatio = volatility === 0 ? null : (annualizedReturn - 0.06) / volatility;
    
    // Calculate max drawdown
    let maxDrawdown = 0;
    let peak = navValues[0];
    for (let i = 1; i < navValues.length; i++) {
      if (navValues[i] > peak) peak = navValues[i];
      else maxDrawdown = Math.max(maxDrawdown, (peak - navValues[i]) / peak);
    }
    
    // Update database
    await db.execute(sql`
      UPDATE fund_performance_metrics 
      SET 
        volatility = ${volatility},
        sharpe_ratio = ${sharpeRatio},
        max_drawdown = ${maxDrawdown},
        calculation_date = NOW()
      WHERE fund_id = ${fundId}
    `);
    
    return true;
    
  } catch (error) {
    console.log(`Fund ${fundId} failed: ${error.message}`);
    return false;
  }
}

async function main() {
  console.log('Starting streamlined advanced ratios completion');
  
  const fundsResult = await db.execute(sql`
    SELECT fund_id
    FROM fund_performance_metrics 
    WHERE total_nav_records >= 252
    AND volatility IS NULL
    ORDER BY total_nav_records DESC
    LIMIT 1000
  `);
  
  console.log(`Processing ${fundsResult.rows.length} funds`);
  
  let completed = 0;
  for (let i = 0; i < fundsResult.rows.length; i++) {
    const success = await calculateAndUpdateFund(fundsResult.rows[i].fund_id);
    if (success) completed++;
    
    if ((i + 1) % 100 === 0) {
      console.log(`Processed ${i + 1}/${fundsResult.rows.length}, completed: ${completed}`);
    }
  }
  
  console.log(`Completed: ${completed} funds`);
  await pool.end();
}

main();