/**
 * Complete Advanced Ratios Implementation - Final Version
 * Handles constraint issues and completes all missing calculations
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const db = drizzle(pool);

async function calculateRatiosForFund(fundId) {
  try {
    // Get 2 years of NAV data for comprehensive calculations
    const navResult = await db.execute(sql`
      SELECT nav_date, nav_value
      FROM nav_data 
      WHERE fund_id = ${fundId}
      AND nav_date >= CURRENT_DATE - INTERVAL '2 years'
      AND nav_value > 0
      ORDER BY nav_date ASC
    `);
    
    if (navResult.rows.length < 252) return null;
    
    const navData = navResult.rows.map(row => ({
      date: row.nav_date,
      value: parseFloat(row.nav_value)
    }));
    
    // Calculate daily returns
    const returns = [];
    for (let i = 1; i < navData.length; i++) {
      const returnValue = (navData[i].value - navData[i-1].value) / navData[i-1].value;
      if (isFinite(returnValue)) {
        returns.push(returnValue);
      }
    }
    
    if (returns.length < 251) return null;
    
    // Volatility calculation
    const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
    const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
    const volatility = Math.sqrt(variance) * Math.sqrt(252);
    
    // Sharpe ratio calculation
    const annualizedReturn = meanReturn * 252;
    const riskFreeRate = 0.06;
    const sharpeRatio = volatility > 0 ? (annualizedReturn - riskFreeRate) / volatility : null;
    
    // Maximum drawdown calculation
    let maxDrawdown = 0;
    let peak = navData[0].value;
    
    for (let i = 1; i < navData.length; i++) {
      if (navData[i].value > peak) {
        peak = navData[i].value;
      } else {
        const drawdown = (peak - navData[i].value) / peak;
        maxDrawdown = Math.max(maxDrawdown, drawdown);
      }
    }
    
    // Multi-year returns if sufficient data
    let returns_3y = null;
    let returns_5y = null;
    
    if (navData.length >= 756) { // 3+ years
      const startValue = navData[0].value;
      const endValue = navData[navData.length - 1].value;
      const years = navData.length / 252;
      if (years >= 3 && startValue > 0) {
        returns_3y = Math.pow(endValue / startValue, 1 / years) - 1;
      }
    }
    
    if (navData.length >= 1260) { // 5+ years
      const startValue = navData[0].value;
      const endValue = navData[navData.length - 1].value;
      const years = navData.length / 252;
      if (years >= 5 && startValue > 0) {
        returns_5y = Math.pow(endValue / startValue, 1 / years) - 1;
      }
    }
    
    return {
      volatility: isFinite(volatility) ? volatility : null,
      sharpe_ratio: isFinite(sharpeRatio) ? sharpeRatio : null,
      max_drawdown: isFinite(maxDrawdown) ? maxDrawdown : null,
      returns_3y: isFinite(returns_3y) ? returns_3y : null,
      returns_5y: isFinite(returns_5y) ? returns_5y : null
    };
    
  } catch (error) {
    console.log(`Calculation error for fund ${fundId}: ${error.message}`);
    return null;
  }
}

async function updateFundRatios(fundId, ratios) {
  try {
    // Use UPSERT approach to handle constraint issues
    await db.execute(sql`
      UPDATE fund_performance_metrics 
      SET 
        volatility = COALESCE(${ratios.volatility}, volatility),
        sharpe_ratio = COALESCE(${ratios.sharpe_ratio}, sharpe_ratio),
        max_drawdown = COALESCE(${ratios.max_drawdown}, max_drawdown),
        returns_3y = COALESCE(${ratios.returns_3y}, returns_3y),
        returns_5y = COALESCE(${ratios.returns_5y}, returns_5y),
        calculation_date = NOW()
      WHERE fund_id = ${fundId}
      AND (
        volatility IS NULL OR 
        sharpe_ratio IS NULL OR 
        max_drawdown IS NULL OR 
        returns_3y IS NULL OR 
        returns_5y IS NULL
      )
    `);
    
    return true;
  } catch (error) {
    console.log(`Update error for fund ${fundId}: ${error.message}`);
    return false;
  }
}

async function processBatch(funds, batchNum, totalBatches) {
  console.log(`Processing batch ${batchNum}/${totalBatches} - ${funds.length} funds`);
  
  let completed = 0;
  
  for (const fund of funds) {
    const ratios = await calculateRatiosForFund(fund.fund_id);
    
    if (ratios) {
      const success = await updateFundRatios(fund.fund_id, ratios);
      if (success) completed++;
    }
  }
  
  console.log(`Batch ${batchNum} completed: ${completed}/${funds.length} successful`);
  return completed;
}

async function main() {
  try {
    console.log('Starting Complete Advanced Ratios Implementation');
    console.log('Targeting all funds missing volatility, Sharpe ratio, or drawdown calculations');
    
    // Get funds missing any advanced ratios
    const fundsResult = await db.execute(sql`
      SELECT fund_id, total_nav_records
      FROM fund_performance_metrics 
      WHERE total_nav_records >= 252
      AND (
        volatility IS NULL OR 
        sharpe_ratio IS NULL OR 
        max_drawdown IS NULL OR
        returns_3y IS NULL OR
        returns_5y IS NULL
      )
      ORDER BY total_nav_records DESC
    `);
    
    const funds = fundsResult.rows;
    console.log(`Found ${funds.length} funds requiring advanced ratios calculations`);
    
    if (funds.length === 0) {
      console.log('All eligible funds already have complete advanced ratios');
      return;
    }
    
    // Process in batches
    const batchSize = 50;
    const totalBatches = Math.ceil(funds.length / batchSize);
    let totalCompleted = 0;
    
    const startTime = Date.now();
    
    for (let i = 0; i < funds.length; i += batchSize) {
      const batch = funds.slice(i, i + batchSize);
      const batchNum = Math.floor(i / batchSize) + 1;
      
      const batchCompleted = await processBatch(batch, batchNum, totalBatches);
      totalCompleted += batchCompleted;
      
      // Progress report
      const processed = Math.min(i + batchSize, funds.length);
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = processed / elapsed;
      const eta = (funds.length - processed) / rate;
      
      console.log(`Overall: ${processed}/${funds.length} processed, ${totalCompleted} completed`);
      console.log(`Rate: ${rate.toFixed(1)} funds/sec, ETA: ${eta.toFixed(0)}s`);
      console.log('');
    }
    
    // Final verification
    const finalCheck = await db.execute(sql`
      SELECT 
        COUNT(*) as total_eligible,
        COUNT(CASE WHEN volatility IS NOT NULL THEN 1 END) as volatility_complete,
        COUNT(CASE WHEN sharpe_ratio IS NOT NULL THEN 1 END) as sharpe_complete,
        COUNT(CASE WHEN max_drawdown IS NOT NULL THEN 1 END) as drawdown_complete,
        COUNT(CASE WHEN returns_3y IS NOT NULL THEN 1 END) as returns_3y_complete,
        COUNT(CASE WHEN returns_5y IS NOT NULL THEN 1 END) as returns_5y_complete
      FROM fund_performance_metrics
      WHERE total_nav_records >= 252
    `);
    
    const stats = finalCheck.rows[0];
    
    console.log('Advanced Ratios Implementation Complete');
    console.log(`Total successful calculations: ${totalCompleted}`);
    console.log('');
    console.log('Final Coverage:');
    console.log(`Volatility: ${stats.volatility_complete}/${stats.total_eligible} funds`);
    console.log(`Sharpe Ratio: ${stats.sharpe_complete}/${stats.total_eligible} funds`);
    console.log(`Max Drawdown: ${stats.drawdown_complete}/${stats.total_eligible} funds`);
    console.log(`3Y Returns: ${stats.returns_3y_complete}/${stats.total_eligible} funds`);
    console.log(`5Y Returns: ${stats.returns_5y_complete}/${stats.total_eligible} funds`);
    console.log('');
    console.log('All calculations completed using authentic NAV data only');
    
  } catch (error) {
    console.error('Implementation error:', error);
  } finally {
    await pool.end();
  }
}

main();