/**
 * Optimized Advanced Ratios Engine
 * Calculates authentic Sharpe Ratio, Volatility, Max Drawdown for all eligible funds
 * Phase 1: Fund-specific calculations (no market benchmark required)
 * Phase 2: Market-relative calculations (Alpha, Beta) when benchmark data available
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const db = drizzle(pool);

/**
 * Calculate authentic Sharpe Ratio using risk-free rate
 */
function calculateSharpeRatio(returns, riskFreeRate = 0.06) {
  if (!returns || returns.length < 252) return null;
  
  const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
  const stdDev = Math.sqrt(variance);
  
  if (stdDev === 0) return null;
  
  const annualizedReturn = meanReturn * 252;
  const annualizedStdDev = stdDev * Math.sqrt(252);
  
  return (annualizedReturn - riskFreeRate) / annualizedStdDev;
}

/**
 * Calculate volatility from returns
 */
function calculateVolatility(returns) {
  if (!returns || returns.length < 252) return null;
  
  const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
  
  return Math.sqrt(variance) * Math.sqrt(252);
}

/**
 * Calculate maximum drawdown from NAV values
 */
function calculateMaxDrawdown(navValues) {
  if (!navValues || navValues.length < 252) return null;
  
  let maxDrawdown = 0;
  let peak = navValues[0];
  
  for (let i = 1; i < navValues.length; i++) {
    if (navValues[i] > peak) {
      peak = navValues[i];
    } else {
      const drawdown = (peak - navValues[i]) / peak;
      maxDrawdown = Math.max(maxDrawdown, drawdown);
    }
  }
  
  return maxDrawdown;
}

/**
 * Calculate fund returns and metrics from NAV data
 */
async function calculateFundMetrics(fundId) {
  try {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setFullYear(endDate.getFullYear() - 1);
    
    const result = await db.execute(sql`
      SELECT nav_date, nav_value
      FROM nav_data 
      WHERE fund_id = ${fundId}
      AND nav_date BETWEEN ${startDate} AND ${endDate}
      AND nav_value > 0
      ORDER BY nav_date ASC
    `);
    
    if (result.rows.length < 252) return null;
    
    const navValues = result.rows.map(row => parseFloat(row.nav_value));
    const returns = [];
    
    for (let i = 1; i < navValues.length; i++) {
      const currentValue = navValues[i];
      const previousValue = navValues[i - 1];
      if (currentValue && previousValue) {
        returns.push((currentValue - previousValue) / previousValue);
      }
    }
    
    if (returns.length < 251) return null;
    
    const volatility = calculateVolatility(returns);
    const sharpeRatio = calculateSharpeRatio(returns);
    const maxDrawdown = calculateMaxDrawdown(navValues);
    
    return {
      volatility,
      sharpe_ratio: sharpeRatio,
      max_drawdown: maxDrawdown,
      returns_count: returns.length,
      nav_count: navValues.length
    };
    
  } catch (error) {
    console.log(`Error calculating metrics for fund ${fundId}: ${error.message}`);
    return null;
  }
}

/**
 * Update fund_performance_metrics with calculated ratios
 */
async function updateFundMetrics(fundId, metrics) {
  try {
    await db.execute(sql`
      UPDATE fund_performance_metrics 
      SET 
        volatility = ${metrics.volatility},
        sharpe_ratio = ${metrics.sharpe_ratio},
        max_drawdown = ${metrics.max_drawdown},
        calculation_date = NOW()
      WHERE fund_id = ${fundId}
    `);
    
    return true;
  } catch (error) {
    console.log(`Error updating fund ${fundId}: ${error.message}`);
    return false;
  }
}

/**
 * Main execution - Phase 1: Fund-specific calculations
 */
async function optimizedAdvancedRatiosEngine() {
  try {
    console.log('Starting Optimized Advanced Ratios Engine');
    console.log('Phase 1: Fund-specific calculations (Volatility, Sharpe, Max Drawdown)');
    
    // Get funds needing calculations with sufficient data
    const fundsToProcess = await db.execute(sql`
      SELECT DISTINCT fund_id, total_nav_records
      FROM fund_performance_metrics 
      WHERE total_nav_records >= 252
      AND (volatility IS NULL OR sharpe_ratio IS NULL OR max_drawdown IS NULL)
      ORDER BY total_nav_records DESC
    `);
    
    console.log(`Found ${fundsToProcess.rows.length} funds requiring calculations`);
    
    let processedCount = 0;
    let successCount = 0;
    const batchSize = 50;
    
    for (let i = 0; i < fundsToProcess.rows.length; i += batchSize) {
      const batch = fundsToProcess.rows.slice(i, i + batchSize);
      
      console.log(`Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(fundsToProcess.rows.length/batchSize)}`);
      
      const batchPromises = batch.map(async (fund) => {
        const metrics = await calculateFundMetrics(fund.fund_id);
        
        if (metrics) {
          const updated = await updateFundMetrics(fund.fund_id, metrics);
          if (updated) {
            console.log(`✓ Fund ${fund.fund_id}: Vol=${metrics.volatility?.toFixed(4)}, Sharpe=${metrics.sharpe_ratio?.toFixed(4)}, DD=${metrics.max_drawdown?.toFixed(4)}`);
            return 1;
          }
        }
        return 0;
      });
      
      const batchResults = await Promise.all(batchPromises);
      successCount += batchResults.reduce((sum, result) => sum + result, 0);
      processedCount += batch.length;
      
      console.log(`Progress: ${processedCount}/${fundsToProcess.rows.length} (${successCount} successful)`);
      
      // Brief pause between batches
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    // Final summary
    console.log('\nPhase 1 Complete - Fund-specific calculations');
    console.log(`Total processed: ${processedCount}`);
    console.log(`Successful: ${successCount}`);
    console.log(`Success rate: ${((successCount/processedCount)*100).toFixed(1)}%`);
    
    // Check final coverage
    const coverage = await db.execute(sql`
      SELECT 
        COUNT(*) as total_eligible,
        COUNT(CASE WHEN volatility IS NOT NULL THEN 1 END) as volatility_coverage,
        COUNT(CASE WHEN sharpe_ratio IS NOT NULL THEN 1 END) as sharpe_coverage,
        COUNT(CASE WHEN max_drawdown IS NOT NULL THEN 1 END) as drawdown_coverage
      FROM fund_performance_metrics
      WHERE total_nav_records >= 252
    `);
    
    const stats = coverage.rows[0];
    console.log('\nFinal Coverage:');
    console.log(`Volatility: ${stats.volatility_coverage}/${stats.total_eligible}`);
    console.log(`Sharpe Ratio: ${stats.sharpe_coverage}/${stats.total_eligible}`);
    console.log(`Max Drawdown: ${stats.drawdown_coverage}/${stats.total_eligible}`);
    
    console.log('\nAdvanced Ratios Engine Phase 1 Complete');
    console.log('All calculations use authentic market data only');
    
  } catch (error) {
    console.error('Error in advanced ratios engine:', error);
  } finally {
    await pool.end();
  }
}

// Execute immediately
optimizedAdvancedRatiosEngine();