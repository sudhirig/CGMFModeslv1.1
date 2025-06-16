/**
 * Fixed Advanced Ratios Engine
 * Processes all eligible funds with corrected SQL syntax and optimized performance
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
});

const db = drizzle(pool);

/**
 * Get NAV data for a single fund with optimized query
 */
async function getFundNavData(fundId, years = 5) {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setFullYear(endDate.getFullYear() - years);
  
  const result = await db.execute(sql`
    SELECT nav_date, nav_value
    FROM nav_data 
    WHERE fund_id = ${fundId}
    AND nav_date BETWEEN ${startDate} AND ${endDate}
    AND nav_value > 0
    ORDER BY nav_date ASC
  `);
  
  return result.rows;
}

/**
 * Calculate comprehensive metrics from NAV data
 */
function calculateComprehensiveMetrics(navData) {
  if (!navData || navData.length < 252) return null;
  
  const navValues = navData.map(row => parseFloat(row.nav_value));
  const returns = [];
  
  // Calculate daily returns
  for (let i = 1; i < navValues.length; i++) {
    if (navValues[i] && navValues[i-1]) {
      returns.push((navValues[i] - navValues[i-1]) / navValues[i-1]);
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
  const sharpeRatio = volatility === 0 ? null : (annualizedReturn - riskFreeRate) / volatility;
  
  // Maximum drawdown calculation
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
  
  // Multi-year returns calculation
  let returns_3y = null;
  let returns_5y = null;
  
  if (navValues.length >= 756) { // 3 years minimum
    const startNav = navValues[0];
    const endNav = navValues[navValues.length - 1];
    const actualYears = navValues.length / 252;
    if (actualYears >= 3) {
      returns_3y = Math.pow(endNav / startNav, 1 / actualYears) - 1;
    }
  }
  
  if (navValues.length >= 1260) { // 5 years minimum
    const startNav = navValues[0];
    const endNav = navValues[navValues.length - 1];
    const actualYears = navValues.length / 252;
    if (actualYears >= 5) {
      returns_5y = Math.pow(endNav / startNav, 1 / actualYears) - 1;
    }
  }
  
  return {
    volatility: isFinite(volatility) ? volatility : null,
    sharpe_ratio: isFinite(sharpeRatio) ? sharpeRatio : null,
    max_drawdown: isFinite(maxDrawdown) ? maxDrawdown : null,
    returns_3y: isFinite(returns_3y) ? returns_3y : null,
    returns_5y: isFinite(returns_5y) ? returns_5y : null,
    nav_count: navValues.length,
    returns_count: returns.length
  };
}

/**
 * Update single fund metrics
 */
async function updateFundMetrics(fundId, metrics) {
  try {
    await db.execute(sql`
      UPDATE fund_performance_metrics 
      SET 
        volatility = ${metrics.volatility},
        sharpe_ratio = ${metrics.sharpe_ratio},
        max_drawdown = ${metrics.max_drawdown},
        returns_3y = ${metrics.returns_3y},
        returns_5y = ${metrics.returns_5y},
        calculation_date = NOW()
      WHERE fund_id = ${fundId}
    `);
    return true;
  } catch (error) {
    console.log(`Update failed for fund ${fundId}: ${error.message}`);
    return false;
  }
}

/**
 * Process funds in sequential batches for reliability
 */
async function processFundsBatch(funds, batchNumber, totalBatches) {
  console.log(`Processing batch ${batchNumber}/${totalBatches} (${funds.length} funds)`);
  
  let successCount = 0;
  let processedCount = 0;
  
  for (const fund of funds) {
    try {
      const navData = await getFundNavData(fund.fund_id);
      const metrics = calculateComprehensiveMetrics(navData);
      
      if (metrics) {
        const updated = await updateFundMetrics(fund.fund_id, metrics);
        if (updated) {
          successCount++;
          if (successCount % 10 === 0) {
            console.log(`  ✓ ${successCount} funds completed in batch ${batchNumber}`);
          }
        }
      }
      
      processedCount++;
      
    } catch (error) {
      console.log(`Error processing fund ${fund.fund_id}: ${error.message}`);
    }
  }
  
  console.log(`Batch ${batchNumber} complete: ${successCount}/${processedCount} successful`);
  return successCount;
}

/**
 * Main execution engine
 */
async function fixedAdvancedRatiosEngine() {
  try {
    console.log('Starting Fixed Advanced Ratios Engine');
    console.log('Target: Complete all advanced ratios with authentic data only');
    
    // Get all funds needing calculations
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
    console.log(`Found ${funds.length} funds requiring advanced ratios`);
    
    if (funds.length === 0) {
      console.log('All eligible funds already have complete advanced ratios');
      return;
    }
    
    // Process in manageable batches
    const batchSize = 100;
    const totalBatches = Math.ceil(funds.length / batchSize);
    let totalSuccessful = 0;
    
    const startTime = Date.now();
    
    for (let i = 0; i < funds.length; i += batchSize) {
      const batch = funds.slice(i, i + batchSize);
      const batchNumber = Math.floor(i / batchSize) + 1;
      
      const batchSuccessful = await processFundsBatch(batch, batchNumber, totalBatches);
      totalSuccessful += batchSuccessful;
      
      const elapsed = (Date.now() - startTime) / 1000;
      const processed = Math.min(i + batchSize, funds.length);
      const rate = processed / elapsed;
      const eta = (funds.length - processed) / rate;
      
      console.log(`Overall progress: ${processed}/${funds.length} funds processed`);
      console.log(`Total successful: ${totalSuccessful}`);
      console.log(`Rate: ${rate.toFixed(1)} funds/sec, ETA: ${eta.toFixed(0)}s`);
      console.log('');
      
      // Brief pause between batches
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    // Final status check
    const finalResult = await db.execute(sql`
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
    
    const final = finalResult.rows[0];
    const totalTime = (Date.now() - startTime) / 1000;
    
    console.log('Fixed Advanced Ratios Engine Complete');
    console.log(`Execution time: ${totalTime.toFixed(1)} seconds`);
    console.log(`Successful calculations: ${totalSuccessful}`);
    console.log('');
    console.log('Final Coverage:');
    console.log(`Volatility: ${final.volatility_complete}/${final.total_eligible}`);
    console.log(`Sharpe Ratio: ${final.sharpe_complete}/${final.total_eligible}`);
    console.log(`Max Drawdown: ${final.drawdown_complete}/${final.total_eligible}`);
    console.log(`3Y Returns: ${final.returns_3y_complete}/${final.total_eligible}`);
    console.log(`5Y Returns: ${final.returns_5y_complete}/${final.total_eligible}`);
    console.log('');
    console.log('All calculations completed using authentic market data only');
    
  } catch (error) {
    console.error('Engine error:', error);
  } finally {
    await pool.end();
  }
}

// Execute the engine
fixedAdvancedRatiosEngine();