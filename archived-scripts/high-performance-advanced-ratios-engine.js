/**
 * High-Performance Advanced Ratios Engine
 * Processes all 17,797 eligible funds with authentic calculations
 * Optimized for maximum throughput while maintaining data integrity
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { sql } from 'drizzle-orm';
import pg from 'pg';

const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20, // Increased connection pool
});

const db = drizzle(pool);

/**
 * Bulk NAV data retrieval for multiple funds
 */
async function getBulkNavData(fundIds, years = 1) {
  const endDate = new Date();
  const startDate = new Date();
  startDate.setFullYear(endDate.getFullYear() - years);
  
  const result = await db.execute(sql`
    SELECT 
      fund_id,
      nav_date,
      nav_value,
      LAG(nav_value) OVER (PARTITION BY fund_id ORDER BY nav_date) as prev_nav
    FROM nav_data 
    WHERE fund_id = ANY(${fundIds})
    AND nav_date BETWEEN ${startDate} AND ${endDate}
    AND nav_value > 0
    ORDER BY fund_id, nav_date
  `);
  
  return result.rows;
}

/**
 * Calculate all metrics for a fund from NAV data
 */
function calculateAllMetrics(navData) {
  if (!navData || navData.length < 252) return null;
  
  const returns = [];
  const navValues = [];
  
  // Process NAV data
  for (const record of navData) {
    navValues.push(parseFloat(record.nav_value));
    
    if (record.prev_nav) {
      const currentNav = parseFloat(record.nav_value);
      const prevNav = parseFloat(record.prev_nav);
      if (currentNav && prevNav) {
        returns.push((currentNav - prevNav) / prevNav);
      }
    }
  }
  
  if (returns.length < 251) return null;
  
  // Calculate volatility
  const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
  const volatility = Math.sqrt(variance) * Math.sqrt(252);
  
  // Calculate Sharpe ratio
  const annualizedReturn = meanReturn * 252;
  const riskFreeRate = 0.06;
  const sharpeRatio = volatility === 0 ? null : (annualizedReturn - riskFreeRate) / volatility;
  
  // Calculate maximum drawdown
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
  
  // Calculate multi-year returns if sufficient data
  let returns_3y = null;
  let returns_5y = null;
  
  if (navValues.length >= 756) { // 3 years
    const startNav = navValues[0];
    const endNav = navValues[navValues.length - 1];
    const actualYears = navValues.length / 252;
    returns_3y = Math.pow(endNav / startNav, 1 / actualYears) - 1;
  }
  
  if (navValues.length >= 1260) { // 5 years
    const startNav = navValues[0];
    const endNav = navValues[navValues.length - 1];
    const actualYears = navValues.length / 252;
    returns_5y = Math.pow(endNav / startNav, 1 / actualYears) - 1;
  }
  
  return {
    volatility,
    sharpe_ratio: sharpeRatio,
    max_drawdown: maxDrawdown,
    returns_3y,
    returns_5y,
    data_points: navValues.length,
    return_data_points: returns.length
  };
}

/**
 * Bulk update fund performance metrics
 */
async function bulkUpdateMetrics(updates) {
  const batchSize = 100;
  let successCount = 0;
  
  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    
    try {
      // Use transaction for batch updates
      await db.transaction(async (tx) => {
        for (const update of batch) {
          await tx.execute(sql`
            UPDATE fund_performance_metrics 
            SET 
              volatility = ${update.volatility},
              sharpe_ratio = ${update.sharpe_ratio},
              max_drawdown = ${update.max_drawdown},
              returns_3y = ${update.returns_3y},
              returns_5y = ${update.returns_5y},
              calculation_date = NOW()
            WHERE fund_id = ${update.fund_id}
          `);
        }
      });
      
      successCount += batch.length;
      console.log(`✓ Updated batch ${Math.floor(i/batchSize) + 1}: ${batch.length} funds`);
      
    } catch (error) {
      console.log(`Error updating batch starting at ${i}: ${error.message}`);
      
      // Try individual updates for failed batch
      for (const update of batch) {
        try {
          await db.execute(sql`
            UPDATE fund_performance_metrics 
            SET 
              volatility = ${update.volatility},
              sharpe_ratio = ${update.sharpe_ratio},
              max_drawdown = ${update.max_drawdown},
              returns_3y = ${update.returns_3y},
              returns_5y = ${update.returns_5y},
              calculation_date = NOW()
            WHERE fund_id = ${update.fund_id}
          `);
          successCount++;
        } catch (individualError) {
          console.log(`Failed to update fund ${update.fund_id}: ${individualError.message}`);
        }
      }
    }
  }
  
  return successCount;
}

/**
 * Process funds in parallel batches
 */
async function processFundBatch(fundIds) {
  try {
    console.log(`Processing batch of ${fundIds.length} funds...`);
    
    // Get NAV data for all funds in batch
    const navData = await getBulkNavData(fundIds, 5); // Get 5 years max for comprehensive analysis
    
    // Group NAV data by fund
    const fundNavData = {};
    for (const record of navData) {
      if (!fundNavData[record.fund_id]) {
        fundNavData[record.fund_id] = [];
      }
      fundNavData[record.fund_id].push(record);
    }
    
    // Calculate metrics for each fund
    const updates = [];
    for (const fundId of fundIds) {
      const metrics = calculateAllMetrics(fundNavData[fundId]);
      if (metrics) {
        updates.push({
          fund_id: fundId,
          ...metrics
        });
      }
    }
    
    console.log(`Calculated metrics for ${updates.length}/${fundIds.length} funds in batch`);
    
    // Bulk update database
    if (updates.length > 0) {
      const successCount = await bulkUpdateMetrics(updates);
      return successCount;
    }
    
    return 0;
    
  } catch (error) {
    console.log(`Error processing batch: ${error.message}`);
    return 0;
  }
}

/**
 * Main high-performance execution
 */
async function highPerformanceAdvancedRatiosEngine() {
  try {
    console.log('🚀 High-Performance Advanced Ratios Engine Starting');
    console.log('📊 Target: Complete Advanced Ratios for all eligible funds');
    console.log('🔒 Authentic data only - zero synthetic generation');
    
    // Get all funds needing advanced ratios
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
    
    const fundIds = fundsResult.rows.map(row => row.fund_id);
    console.log(`📈 Found ${fundIds.length} funds requiring advanced ratios`);
    
    if (fundIds.length === 0) {
      console.log('✅ All eligible funds already have complete advanced ratios');
      return;
    }
    
    // Process in optimized parallel batches
    const batchSize = 500; // Large batches for efficiency
    const maxConcurrent = 4; // Parallel processing
    let totalProcessed = 0;
    let totalSuccessful = 0;
    
    const startTime = Date.now();
    
    for (let i = 0; i < fundIds.length; i += batchSize * maxConcurrent) {
      const batchPromises = [];
      
      // Create concurrent batch processes
      for (let j = 0; j < maxConcurrent && (i + j * batchSize) < fundIds.length; j++) {
        const startIdx = i + j * batchSize;
        const endIdx = Math.min(startIdx + batchSize, fundIds.length);
        const batchFundIds = fundIds.slice(startIdx, endIdx);
        
        batchPromises.push(processFundBatch(batchFundIds));
      }
      
      // Wait for all concurrent batches to complete
      const batchResults = await Promise.all(batchPromises);
      const batchSuccessful = batchResults.reduce((sum, count) => sum + count, 0);
      
      totalProcessed += Math.min(batchSize * maxConcurrent, fundIds.length - i);
      totalSuccessful += batchSuccessful;
      
      const elapsed = (Date.now() - startTime) / 1000;
      const rate = totalProcessed / elapsed;
      const eta = (fundIds.length - totalProcessed) / rate;
      
      console.log(`📊 Progress: ${totalProcessed}/${fundIds.length} funds processed`);
      console.log(`✅ Successful: ${totalSuccessful} calculations completed`);
      console.log(`⚡ Rate: ${rate.toFixed(1)} funds/sec, ETA: ${eta.toFixed(0)}s`);
      console.log('');
      
      // Brief pause to prevent overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Final verification and summary
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
    
    const final = finalCheck.rows[0];
    const completionTime = (Date.now() - startTime) / 1000;
    
    console.log('🎯 High-Performance Advanced Ratios Engine Complete');
    console.log(`⏱️  Total execution time: ${completionTime.toFixed(1)} seconds`);
    console.log(`📈 Funds processed: ${totalProcessed}`);
    console.log(`✅ Successful calculations: ${totalSuccessful}`);
    console.log('');
    console.log('📊 Final Coverage Summary:');
    console.log(`Volatility: ${final.volatility_complete}/${final.total_eligible} funds`);
    console.log(`Sharpe Ratio: ${final.sharpe_complete}/${final.total_eligible} funds`);
    console.log(`Max Drawdown: ${final.drawdown_complete}/${final.total_eligible} funds`);
    console.log(`3Y Returns: ${final.returns_3y_complete}/${final.total_eligible} funds`);
    console.log(`5Y Returns: ${final.returns_5y_complete}/${final.total_eligible} funds`);
    console.log('');
    console.log('🔒 All calculations completed using authentic market data only');
    
  } catch (error) {
    console.error('❌ Error in high-performance engine:', error);
  } finally {
    await pool.end();
  }
}

// Execute the high-performance engine
highPerformanceAdvancedRatiosEngine();