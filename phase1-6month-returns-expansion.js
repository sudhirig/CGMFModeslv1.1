/**
 * Phase 1.1: 6-Month Returns Expansion
 * Target: Process 25,121 funds missing 6-month returns
 * Requirement: Funds with 180+ days of historical NAV data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function expand6MonthReturns() {
  try {
    console.log('Starting Phase 1.1: 6-Month Returns Expansion...\n');
    
    let totalProcessed = 0;
    let totalAdded = 0;
    let batchNumber = 0;
    
    while (batchNumber < 50) {
      batchNumber++;
      
      console.log(`Processing 6-month returns batch ${batchNumber}...`);
      
      // Get funds without 6-month scores that have sufficient historical data
      const eligibleFunds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE fs.return_6m_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '180 days'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 100
        )
        ORDER BY fs.fund_id
        LIMIT 1500
      `);
      
      const funds = eligibleFunds.rows;
      if (funds.length === 0) {
        console.log('All eligible funds processed for 6-month returns');
        break;
      }
      
      console.log(`  Processing ${funds.length} funds for 6-month returns...`);
      
      // Process in chunks
      let batchAdded = 0;
      const chunkSize = 200;
      
      for (let i = 0; i < funds.length; i += chunkSize) {
        const chunk = funds.slice(i, i + chunkSize);
        const promises = chunk.map(fund => calculate6MonthReturn(fund.fund_id));
        const results = await Promise.allSettled(promises);
        batchAdded += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      }
      
      totalProcessed += funds.length;
      totalAdded += batchAdded;
      
      console.log(`  Batch ${batchNumber}: +${batchAdded} 6-month returns calculated`);
      
      // Progress report every 10 batches
      if (batchNumber % 10 === 0) {
        const coverage = await get6MonthCoverage();
        console.log(`\n--- Batch ${batchNumber} Progress ---`);
        console.log(`6-Month Coverage: ${coverage.with_6m}/${coverage.total} (${coverage.percentage}%)`);
        console.log(`Session totals: +${totalAdded} 6-month returns calculated\n`);
      }
    }
    
    // Final results
    const finalCoverage = await get6MonthCoverage();
    
    console.log(`\n=== PHASE 1.1 COMPLETE: 6-MONTH RETURNS ===`);
    console.log(`Final Coverage: ${finalCoverage.with_6m}/${finalCoverage.total} (${finalCoverage.percentage}%)`);
    console.log(`Total processed: ${totalProcessed} funds`);
    console.log(`Successfully added: ${totalAdded} 6-month return scores`);
    
    return {
      success: true,
      totalAdded,
      finalCoverage: finalCoverage.percentage
    };
    
  } catch (error) {
    console.error('Error in 6-month returns expansion:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculate6MonthReturn(fundId) {
  try {
    // Calculate 6-month return using precise NAV data
    const result = await pool.query(`
      WITH nav_data_6m AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 250
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data_6m
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      six_month_nav AS (
        SELECT nav_value as historical_value
        FROM nav_data_6m
        WHERE nav_date <= CURRENT_DATE - INTERVAL '6 months'
        ORDER BY nav_date DESC
        LIMIT 1
      )
      SELECT 
        CASE 
          WHEN h.historical_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - h.historical_value) / h.historical_value) * 100
          ELSE NULL 
        END as return_6m
      FROM current_nav c
      CROSS JOIN six_month_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_6m !== null) {
      const return6M = parseFloat(result.rows[0].return_6m);
      const score = calculate6MonthScore(return6M);
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_6m_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

function calculate6MonthScore(returnValue) {
  // Comprehensive 6-month scoring system
  if (returnValue >= 100) return 100;      // Exceptional 6-month performance
  if (returnValue >= 75) return 95;        // Outstanding 6-month growth
  if (returnValue >= 50) return 90;        // Excellent 6-month performance
  if (returnValue >= 30) return 85;        // Very strong 6-month
  if (returnValue >= 20) return 78;        // Strong 6-month performance
  if (returnValue >= 15) return 72;        // Good 6-month growth
  if (returnValue >= 10) return 65;        // Above average 6-month
  if (returnValue >= 5) return 58;         // Average positive 6-month
  if (returnValue >= 0) return 50;         // Flat to slight positive
  if (returnValue >= -5) return 42;        // Slight negative 6-month
  if (returnValue >= -10) return 35;       // Moderate negative 6-month
  if (returnValue >= -20) return 25;       // Poor 6-month performance
  if (returnValue >= -30) return 15;       // Very poor 6-month
  return 10;                               // Extremely poor 6-month
}

async function get6MonthCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) as with_6m,
      ROUND(COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as percentage
    FROM fund_scores
  `);
  
  return result.rows[0];
}

expand6MonthReturns();