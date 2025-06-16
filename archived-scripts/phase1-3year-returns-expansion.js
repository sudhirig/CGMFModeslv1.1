/**
 * Phase 1.2: 3-Year Returns Expansion
 * Target: Process 25,120 funds missing 3-year returns
 * Requirement: Funds with 1,095+ days of historical NAV data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function expand3YearReturns() {
  try {
    console.log('Starting Phase 1.2: 3-Year Returns Expansion...\n');
    
    let totalProcessed = 0;
    let totalAdded = 0;
    let batchNumber = 0;
    
    while (batchNumber < 60) {
      batchNumber++;
      
      console.log(`Processing 3-year returns batch ${batchNumber}...`);
      
      // Get funds without 3-year scores that have sufficient historical data
      const eligibleFunds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE fs.return_3y_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '3 years'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 150
        )
        ORDER BY fs.fund_id
        LIMIT 1200
      `);
      
      const funds = eligibleFunds.rows;
      if (funds.length === 0) {
        console.log('All eligible funds processed for 3-year returns');
        break;
      }
      
      console.log(`  Processing ${funds.length} funds for 3-year returns...`);
      
      // Process in chunks
      let batchAdded = 0;
      const chunkSize = 150;
      
      for (let i = 0; i < funds.length; i += chunkSize) {
        const chunk = funds.slice(i, i + chunkSize);
        const promises = chunk.map(fund => calculate3YearReturn(fund.fund_id));
        const results = await Promise.allSettled(promises);
        batchAdded += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
      }
      
      totalProcessed += funds.length;
      totalAdded += batchAdded;
      
      console.log(`  Batch ${batchNumber}: +${batchAdded} 3-year returns calculated`);
      
      // Progress report every 10 batches
      if (batchNumber % 10 === 0) {
        const coverage = await get3YearCoverage();
        console.log(`\n--- Batch ${batchNumber} Progress ---`);
        console.log(`3-Year Coverage: ${coverage.with_3y}/${coverage.total} (${coverage.percentage}%)`);
        console.log(`Session totals: +${totalAdded} 3-year returns calculated\n`);
      }
    }
    
    // Final results
    const finalCoverage = await get3YearCoverage();
    
    console.log(`\n=== PHASE 1.2 COMPLETE: 3-YEAR RETURNS ===`);
    console.log(`Final Coverage: ${finalCoverage.with_3y}/${finalCoverage.total} (${finalCoverage.percentage}%)`);
    console.log(`Total processed: ${totalProcessed} funds`);
    console.log(`Successfully added: ${totalAdded} 3-year return scores`);
    
    return {
      success: true,
      totalAdded,
      finalCoverage: finalCoverage.percentage
    };
    
  } catch (error) {
    console.error('Error in 3-year returns expansion:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculate3YearReturn(fundId) {
  try {
    // Calculate 3-year return using precise NAV data
    const result = await pool.query(`
      WITH nav_data_3y AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 1500
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data_3y
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      three_year_nav AS (
        SELECT nav_value as historical_value
        FROM nav_data_3y
        WHERE nav_date <= CURRENT_DATE - INTERVAL '3 years'
        ORDER BY nav_date DESC
        LIMIT 1
      )
      SELECT 
        CASE 
          WHEN h.historical_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - h.historical_value) / h.historical_value) * 100
          ELSE NULL 
        END as return_3y
      FROM current_nav c
      CROSS JOIN three_year_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_3y !== null) {
      const return3Y = parseFloat(result.rows[0].return_3y);
      const score = calculate3YearScore(return3Y);
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_3y_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

function calculate3YearScore(returnValue) {
  // Comprehensive 3-year scoring system
  if (returnValue >= 500) return 100;      // Multi-bagger 3-year performance
  if (returnValue >= 300) return 98;       // Exceptional 3-year growth
  if (returnValue >= 200) return 95;       // Outstanding 3-year performance
  if (returnValue >= 150) return 92;       // Excellent 3-year growth
  if (returnValue >= 100) return 88;       // Very strong 3-year performance
  if (returnValue >= 75) return 84;        // Strong 3-year growth
  if (returnValue >= 50) return 78;        // Good 3-year performance
  if (returnValue >= 30) return 72;        // Above average 3-year
  if (returnValue >= 20) return 65;        // Average positive 3-year
  if (returnValue >= 10) return 58;        // Modest 3-year growth
  if (returnValue >= 0) return 50;         // Flat to slight positive
  if (returnValue >= -10) return 42;       // Slight negative 3-year
  if (returnValue >= -25) return 32;       // Moderate negative 3-year
  if (returnValue >= -40) return 22;       // Poor 3-year performance
  if (returnValue >= -60) return 15;       // Very poor 3-year
  return 10;                               // Extremely poor 3-year
}

async function get3YearCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) as with_3y,
      ROUND(COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as percentage
    FROM fund_scores
  `);
  
  return result.rows[0];
}

expand3YearReturns();