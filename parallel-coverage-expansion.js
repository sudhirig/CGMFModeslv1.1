/**
 * Parallel Coverage Expansion
 * Optimized approach to process maximum funds simultaneously
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function parallelCoverageExpansion() {
  try {
    console.log('Starting parallel 5Y and YTD coverage expansion...\n');
    
    // Process both 5Y and YTD in parallel for maximum efficiency
    const [results5Y, resultsYTD] = await Promise.all([
      processMassive5YExpansion(),
      processMassiveYTDExpansion()
    ]);
    
    // Get final coverage statistics
    const finalStats = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const stats = finalStats.rows[0];
    
    console.log(`\n=== PARALLEL EXPANSION COMPLETE ===`);
    console.log(`5Y Coverage: ${stats.funds_5y}/${stats.total_funds} (${stats.pct_5y}%)`);
    console.log(`YTD Coverage: ${stats.funds_ytd}/${stats.total_funds} (${stats.pct_ytd}%)`);
    console.log(`Added: +${results5Y.processed} 5Y, +${resultsYTD.processed} YTD scores`);
    
  } catch (error) {
    console.error('Error in parallel expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processMassive5YExpansion() {
  let totalProcessed = 0;
  let batchCount = 0;
  
  while (true) {
    const funds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '4 years'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 250
      )
      ORDER BY f.id
      LIMIT 300
    `);
    
    if (funds.rows.length === 0) break;
    
    batchCount++;
    console.log(`5Y Batch ${batchCount}: Processing ${funds.rows.length} funds`);
    
    const batchPromises = funds.rows.map(fund => process5YFund(fund.id));
    const results = await Promise.allSettled(batchPromises);
    
    const successful = results.filter(r => r.status === 'fulfilled' && r.value).length;
    totalProcessed += successful;
    
    console.log(`  Completed: +${successful} 5Y scores (Total: ${totalProcessed})`);
    
    if (batchCount % 5 === 0) {
      console.log(`  === 5Y Progress Checkpoint: ${totalProcessed} funds ===`);
    }
  }
  
  return { processed: totalProcessed };
}

async function processMassiveYTDExpansion() {
  let totalProcessed = 0;
  let batchCount = 0;
  
  while (true) {
    const funds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '45 days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 30
      )
      ORDER BY f.id
      LIMIT 400
    `);
    
    if (funds.rows.length === 0) break;
    
    batchCount++;
    console.log(`YTD Batch ${batchCount}: Processing ${funds.rows.length} funds`);
    
    const batchPromises = funds.rows.map(fund => processYTDFund(fund.id));
    const results = await Promise.allSettled(batchPromises);
    
    const successful = results.filter(r => r.status === 'fulfilled' && r.value).length;
    totalProcessed += successful;
    
    console.log(`  Completed: +${successful} YTD scores (Total: ${totalProcessed})`);
    
    if (batchCount % 6 === 0) {
      console.log(`  === YTD Progress Checkpoint: ${totalProcessed} funds ===`);
    }
  }
  
  return { processed: totalProcessed };
}

async function process5YFund(fundId) {
  try {
    // Streamlined 5Y calculation
    const result = await pool.query(`
      WITH current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC 
        LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value as historical_value
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date <= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY nav_date DESC 
        LIMIT 1
      )
      SELECT 
        c.current_value,
        h.historical_value,
        CASE 
          WHEN h.historical_value IS NOT NULL AND h.historical_value > 0 
          THEN ((c.current_value - h.historical_value) / h.historical_value) * 100
          ELSE NULL 
        END as return_5y
      FROM current_nav c
      CROSS JOIN historical_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = calculate5YScore(return5Y);
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_5y_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function processYTDFund(fundId) {
  try {
    // Streamlined YTD calculation
    const result = await pool.query(`
      WITH current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC 
        LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value as year_start_value
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '15 days'
        AND nav_date <= DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '15 days'
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        c.current_value,
        y.year_start_value,
        CASE 
          WHEN y.year_start_value IS NOT NULL AND y.year_start_value > 0 
          THEN ((c.current_value - y.year_start_value) / y.year_start_value) * 100
          ELSE NULL 
        END as return_ytd
      FROM current_nav c
      CROSS JOIN year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = calculateYTDScore(returnYTD);
      
      await pool.query(`
        UPDATE fund_scores 
        SET return_ytd_score = $1
        WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

function calculate5YScore(returnValue) {
  if (returnValue >= 1000) return 100;
  if (returnValue >= 500) return 95;
  if (returnValue >= 300) return 90;
  if (returnValue >= 200) return 85;
  if (returnValue >= 150) return 80;
  if (returnValue >= 100) return 75;
  if (returnValue >= 75) return 70;
  if (returnValue >= 50) return 65;
  if (returnValue >= 25) return 55;
  if (returnValue >= 0) return 45;
  if (returnValue >= -25) return 30;
  return 15;
}

function calculateYTDScore(returnValue) {
  if (returnValue >= 100) return 100;
  if (returnValue >= 60) return 95;
  if (returnValue >= 40) return 90;
  if (returnValue >= 25) return 85;
  if (returnValue >= 15) return 80;
  if (returnValue >= 10) return 70;
  if (returnValue >= 5) return 60;
  if (returnValue >= 0) return 50;
  if (returnValue >= -10) return 35;
  return 20;
}

parallelCoverageExpansion();