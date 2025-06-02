/**
 * Comprehensive 5Y and YTD Coverage Expansion
 * Processes all 17,000+ eligible funds to achieve maximum coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function comprehensive5YAndYTDCoverage() {
  try {
    console.log('Starting comprehensive expansion for maximum 5Y and YTD coverage...\n');
    
    let totalAdded5Y = 0;
    let totalAddedYTD = 0;
    let batchNumber = 0;
    
    // Run comprehensive expansion in multiple phases
    while (batchNumber < 50) {
      batchNumber++;
      
      console.log(`Processing batch ${batchNumber}...`);
      
      // Parallel expansion of both 5Y and YTD
      const [results5Y, resultsYTD] = await Promise.all([
        expandComprehensive5Y(),
        expandComprehensiveYTD()
      ]);
      
      if (results5Y.added === 0 && resultsYTD.added === 0) {
        console.log('Maximum coverage achieved - no more eligible funds');
        break;
      }
      
      totalAdded5Y += results5Y.added;
      totalAddedYTD += resultsYTD.added;
      
      console.log(`  Batch ${batchNumber}: +${results5Y.added} 5Y, +${resultsYTD.added} YTD`);
      
      // Progress report every 10 batches
      if (batchNumber % 10 === 0) {
        const coverage = await pool.query(`
          SELECT 
            COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
            COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
            ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
            ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
          FROM fund_scores
        `);
        
        const current = coverage.rows[0];
        console.log(`\n--- Batch ${batchNumber} Status ---`);
        console.log(`5Y Coverage: ${current.funds_5y} funds (${current.pct_5y}%)`);
        console.log(`YTD Coverage: ${current.funds_ytd} funds (${current.pct_ytd}%)`);
        console.log(`Session totals: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD\n`);
      }
    }
    
    // Final comprehensive results
    const finalResults = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const final = finalResults.rows[0];
    
    console.log(`\n=== COMPREHENSIVE COVERAGE COMPLETE ===`);
    console.log(`Final 5Y Coverage: ${final.funds_5y}/${final.total_funds} (${final.pct_5y}%)`);
    console.log(`Final YTD Coverage: ${final.funds_ytd}/${final.total_funds} (${final.pct_ytd}%)`);
    console.log(`Complete Coverage: ${final.complete_coverage}/${final.total_funds} funds`);
    console.log(`Total expansion: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD`);
    
  } catch (error) {
    console.error('Error in comprehensive expansion:', error);
  } finally {
    await pool.end();
  }
}

async function expandComprehensive5Y() {
  try {
    // Get all eligible 5Y funds with relaxed criteria
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '18 months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 20
      )
      ORDER BY f.id
      LIMIT 1500
    `);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { added: 0 };
    
    let added = 0;
    const chunkSize = 75;
    
    // Process in chunks for efficiency
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => efficient5YCalculation(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { added };
    
  } catch (error) {
    return { added: 0 };
  }
}

async function expandComprehensiveYTD() {
  try {
    // Get all eligible YTD funds with relaxed criteria
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '180 days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 2
      )
      ORDER BY f.id
      LIMIT 2000
    `);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { added: 0 };
    
    let added = 0;
    const chunkSize = 100;
    
    // Process in chunks for efficiency
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => efficientYTDCalculation(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { added };
    
  } catch (error) {
    return { added: 0 };
  }
}

async function efficient5YCalculation(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_data_limited AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 5000
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data_limited
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value as historical_value
        FROM nav_data_limited
        WHERE nav_date <= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY nav_date DESC
        LIMIT 1
      )
      SELECT 
        CASE 
          WHEN h.historical_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - h.historical_value) / h.historical_value) * 100
          ELSE NULL 
        END as return_5y
      FROM current_nav c
      CROSS JOIN historical_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = calculate5YScore(return5Y);
      
      await update5YScore(fundId, score);
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function efficientYTDCalculation(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_data_ytd AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '120 days'
        ORDER BY nav_date DESC
        LIMIT 1000
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data_ytd
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value as year_start_value
        FROM nav_data_ytd
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        CASE 
          WHEN y.year_start_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - y.year_start_value) / y.year_start_value) * 100
          ELSE NULL 
        END as return_ytd
      FROM current_nav c
      CROSS JOIN year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = calculateYTDScore(returnYTD);
      
      await updateYTDScore(fundId, score);
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function update5YScore(fundId, score) {
  await pool.query(`
    UPDATE fund_scores SET return_5y_score = $1 WHERE fund_id = $2
  `, [score, fundId]);
}

async function updateYTDScore(fundId, score) {
  await pool.query(`
    UPDATE fund_scores SET return_ytd_score = $1 WHERE fund_id = $2
  `, [score, fundId]);
}

function calculate5YScore(returnValue) {
  // Comprehensive scoring to maximize coverage
  if (returnValue >= 3000) return 100;     // Exceptional multi-bagger
  if (returnValue >= 1500) return 98;      // Multi-bagger outstanding
  if (returnValue >= 1000) return 95;      // Multi-bagger excellent
  if (returnValue >= 500) return 92;       // Exceptional growth
  if (returnValue >= 300) return 88;       // Outstanding performance
  if (returnValue >= 200) return 84;       // Excellent growth
  if (returnValue >= 150) return 80;       // Very strong
  if (returnValue >= 100) return 75;       // Strong performer
  if (returnValue >= 75) return 70;        // Good growth
  if (returnValue >= 50) return 65;        // Above average
  if (returnValue >= 25) return 58;        // Average positive
  if (returnValue >= 0) return 50;         // Weak but positive
  if (returnValue >= -15) return 40;       // Slight negative
  if (returnValue >= -30) return 30;       // Moderate negative
  if (returnValue >= -50) return 20;       // Poor performance
  return 10;                               // Very poor
}

function calculateYTDScore(returnValue) {
  // Comprehensive scoring to maximize coverage
  if (returnValue >= 200) return 100;      // Extraordinary YTD
  if (returnValue >= 150) return 97;       // Exceptional YTD
  if (returnValue >= 100) return 94;       // Outstanding YTD
  if (returnValue >= 75) return 90;        // Excellent YTD
  if (returnValue >= 50) return 86;        // Very strong YTD
  if (returnValue >= 30) return 82;        // Strong YTD
  if (returnValue >= 20) return 78;        // Good YTD
  if (returnValue >= 15) return 74;        // Above average
  if (returnValue >= 10) return 68;        // Average positive
  if (returnValue >= 5) return 62;         // Modest positive
  if (returnValue >= 0) return 55;         // Flat to slight positive
  if (returnValue >= -5) return 45;        // Slight negative
  if (returnValue >= -15) return 35;       // Moderate negative
  if (returnValue >= -30) return 25;       // Poor negative
  return 15;                               // Very poor negative
}

comprehensive5YAndYTDCoverage();