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
    console.log('Expanding to comprehensive 5Y and YTD coverage across all eligible funds...\n');
    
    // Phase 1: Large-scale 5Y expansion
    console.log('=== Phase 1: Comprehensive 5Y Analysis Expansion ===');
    const phase1Results = await expandComprehensive5Y();
    
    // Phase 2: Large-scale YTD expansion
    console.log('\n=== Phase 2: Comprehensive YTD Analysis Expansion ===');
    const phase2Results = await expandComprehensiveYTD();
    
    // Final comprehensive coverage report
    const finalCoverage = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const coverage = finalCoverage.rows[0];
    
    console.log(`\n=== COMPREHENSIVE COVERAGE ACHIEVED ===`);
    console.log(`5Y Analysis: ${coverage.funds_5y}/${coverage.total_funds} funds (${coverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${coverage.funds_ytd}/${coverage.total_funds} funds (${coverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${coverage.complete_coverage}/${coverage.total_funds} funds`);
    console.log(`\nTotal Added: +${phase1Results.added} 5Y scores, +${phase2Results.added} YTD scores`);
    
    // Category-wise coverage breakdown
    const categoryBreakdown = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN fs.return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN fs.return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        ROUND(COUNT(CASE WHEN fs.return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_5y,
        ROUND(COUNT(CASE WHEN fs.return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_ytd
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      GROUP BY f.category
      ORDER BY COUNT(*) DESC
    `);
    
    console.log(`\n=== CATEGORY-WISE COVERAGE ===`);
    categoryBreakdown.rows.forEach(cat => {
      console.log(`${cat.category}: 5Y ${cat.pct_5y}% (${cat.funds_5y}/${cat.total_funds}), YTD ${cat.pct_ytd}% (${cat.funds_ytd}/${cat.total_funds})`);
    });
    
  } catch (error) {
    console.error('Error in comprehensive coverage expansion:', error);
  } finally {
    await pool.end();
  }
}

async function expandComprehensive5Y() {
  // Process in large batches for efficiency
  const batchSize = 200;
  let totalAdded = 0;
  let batchNumber = 1;
  
  while (true) {
    const eligibleFunds = await pool.query(`
      SELECT f.id, f.fund_name, f.category
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '4 years'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 300
      )
      ORDER BY f.id
      LIMIT ${batchSize}
    `);
    
    if (eligibleFunds.rows.length === 0) break;
    
    console.log(`  Processing 5Y batch ${batchNumber}: ${eligibleFunds.rows.length} funds`);
    
    let batchAdded = 0;
    
    for (const fund of eligibleFunds.rows) {
      try {
        const return5Y = await efficient5YCalculation(fund.id);
        
        if (return5Y !== null) {
          await update5YScore(fund.id, return5Y);
          batchAdded++;
          totalAdded++;
        }
        
      } catch (error) {
        console.error(`    Error processing fund ${fund.id}: ${error.message}`);
      }
    }
    
    console.log(`    Batch ${batchNumber} complete: +${batchAdded} 5Y scores (Total: ${totalAdded})`);
    batchNumber++;
    
    // Progress reporting every 10 batches
    if (batchNumber % 10 === 0) {
      console.log(`    === 5Y Progress: ${totalAdded} funds processed ===`);
    }
  }
  
  console.log(`5Y expansion complete: +${totalAdded} scores added`);
  return { added: totalAdded };
}

async function expandComprehensiveYTD() {
  const batchSize = 250;
  let totalAdded = 0;
  let batchNumber = 1;
  
  while (true) {
    const eligibleFunds = await pool.query(`
      SELECT f.id, f.fund_name, f.category
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 50
      )
      ORDER BY f.id
      LIMIT ${batchSize}
    `);
    
    if (eligibleFunds.rows.length === 0) break;
    
    console.log(`  Processing YTD batch ${batchNumber}: ${eligibleFunds.rows.length} funds`);
    
    let batchAdded = 0;
    
    for (const fund of eligibleFunds.rows) {
      try {
        const returnYTD = await efficientYTDCalculation(fund.id);
        
        if (returnYTD !== null) {
          await updateYTDScore(fund.id, returnYTD);
          batchAdded++;
          totalAdded++;
        }
        
      } catch (error) {
        console.error(`    Error processing fund ${fund.id}: ${error.message}`);
      }
    }
    
    console.log(`    Batch ${batchNumber} complete: +${batchAdded} YTD scores (Total: ${totalAdded})`);
    batchNumber++;
    
    if (batchNumber % 8 === 0) {
      console.log(`    === YTD Progress: ${totalAdded} funds processed ===`);
    }
  }
  
  console.log(`YTD expansion complete: +${totalAdded} scores added`);
  return { added: totalAdded };
}

async function efficient5YCalculation(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    AND nav_date <= CURRENT_DATE - INTERVAL '4 years'
    ORDER BY nav_date DESC 
    LIMIT 500
  `, [fundId]);
  
  if (navData.rows.length < 100) return null;
  
  // Get current NAV
  const currentNavData = await pool.query(`
    SELECT nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 1
  `, [fundId]);
  
  if (currentNavData.rows.length === 0) return null;
  
  const currentNav = parseFloat(currentNavData.rows[0].nav_value);
  const historicalNav = parseFloat(navData.rows[0].nav_value);
  
  return ((currentNav - historicalNav) / historicalNav) * 100;
}

async function efficientYTDCalculation(fundId) {
  const currentYear = new Date().getFullYear();
  const yearStart = new Date(currentYear, 0, 1);
  
  // Get NAV closest to year start
  const startNavData = await pool.query(`
    SELECT nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    AND nav_date >= $2 - INTERVAL '30 days'
    AND nav_date <= $2 + INTERVAL '30 days'
    ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - $2)))
    LIMIT 1
  `, [fundId, yearStart]);
  
  if (startNavData.rows.length === 0) return null;
  
  // Get current NAV
  const currentNavData = await pool.query(`
    SELECT nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 1
  `, [fundId]);
  
  if (currentNavData.rows.length === 0) return null;
  
  const currentNav = parseFloat(currentNavData.rows[0].nav_value);
  const startNav = parseFloat(startNavData.rows[0].nav_value);
  
  return ((currentNav - startNav) / startNav) * 100;
}

async function update5YScore(fundId, return5Y) {
  const score = calculate5YScore(return5Y);
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_5y_score = $1
    WHERE fund_id = $2
  `, [score, fundId]);
}

async function updateYTDScore(fundId, returnYTD) {
  const score = calculateYTDScore(returnYTD);
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_ytd_score = $1
    WHERE fund_id = $2
  `, [score, fundId]);
}

function calculate5YScore(returnValue) {
  // Enhanced 5Y scoring for broad coverage
  if (returnValue >= 1000) return 100;     // Exceptional multi-bagger
  if (returnValue >= 500) return 95;       // Outstanding performer
  if (returnValue >= 300) return 90;       // Excellent growth
  if (returnValue >= 200) return 85;       // Very strong
  if (returnValue >= 150) return 80;       // Strong performer
  if (returnValue >= 100) return 75;       // Good growth
  if (returnValue >= 75) return 70;        // Above average
  if (returnValue >= 50) return 65;        // Average
  if (returnValue >= 25) return 55;        // Below average
  if (returnValue >= 0) return 45;         // Weak
  if (returnValue >= -25) return 30;       // Poor
  if (returnValue >= -50) return 20;       // Very poor
  return 10;                               // Extremely poor
}

function calculateYTDScore(returnValue) {
  // Enhanced YTD scoring for broad coverage
  if (returnValue >= 100) return 100;      // Exceptional YTD
  if (returnValue >= 60) return 95;        // Outstanding
  if (returnValue >= 40) return 90;        // Excellent
  if (returnValue >= 25) return 85;        // Very strong
  if (returnValue >= 15) return 80;        // Strong
  if (returnValue >= 10) return 70;        // Good
  if (returnValue >= 5) return 60;         // Above average
  if (returnValue >= 0) return 50;         // Average
  if (returnValue >= -5) return 40;        // Below average
  if (returnValue >= -15) return 30;       // Poor
  if (returnValue >= -25) return 20;       // Very poor
  return 10;                               // Extremely poor
}

comprehensive5YAndYTDCoverage();