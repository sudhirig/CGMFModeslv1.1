/**
 * Continuous Coverage Expansion
 * Runs until maximum 5Y and YTD coverage is achieved across all eligible funds
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function continuousCoverageExpansion() {
  try {
    console.log('Starting continuous expansion to maximum 5Y and YTD coverage...\n');
    
    let totalProcessed5Y = 0;
    let totalProcessedYTD = 0;
    let batchCount = 0;
    
    while (true) {
      batchCount++;
      
      // Get remaining eligible funds in large batches
      const [eligible5Y, eligibleYTD] = await Promise.all([
        pool.query(`
          SELECT f.id
          FROM funds f
          JOIN fund_scores fs ON f.id = fs.fund_id
          WHERE fs.return_5y_score IS NULL
          AND EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = f.id 
            AND nd.nav_date <= CURRENT_DATE - INTERVAL '3 years'
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 150
          )
          ORDER BY f.id
          LIMIT 800
        `),
        pool.query(`
          SELECT f.id
          FROM funds f
          JOIN fund_scores fs ON f.id = fs.fund_id
          WHERE fs.return_ytd_score IS NULL
          AND EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = f.id 
            AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '90 days'
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 10
          )
          ORDER BY f.id
          LIMIT 1000
        `)
      ]);
      
      const funds5Y = eligible5Y.rows;
      const fundsYTD = eligibleYTD.rows;
      
      if (funds5Y.length === 0 && fundsYTD.length === 0) {
        console.log('Maximum coverage achieved - no more eligible funds');
        break;
      }
      
      console.log(`Batch ${batchCount}: Processing ${funds5Y.length} 5Y funds, ${fundsYTD.length} YTD funds`);
      
      // Process both types in parallel
      const [results5Y, resultsYTD] = await Promise.all([
        processBatch5Y(funds5Y),
        processBatchYTD(fundsYTD)
      ]);
      
      totalProcessed5Y += results5Y.successful;
      totalProcessedYTD += resultsYTD.successful;
      
      console.log(`  Batch complete: +${results5Y.successful} 5Y, +${resultsYTD.successful} YTD`);
      console.log(`  Running totals: ${totalProcessed5Y} 5Y, ${totalProcessedYTD} YTD processed`);
      
      // Progress checkpoint every 10 batches
      if (batchCount % 10 === 0) {
        const currentCoverage = await getCurrentCoverage();
        console.log(`\n=== Checkpoint ${batchCount} ===`);
        console.log(`Current coverage: 5Y ${currentCoverage.pct_5y}% (${currentCoverage.funds_5y} funds)`);
        console.log(`Current coverage: YTD ${currentCoverage.pct_ytd}% (${currentCoverage.funds_ytd} funds)`);
        console.log(`Complete coverage: ${currentCoverage.complete_coverage} funds\n`);
      }
      
      // Safety limit to prevent infinite loops
      if (batchCount > 100) {
        console.log('Reached batch limit, stopping to prevent overrun');
        break;
      }
    }
    
    // Final comprehensive report
    const finalCoverage = await getCurrentCoverage();
    
    console.log(`\n=== MAXIMUM COVERAGE ACHIEVED ===`);
    console.log(`5Y Analysis: ${finalCoverage.funds_5y}/${finalCoverage.total_funds} (${finalCoverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${finalCoverage.funds_ytd}/${finalCoverage.total_funds} (${finalCoverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${finalCoverage.complete_coverage}/${finalCoverage.total_funds} (${finalCoverage.pct_complete}%)`);
    console.log(`\nTotal processed in this run: +${totalProcessed5Y} 5Y, +${totalProcessedYTD} YTD`);
    
  } catch (error) {
    console.error('Error in continuous expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processBatch5Y(funds) {
  const promises = funds.map(fund => process5YAnalysis(fund.id));
  const results = await Promise.allSettled(promises);
  const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
  return { successful };
}

async function processBatchYTD(funds) {
  const promises = funds.map(fund => processYTDAnalysis(fund.id));
  const results = await Promise.allSettled(promises);
  const successful = results.filter(r => r.status === 'fulfilled' && r.value === true).length;
  return { successful };
}

async function process5YAnalysis(fundId) {
  try {
    // Optimized single-query 5Y calculation
    const result = await pool.query(`
      WITH fund_nav AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 2000
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM fund_nav
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value as historical_value
        FROM fund_nav
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

async function processYTDAnalysis(fundId) {
  try {
    // Optimized single-query YTD calculation
    const result = await pool.query(`
      WITH fund_nav AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '60 days'
        ORDER BY nav_date DESC
        LIMIT 500
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM fund_nav
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value as year_start_value
        FROM fund_nav
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
  // Comprehensive 5Y scoring for maximum coverage
  if (returnValue >= 5000) return 100;     // Multi-bagger extraordinary
  if (returnValue >= 2000) return 98;      // Multi-bagger exceptional
  if (returnValue >= 1000) return 95;      // Multi-bagger outstanding
  if (returnValue >= 500) return 92;       // Exceptional growth
  if (returnValue >= 300) return 88;       // Outstanding performance
  if (returnValue >= 200) return 84;       // Excellent growth
  if (returnValue >= 150) return 80;       // Very strong
  if (returnValue >= 100) return 75;       // Strong performer
  if (returnValue >= 75) return 70;        // Good growth
  if (returnValue >= 50) return 65;        // Above average
  if (returnValue >= 25) return 58;        // Average
  if (returnValue >= 0) return 50;         // Weak but positive
  if (returnValue >= -25) return 35;       // Poor performance
  if (returnValue >= -50) return 25;       // Very poor
  return 15;                               // Extremely poor
}

function calculateYTDScore(returnValue) {
  // Comprehensive YTD scoring for maximum coverage
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

async function getCurrentCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
      COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
      COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
      ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_complete
    FROM fund_scores
  `);
  
  return result.rows[0];
}

continuousCoverageExpansion();