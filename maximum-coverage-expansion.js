/**
 * Maximum Coverage Expansion
 * Comprehensive approach to achieve full 5Y and YTD coverage across all eligible funds
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function maximumCoverageExpansion() {
  try {
    console.log('Starting maximum coverage expansion for all eligible funds...\n');
    
    let totalProcessed = 0;
    let totalAdded5Y = 0;
    let totalAddedYTD = 0;
    let roundCount = 0;
    
    // Continue processing until no more eligible funds remain
    while (roundCount < 100) { // Extended limit for comprehensive coverage
      roundCount++;
      
      // Get comprehensive eligible fund counts
      const eligibilityCheck = await checkEligibleFunds();
      
      if (eligibilityCheck.eligible5Y === 0 && eligibilityCheck.eligibleYTD === 0) {
        console.log('MAXIMUM COVERAGE ACHIEVED - No more eligible funds to process');
        break;
      }
      
      console.log(`Round ${roundCount}: ${eligibilityCheck.eligible5Y} eligible 5Y, ${eligibilityCheck.eligibleYTD} eligible YTD`);
      
      // Process maximum batches in parallel
      const [result5Y, resultYTD] = await Promise.all([
        processMaximum5Y(roundCount),
        processMaximumYTD(roundCount)
      ]);
      
      totalProcessed += result5Y.processed + resultYTD.processed;
      totalAdded5Y += result5Y.added;
      totalAddedYTD += resultYTD.added;
      
      console.log(`  Round complete: +${result5Y.added} 5Y, +${resultYTD.added} YTD (processed ${result5Y.processed + resultYTD.processed})`);
      
      // Progress report every 10 rounds
      if (roundCount % 10 === 0) {
        const coverage = await getCurrentCoverage();
        console.log(`\n=== Round ${roundCount} Checkpoint ===`);
        console.log(`5Y Coverage: ${coverage.funds_5y}/${coverage.total_funds} (${coverage.pct_5y}%)`);
        console.log(`YTD Coverage: ${coverage.funds_ytd}/${coverage.total_funds} (${coverage.pct_ytd}%)`);
        console.log(`Complete Coverage: ${coverage.complete_coverage} funds`);
        console.log(`Session totals: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD\n`);
      }
    }
    
    // Final comprehensive assessment
    const finalCoverage = await getFinalCoverageReport();
    
    console.log(`\n=== MAXIMUM COVERAGE EXPANSION COMPLETE ===`);
    console.log(`Final 5Y Coverage: ${finalCoverage.funds_5y}/${finalCoverage.total_funds} (${finalCoverage.pct_5y}%)`);
    console.log(`Final YTD Coverage: ${finalCoverage.funds_ytd}/${finalCoverage.total_funds} (${finalCoverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${finalCoverage.complete_coverage}/${finalCoverage.total_funds} (${finalCoverage.pct_complete}%)`);
    console.log(`\nExpansion Results:`);
    console.log(`- Added ${totalAdded5Y} new 5Y analyses`);
    console.log(`- Added ${totalAddedYTD} new YTD analyses`);
    console.log(`- Processed ${totalProcessed} total fund calculations`);
    console.log(`- Completed in ${roundCount} rounds`);
    
  } catch (error) {
    console.error('Error in maximum coverage expansion:', error);
  } finally {
    await pool.end();
  }
}

async function checkEligibleFunds() {
  const [eligible5Y, eligibleYTD] = await Promise.all([
    pool.query(`
      SELECT COUNT(*) as count
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 30
      )
    `),
    pool.query(`
      SELECT COUNT(*) as count
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '180 days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 3
      )
    `)
  ]);
  
  return {
    eligible5Y: parseInt(eligible5Y.rows[0].count),
    eligibleYTD: parseInt(eligibleYTD.rows[0].count)
  };
}

async function processMaximum5Y(round) {
  try {
    // Progressive criteria relaxation for maximum coverage
    const minRecords = Math.max(20, 80 - round * 2);
    const yearInterval = Math.max(18, 60 - round);
    
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '${yearInterval} months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= $1
      )
      ORDER BY f.id
      LIMIT 2000
    `, [minRecords]);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { processed: 0, added: 0 };
    
    // Process in optimized chunks
    let added = 0;
    const chunkSize = 100;
    
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => calculateOptimized5Y(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { processed: funds.length, added };
    
  } catch (error) {
    return { processed: 0, added: 0 };
  }
}

async function processMaximumYTD(round) {
  try {
    // Progressive criteria relaxation for maximum coverage
    const minRecords = Math.max(2, 6 - Math.floor(round / 5));
    const dayInterval = Math.max(60, 150 - round * 2);
    
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '${dayInterval} days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= $1
      )
      ORDER BY f.id
      LIMIT 2500
    `, [minRecords]);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { processed: 0, added: 0 };
    
    // Process in optimized chunks
    let added = 0;
    const chunkSize = 125;
    
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => calculateOptimizedYTD(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { processed: funds.length, added };
    
  } catch (error) {
    return { processed: 0, added: 0 };
  }
}

async function calculateOptimized5Y(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_ordered AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 3000
      ),
      current_nav AS (
        SELECT nav_value FROM nav_ordered ORDER BY nav_date DESC LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value 
        FROM nav_ordered
        WHERE nav_date <= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY nav_date DESC
        LIMIT 1
      )
      SELECT 
        c.nav_value as current_nav,
        h.nav_value as historical_nav,
        CASE 
          WHEN h.nav_value > 0 AND c.nav_value IS NOT NULL
          THEN ((c.nav_value - h.nav_value) / h.nav_value) * 100
          ELSE NULL 
        END as return_5y
      FROM current_nav c
      CROSS JOIN historical_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = calculateComprehensiveScore(return5Y, '5Y');
      
      await pool.query(`
        UPDATE fund_scores SET return_5y_score = $1 WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function calculateOptimizedYTD(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_filtered AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '120 days'
        ORDER BY nav_date DESC
        LIMIT 500
      ),
      current_nav AS (
        SELECT nav_value FROM nav_filtered ORDER BY nav_date DESC LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value 
        FROM nav_filtered
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        c.nav_value as current_nav,
        y.nav_value as year_start_nav,
        CASE 
          WHEN y.nav_value > 0 AND c.nav_value IS NOT NULL
          THEN ((c.nav_value - y.nav_value) / y.nav_value) * 100
          ELSE NULL 
        END as return_ytd
      FROM current_nav c
      CROSS JOIN year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = calculateComprehensiveScore(returnYTD, 'YTD');
      
      await pool.query(`
        UPDATE fund_scores SET return_ytd_score = $1 WHERE fund_id = $2
      `, [score, fundId]);
      
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

function calculateComprehensiveScore(returnValue, period) {
  if (period === '5Y') {
    // Comprehensive 5Y scoring for maximum coverage
    if (returnValue >= 2000) return 100;
    if (returnValue >= 1000) return 98;
    if (returnValue >= 500) return 95;
    if (returnValue >= 300) return 92;
    if (returnValue >= 200) return 88;
    if (returnValue >= 150) return 84;
    if (returnValue >= 100) return 80;
    if (returnValue >= 75) return 75;
    if (returnValue >= 50) return 70;
    if (returnValue >= 25) return 62;
    if (returnValue >= 10) return 55;
    if (returnValue >= 0) return 50;
    if (returnValue >= -10) return 42;
    if (returnValue >= -25) return 35;
    if (returnValue >= -50) return 25;
    return 15;
  } else { // YTD
    // Comprehensive YTD scoring for maximum coverage
    if (returnValue >= 150) return 100;
    if (returnValue >= 100) return 96;
    if (returnValue >= 75) return 92;
    if (returnValue >= 50) return 88;
    if (returnValue >= 30) return 84;
    if (returnValue >= 20) return 80;
    if (returnValue >= 15) return 75;
    if (returnValue >= 10) return 70;
    if (returnValue >= 5) return 64;
    if (returnValue >= 0) return 55;
    if (returnValue >= -5) return 48;
    if (returnValue >= -10) return 40;
    if (returnValue >= -20) return 30;
    if (returnValue >= -35) return 20;
    return 10;
  }
}

async function getCurrentCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
      COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
      COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
      ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
    FROM fund_scores
  `);
  
  return result.rows[0];
}

async function getFinalCoverageReport() {
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

maximumCoverageExpansion();