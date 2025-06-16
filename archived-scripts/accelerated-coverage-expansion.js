/**
 * Accelerated Coverage Expansion
 * High-performance approach to achieve maximum 5Y and YTD coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function acceleratedCoverageExpansion() {
  try {
    console.log('Starting accelerated expansion for maximum 5Y and YTD coverage...\n');
    
    let totalBatches = 0;
    let totalAdded5Y = 0;
    let totalAddedYTD = 0;
    
    // Continue processing until no more eligible funds
    while (totalBatches < 50) { // Safety limit
      totalBatches++;
      
      // Process large batches efficiently
      const batchResults = await processLargeBatch(totalBatches);
      
      if (batchResults.funds5Y === 0 && batchResults.fundsYTD === 0) {
        console.log('No more eligible funds found - maximum coverage achieved');
        break;
      }
      
      totalAdded5Y += batchResults.added5Y;
      totalAddedYTD += batchResults.addedYTD;
      
      console.log(`Batch ${totalBatches}: +${batchResults.added5Y} 5Y, +${batchResults.addedYTD} YTD`);
      
      // Progress report every 5 batches
      if (totalBatches % 5 === 0) {
        const currentCoverage = await pool.query(`
          SELECT 
            COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
            COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
            ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
            ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
          FROM fund_scores
        `);
        
        const coverage = currentCoverage.rows[0];
        console.log(`\n--- Progress Update ---`);
        console.log(`5Y Coverage: ${coverage.funds_5y} funds (${coverage.pct_5y}%)`);
        console.log(`YTD Coverage: ${coverage.funds_ytd} funds (${coverage.pct_ytd}%)`);
        console.log(`Total added this run: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD\n`);
      }
    }
    
    // Final comprehensive assessment
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
    
    console.log(`\n=== ACCELERATED EXPANSION COMPLETE ===`);
    console.log(`5Y Analysis: ${final.funds_5y}/${final.total_funds} funds (${final.pct_5y}%)`);
    console.log(`YTD Analysis: ${final.funds_ytd}/${final.total_funds} funds (${final.pct_ytd}%)`);
    console.log(`Complete Coverage: ${final.complete_coverage}/${final.total_funds} funds`);
    console.log(`Session total: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD in ${totalBatches} batches`);
    
  } catch (error) {
    console.error('Error in accelerated expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processLargeBatch(batchNumber) {
  // Get remaining eligible funds with relaxed criteria for maximum coverage
  const [eligible5Y, eligibleYTD] = await Promise.all([
    pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '2 years 6 months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 100
      )
      ORDER BY f.id
      LIMIT 1200
    `),
    pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '120 days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 5
      )
      ORDER BY f.id
      LIMIT 1500
    `)
  ]);
  
  const funds5Y = eligible5Y.rows;
  const fundsYTD = eligibleYTD.rows;
  
  console.log(`  Processing ${funds5Y.length} 5Y funds, ${fundsYTD.length} YTD funds`);
  
  let added5Y = 0;
  let addedYTD = 0;
  
  // Process 5Y funds in chunks
  for (let i = 0; i < funds5Y.length; i += 100) {
    const chunk = funds5Y.slice(i, i + 100);
    const chunkPromises = chunk.map(fund => processQuick5Y(fund.id));
    const results = await Promise.allSettled(chunkPromises);
    added5Y += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
  }
  
  // Process YTD funds in chunks
  for (let i = 0; i < fundsYTD.length; i += 150) {
    const chunk = fundsYTD.slice(i, i + 150);
    const chunkPromises = chunk.map(fund => processQuickYTD(fund.id));
    const results = await Promise.allSettled(chunkPromises);
    addedYTD += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
  }
  
  return {
    funds5Y: funds5Y.length,
    fundsYTD: fundsYTD.length,
    added5Y,
    addedYTD
  };
}

async function processQuick5Y(fundId) {
  try {
    // Fast 5Y calculation using simplified approach
    const result = await pool.query(`
      WITH recent_nav AS (
        SELECT nav_value FROM nav_data 
        WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1
      ),
      old_nav AS (
        SELECT nav_value FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date <= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY nav_date DESC LIMIT 1
      )
      SELECT 
        r.nav_value as current_nav,
        o.nav_value as old_nav,
        CASE WHEN o.nav_value > 0 
             THEN ((r.nav_value - o.nav_value) / o.nav_value) * 100
             ELSE NULL END as return_5y
      FROM recent_nav r, old_nav o
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = quickScore5Y(return5Y);
      
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

async function processQuickYTD(fundId) {
  try {
    // Fast YTD calculation using simplified approach
    const result = await pool.query(`
      WITH recent_nav AS (
        SELECT nav_value FROM nav_data 
        WHERE fund_id = $1 ORDER BY nav_date DESC LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '30 days'
        AND nav_date <= DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '30 days'
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        r.nav_value as current_nav,
        y.nav_value as year_start_nav,
        CASE WHEN y.nav_value > 0 
             THEN ((r.nav_value - y.nav_value) / y.nav_value) * 100
             ELSE NULL END as return_ytd
      FROM recent_nav r, year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = quickScoreYTD(returnYTD);
      
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

function quickScore5Y(returnValue) {
  if (returnValue >= 1000) return 100;
  if (returnValue >= 500) return 95;
  if (returnValue >= 200) return 88;
  if (returnValue >= 100) return 80;
  if (returnValue >= 50) return 70;
  if (returnValue >= 25) return 60;
  if (returnValue >= 0) return 50;
  if (returnValue >= -25) return 35;
  return 20;
}

function quickScoreYTD(returnValue) {
  if (returnValue >= 100) return 100;
  if (returnValue >= 50) return 90;
  if (returnValue >= 25) return 80;
  if (returnValue >= 10) return 70;
  if (returnValue >= 0) return 55;
  if (returnValue >= -15) return 35;
  return 20;
}

acceleratedCoverageExpansion();