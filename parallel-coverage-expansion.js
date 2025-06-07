/**
 * Parallel Coverage Expansion
 * Maximum efficiency approach to complete 5Y and YTD coverage across all eligible funds
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function parallelCoverageExpansion() {
  try {
    console.log('Starting parallel expansion for maximum 5Y and YTD coverage...\n');
    
    let totalProcessed = 0;
    let totalAdded5Y = 0;
    let totalAddedYTD = 0;
    let iterationCount = 0;
    
    // Continue until maximum coverage is achieved
    while (iterationCount < 40) {
      iterationCount++;
      
      // Process both 5Y and YTD in parallel with optimized batches
      const [result5Y, resultYTD] = await Promise.all([
        processParallel5Y(iterationCount),
        processParallelYTD(iterationCount)
      ]);
      
      if (result5Y.processed === 0 && resultYTD.processed === 0) {
        console.log('Maximum coverage achieved - no more eligible funds to process');
        break;
      }
      
      totalProcessed += result5Y.processed + resultYTD.processed;
      totalAdded5Y += result5Y.added;
      totalAddedYTD += resultYTD.added;
      
      console.log(`Iteration ${iterationCount}: Processed ${result5Y.processed + resultYTD.processed} funds (+${result5Y.added} 5Y, +${resultYTD.added} YTD)`);
      
      // Progress checkpoint every 10 iterations
      if (iterationCount % 10 === 0) {
        const coverage = await getCurrentCoverage();
        console.log(`\n--- Checkpoint ${iterationCount} ---`);
        console.log(`5Y Coverage: ${coverage.funds_5y} funds (${coverage.pct_5y}%)`);
        console.log(`YTD Coverage: ${coverage.funds_ytd} funds (${coverage.pct_ytd}%)`);
        console.log(`Session totals: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD\n`);
      }
    }
    
    // Final comprehensive results
    const finalCoverage = await getCurrentCoverage();
    
    console.log(`\n=== PARALLEL EXPANSION COMPLETE ===`);
    console.log(`Final 5Y Coverage: ${finalCoverage.funds_5y} funds (${finalCoverage.pct_5y}%)`);
    console.log(`Final YTD Coverage: ${finalCoverage.funds_ytd} funds (${finalCoverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${finalCoverage.complete_coverage} funds with both 5Y and YTD`);
    console.log(`Total expansion: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD in ${iterationCount} iterations`);
    
  } catch (error) {
    console.error('Error in parallel expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processParallel5Y(iteration) {
  try {
    // Get next batch of eligible 5Y funds with progressive criteria relaxation
    const relaxationLevel = Math.min(iteration * 30, 180); // Gradually relax criteria
    
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '2 years' - INTERVAL '${relaxationLevel} days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= GREATEST(50, 120 - $1 * 5)
      )
      ORDER BY f.id
      LIMIT 800
    `, [iteration]);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { processed: 0, added: 0 };
    
    // Process in parallel chunks
    let added = 0;
    const chunkSize = 50;
    
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => calculate5YReturn(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { processed: funds.length, added };
    
  } catch (error) {
    return { processed: 0, added: 0 };
  }
}

async function processParallelYTD(iteration) {
  try {
    // Get next batch of eligible YTD funds with progressive criteria relaxation
    const relaxationLevel = Math.min(iteration * 15, 90);
    
    const eligibleFunds = await pool.query(`
      SELECT f.id
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '${relaxationLevel} days'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= GREATEST(3, 8 - $1)
      )
      ORDER BY f.id
      LIMIT 1000
    `, [iteration]);
    
    const funds = eligibleFunds.rows;
    if (funds.length === 0) return { processed: 0, added: 0 };
    
    // Process in parallel chunks
    let added = 0;
    const chunkSize = 75;
    
    for (let i = 0; i < funds.length; i += chunkSize) {
      const chunk = funds.slice(i, i + chunkSize);
      const promises = chunk.map(fund => calculateYTDReturn(fund.id));
      const results = await Promise.allSettled(promises);
      added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
    }
    
    return { processed: funds.length, added };
    
  } catch (error) {
    return { processed: 0, added: 0 };
  }
}

async function calculate5YReturn(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_data_sorted AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 2000
      ),
      latest_nav AS (
        SELECT nav_value as current_nav
        FROM nav_data_sorted
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value as historical_nav
        FROM nav_data_sorted
        WHERE nav_date <= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY nav_date DESC
        LIMIT 1
      )
      SELECT 
        l.current_nav,
        h.historical_nav,
        CASE 
          WHEN h.historical_nav > 0 AND l.current_nav IS NOT NULL
          THEN ((l.current_nav - h.historical_nav) / h.historical_nav) * 100
          ELSE NULL 
        END as return_5y
      FROM latest_nav l
      CROSS JOIN historical_nav h
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_5y !== null) {
      const return5Y = parseFloat(result.rows[0].return_5y);
      const score = calculateScoreQuick(return5Y, '5Y');
      
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

async function calculateYTDReturn(fundId) {
  try {
    const result = await pool.query(`
      WITH nav_data_filtered AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '60 days'
        ORDER BY nav_date DESC
        LIMIT 400
      ),
      latest_nav AS (
        SELECT nav_value as current_nav
        FROM nav_data_filtered
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value as year_start_nav
        FROM nav_data_filtered
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        l.current_nav,
        y.year_start_nav,
        CASE 
          WHEN y.year_start_nav > 0 AND l.current_nav IS NOT NULL
          THEN ((l.current_nav - y.year_start_nav) / y.year_start_nav) * 100
          ELSE NULL 
        END as return_ytd
      FROM latest_nav l
      CROSS JOIN year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0 && result.rows[0].return_ytd !== null) {
      const returnYTD = parseFloat(result.rows[0].return_ytd);
      const score = calculateScoreQuick(returnYTD, 'YTD');
      
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

function calculateScoreQuick(returnValue, period) {
  if (period === '5Y') {
    if (returnValue >= 1000) return 100;
    if (returnValue >= 500) return 95;
    if (returnValue >= 200) return 88;
    if (returnValue >= 100) return 80;
    if (returnValue >= 50) return 70;
    if (returnValue >= 25) return 60;
    if (returnValue >= 0) return 50;
    if (returnValue >= -25) return 35;
    return 20;
  } else { // YTD
    if (returnValue >= 100) return 100;
    if (returnValue >= 50) return 90;
    if (returnValue >= 25) return 80;
    if (returnValue >= 10) return 70;
    if (returnValue >= 0) return 55;
    if (returnValue >= -15) return 35;
    return 20;
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

parallelCoverageExpansion();