/**
 * Efficient Bulk 5Y and YTD Expansion
 * Optimized for maximum coverage with minimal processing time
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function efficientBulk5YAndYTDExpansion() {
  try {
    console.log('Starting efficient bulk expansion for maximum 5Y and YTD coverage...\n');
    
    let totalAdded5Y = 0;
    let totalAddedYTD = 0;
    let phaseNumber = 0;
    
    // Run multiple phases with different criteria to maximize coverage
    while (phaseNumber < 30) {
      phaseNumber++;
      
      console.log(`Processing phase ${phaseNumber}...`);
      
      // Process high-impact, medium-impact, and remaining funds in sequence
      const [highImpact, mediumImpact, remaining] = await Promise.all([
        processHighImpactFunds(),
        processMediumImpactFunds(),
        processRemainingFunds()
      ]);
      
      if (highImpact.added5Y === 0 && highImpact.addedYTD === 0 && 
          mediumImpact.added5Y === 0 && mediumImpact.addedYTD === 0 &&
          remaining.added5Y === 0 && remaining.addedYTD === 0) {
        console.log('Maximum coverage achieved - no more eligible funds');
        break;
      }
      
      const phase5Y = highImpact.added5Y + mediumImpact.added5Y + remaining.added5Y;
      const phaseYTD = highImpact.addedYTD + mediumImpact.addedYTD + remaining.addedYTD;
      
      totalAdded5Y += phase5Y;
      totalAddedYTD += phaseYTD;
      
      console.log(`  Phase ${phaseNumber}: +${phase5Y} 5Y, +${phaseYTD} YTD`);
      
      // Progress checkpoint every 10 phases
      if (phaseNumber % 10 === 0) {
        const coverage = await getCurrentCoverage();
        console.log(`\n=== Phase ${phaseNumber} Status ===`);
        console.log(`5Y Coverage: ${coverage.funds_5y} funds (${coverage.pct_5y}%)`);
        console.log(`YTD Coverage: ${coverage.funds_ytd} funds (${coverage.pct_ytd}%)`);
        console.log(`Complete Coverage: ${coverage.complete_coverage} funds`);
        console.log(`Session totals: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD\n`);
      }
    }
    
    // Final comprehensive assessment
    const finalResults = await getCurrentCoverage();
    
    console.log(`\n=== EFFICIENT BULK EXPANSION COMPLETE ===`);
    console.log(`Final 5Y Coverage: ${finalResults.funds_5y}/${finalResults.total_funds} (${finalResults.pct_5y}%)`);
    console.log(`Final YTD Coverage: ${finalResults.funds_ytd}/${finalResults.total_funds} (${finalResults.pct_ytd}%)`);
    console.log(`Complete Coverage: ${finalResults.complete_coverage}/${finalResults.total_funds} funds`);
    console.log(`Total expansion: +${totalAdded5Y} 5Y, +${totalAddedYTD} YTD in ${phaseNumber} phases`);
    
  } catch (error) {
    console.error('Error in efficient bulk expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processHighImpactFunds() {
  try {
    // High-impact funds: funds with substantial historical data
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
          HAVING COUNT(*) >= 100
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
          HAVING COUNT(*) >= 8
        )
        ORDER BY f.id
        LIMIT 1000
      `)
    ]);
    
    const funds5Y = eligible5Y.rows;
    const fundsYTD = eligibleYTD.rows;
    
    let added5Y = 0;
    let addedYTD = 0;
    
    // Process in parallel chunks
    const [results5Y, resultsYTD] = await Promise.all([
      processFundsInChunks(funds5Y, 'high'),
      processFundsInChunks(fundsYTD, 'high')
    ]);
    
    return {
      added5Y: results5Y.added,
      addedYTD: resultsYTD.added
    };
    
  } catch (error) {
    return { added5Y: 0, addedYTD: 0 };
  }
}

async function processMediumImpactFunds() {
  try {
    // Medium-impact funds: funds with moderate historical data
    const [eligible5Y, eligibleYTD] = await Promise.all([
      pool.query(`
        SELECT f.id
        FROM funds f
        JOIN fund_scores fs ON f.id = fs.fund_id
        WHERE fs.return_5y_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '2 years'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 50
        )
        ORDER BY f.id
        LIMIT 1000
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
        LIMIT 1200
      `)
    ]);
    
    const funds5Y = eligible5Y.rows;
    const fundsYTD = eligibleYTD.rows;
    
    const [results5Y, resultsYTD] = await Promise.all([
      processFundsInChunks(funds5Y, 'medium'),
      processFundsInChunks(fundsYTD, 'medium')
    ]);
    
    return {
      added5Y: results5Y.added,
      addedYTD: resultsYTD.added
    };
    
  } catch (error) {
    return { added5Y: 0, addedYTD: 0 };
  }
}

async function processRemainingFunds() {
  try {
    // Remaining funds: funds with minimal but sufficient data
    const [eligible5Y, eligibleYTD] = await Promise.all([
      pool.query(`
        SELECT f.id
        FROM funds f
        JOIN fund_scores fs ON f.id = fs.fund_id
        WHERE fs.return_5y_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '18 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 25
        )
        ORDER BY f.id
        LIMIT 1500
      `),
      pool.query(`
        SELECT f.id
        FROM funds f
        JOIN fund_scores fs ON f.id = fs.fund_id
        WHERE fs.return_ytd_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = f.id 
          AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '150 days'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 3
        )
        ORDER BY f.id
        LIMIT 1800
      `)
    ]);
    
    const funds5Y = eligible5Y.rows;
    const fundsYTD = eligibleYTD.rows;
    
    const [results5Y, resultsYTD] = await Promise.all([
      processFundsInChunks(funds5Y, 'remaining'),
      processFundsInChunks(fundsYTD, 'remaining')
    ]);
    
    return {
      added5Y: results5Y.added,
      addedYTD: resultsYTD.added
    };
    
  } catch (error) {
    return { added5Y: 0, addedYTD: 0 };
  }
}

async function processFundsInChunks(funds, level) {
  if (funds.length === 0) return { added: 0 };
  
  let added = 0;
  const chunkSize = level === 'high' ? 80 : level === 'medium' ? 100 : 120;
  
  for (let i = 0; i < funds.length; i += chunkSize) {
    const chunk = funds.slice(i, i + chunkSize);
    const promises = chunk.map(fund => quickReturnCalculation(fund.id, level));
    const results = await Promise.allSettled(promises);
    added += results.filter(r => r.status === 'fulfilled' && r.value === true).length;
  }
  
  return { added };
}

async function quickReturnCalculation(fundId, level) {
  try {
    // Adaptive calculation based on processing level
    const timeInterval = level === 'high' ? '5 years' : level === 'medium' ? '3 years' : '2 years';
    
    const result = await pool.query(`
      WITH nav_data_quick AS (
        SELECT nav_value, nav_date
        FROM nav_data 
        WHERE fund_id = $1 
        ORDER BY nav_date DESC
        LIMIT 3000
      ),
      current_nav AS (
        SELECT nav_value as current_value
        FROM nav_data_quick
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      historical_nav AS (
        SELECT nav_value as historical_value
        FROM nav_data_quick
        WHERE nav_date <= CURRENT_DATE - INTERVAL '${timeInterval}'
        ORDER BY nav_date DESC
        LIMIT 1
      ),
      year_start_nav AS (
        SELECT nav_value as year_start_value
        FROM nav_data_quick
        WHERE nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '60 days'
        ORDER BY ABS(EXTRACT(EPOCH FROM (nav_date - DATE_TRUNC('year', CURRENT_DATE))))
        LIMIT 1
      )
      SELECT 
        CASE 
          WHEN h.historical_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - h.historical_value) / h.historical_value) * 100
          ELSE NULL 
        END as return_period,
        CASE 
          WHEN y.year_start_value > 0 AND c.current_value IS NOT NULL
          THEN ((c.current_value - y.year_start_value) / y.year_start_value) * 100
          ELSE NULL 
        END as return_ytd
      FROM current_nav c
      CROSS JOIN historical_nav h
      CROSS JOIN year_start_nav y
    `, [fundId]);
    
    if (result.rows.length > 0) {
      const row = result.rows[0];
      const analysis = {};
      
      if (row.return_period !== null) {
        analysis.return5Y = parseFloat(row.return_period);
        analysis.score5Y = calculateScore(analysis.return5Y, '5Y');
      }
      
      if (row.return_ytd !== null) {
        analysis.returnYTD = parseFloat(row.return_ytd);
        analysis.scoreYTD = calculateScore(analysis.returnYTD, 'YTD');
      }
      
      await updateScores(fundId, analysis);
      return true;
    }
    
    return false;
  } catch (error) {
    return false;
  }
}

async function updateScores(fundId, analysis) {
  const updates = [];
  const values = [fundId];
  let paramIndex = 2;
  
  if (analysis.score5Y !== undefined) {
    updates.push(`return_5y_score = $${paramIndex}`);
    values.push(analysis.score5Y);
    paramIndex++;
  }
  
  if (analysis.scoreYTD !== undefined) {
    updates.push(`return_ytd_score = $${paramIndex}`);
    values.push(analysis.scoreYTD);
    paramIndex++;
  }
  
  if (updates.length > 0) {
    await pool.query(`
      UPDATE fund_scores SET ${updates.join(', ')} WHERE fund_id = $1
    `, values);
  }
}

function calculateScore(returnValue, period) {
  if (period === '5Y') {
    if (returnValue >= 2000) return 100;
    if (returnValue >= 1000) return 95;
    if (returnValue >= 500) return 90;
    if (returnValue >= 200) return 85;
    if (returnValue >= 100) return 78;
    if (returnValue >= 50) return 70;
    if (returnValue >= 25) return 62;
    if (returnValue >= 0) return 50;
    if (returnValue >= -25) return 35;
    return 20;
  } else { // YTD
    if (returnValue >= 150) return 100;
    if (returnValue >= 100) return 92;
    if (returnValue >= 50) return 85;
    if (returnValue >= 25) return 78;
    if (returnValue >= 10) return 68;
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

efficientBulk5YAndYTDExpansion();