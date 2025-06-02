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
    console.log('Starting efficient bulk 5Y and YTD expansion...\n');
    
    // Get current coverage baseline
    const initialCoverage = await getCurrentCoverage();
    console.log('Initial Coverage:');
    console.log(`5Y: ${initialCoverage.funds_5y}/${initialCoverage.total_funds} (${initialCoverage.pct_5y}%)`);
    console.log(`YTD: ${initialCoverage.funds_ytd}/${initialCoverage.total_funds} (${initialCoverage.pct_ytd}%)\n`);
    
    // Process high-impact funds first (highest data quality and volume)
    console.log('=== Processing High-Impact Funds ===');
    const highImpactResults = await processHighImpactFunds();
    
    // Process medium-impact funds
    console.log('\n=== Processing Medium-Impact Funds ===');
    const mediumImpactResults = await processMediumImpactFunds();
    
    // Process remaining eligible funds
    console.log('\n=== Processing Remaining Eligible Funds ===');
    const remainingResults = await processRemainingFunds();
    
    // Final coverage assessment
    const finalCoverage = await getCurrentCoverage();
    const totalAdded5Y = finalCoverage.funds_5y - initialCoverage.funds_5y;
    const totalAddedYTD = finalCoverage.funds_ytd - initialCoverage.funds_ytd;
    
    console.log(`\n=== EXPANSION COMPLETE ===`);
    console.log(`5Y Analysis Added: ${totalAdded5Y} funds`);
    console.log(`YTD Analysis Added: ${totalAddedYTD} funds`);
    console.log(`\nFinal Coverage:`);
    console.log(`5Y: ${finalCoverage.funds_5y}/${finalCoverage.total_funds} (${finalCoverage.pct_5y}%)`);
    console.log(`YTD: ${finalCoverage.funds_ytd}/${finalCoverage.total_funds} (${finalCoverage.pct_ytd}%)`);
    console.log(`Complete: ${finalCoverage.complete_coverage}/${finalCoverage.total_funds} (${finalCoverage.pct_complete}%)`);
    
    // Calculate improvement
    const improvement5Y = ((finalCoverage.pct_5y - initialCoverage.pct_5y) / initialCoverage.pct_5y * 100).toFixed(1);
    const improvementYTD = ((finalCoverage.pct_ytd - initialCoverage.pct_ytd) / initialCoverage.pct_ytd * 100).toFixed(1);
    
    console.log(`\nImprovement: 5Y +${improvement5Y}%, YTD +${improvementYTD}%`);
    
  } catch (error) {
    console.error('Error in efficient bulk expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processHighImpactFunds() {
  const funds = await pool.query(`
    SELECT f.id, f.fund_name, f.category
    FROM funds f
    JOIN fund_scores fs ON f.id = fs.fund_id
    WHERE EXISTS (
      SELECT 1 FROM nav_data nd 
      WHERE nd.fund_id = f.id 
      GROUP BY nd.fund_id
      HAVING COUNT(*) >= 2000
    )
    AND (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
    AND f.category IN ('Equity', 'Debt')
    ORDER BY f.category, f.id
    LIMIT 200
  `);
  
  console.log(`Processing ${funds.rows.length} high-impact funds (2000+ NAV records)`);
  
  let updated5Y = 0;
  let updatedYTD = 0;
  
  for (const fund of funds.rows) {
    try {
      const analysis = await quickReturnCalculation(fund.id, 'comprehensive');
      
      if (analysis.updated) {
        await updateScores(fund.id, analysis);
        if (analysis.return5Y !== null) updated5Y++;
        if (analysis.returnYTD !== null) updatedYTD++;
        
        if ((updated5Y + updatedYTD) % 25 === 0) {
          console.log(`  Progress: +${updated5Y} 5Y, +${updatedYTD} YTD`);
        }
      }
      
    } catch (error) {
      console.error(`Error processing fund ${fund.id}: ${error.message}`);
    }
  }
  
  console.log(`High-impact complete: +${updated5Y} 5Y, +${updatedYTD} YTD`);
  return { updated5Y, updatedYTD };
}

async function processMediumImpactFunds() {
  const funds = await pool.query(`
    SELECT f.id, f.fund_name, f.category
    FROM funds f
    JOIN fund_scores fs ON f.id = fs.fund_id
    WHERE EXISTS (
      SELECT 1 FROM nav_data nd 
      WHERE nd.fund_id = f.id 
      GROUP BY nd.fund_id
      HAVING COUNT(*) >= 1000 AND COUNT(*) < 2000
    )
    AND (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
    ORDER BY f.category, f.id
    LIMIT 300
  `);
  
  console.log(`Processing ${funds.rows.length} medium-impact funds (1000-2000 NAV records)`);
  
  let updated5Y = 0;
  let updatedYTD = 0;
  
  for (const fund of funds.rows) {
    try {
      const analysis = await quickReturnCalculation(fund.id, 'standard');
      
      if (analysis.updated) {
        await updateScores(fund.id, analysis);
        if (analysis.return5Y !== null) updated5Y++;
        if (analysis.returnYTD !== null) updatedYTD++;
        
        if ((updated5Y + updatedYTD) % 30 === 0) {
          console.log(`  Progress: +${updated5Y} 5Y, +${updatedYTD} YTD`);
        }
      }
      
    } catch (error) {
      console.error(`Error processing fund ${fund.id}: ${error.message}`);
    }
  }
  
  console.log(`Medium-impact complete: +${updated5Y} 5Y, +${updatedYTD} YTD`);
  return { updated5Y, updatedYTD };
}

async function processRemainingFunds() {
  const funds = await pool.query(`
    SELECT f.id, f.fund_name, f.category
    FROM funds f
    JOIN fund_scores fs ON f.id = fs.fund_id
    WHERE EXISTS (
      SELECT 1 FROM nav_data nd 
      WHERE nd.fund_id = f.id 
      GROUP BY nd.fund_id
      HAVING COUNT(*) >= 500
    )
    AND (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
    ORDER BY f.id
    LIMIT 400
  `);
  
  console.log(`Processing ${funds.rows.length} remaining eligible funds (500+ NAV records)`);
  
  let updated5Y = 0;
  let updatedYTD = 0;
  
  for (const fund of funds.rows) {
    try {
      const analysis = await quickReturnCalculation(fund.id, 'basic');
      
      if (analysis.updated) {
        await updateScores(fund.id, analysis);
        if (analysis.return5Y !== null) updated5Y++;
        if (analysis.returnYTD !== null) updatedYTD++;
        
        if ((updated5Y + updatedYTD) % 40 === 0) {
          console.log(`  Progress: +${updated5Y} 5Y, +${updatedYTD} YTD`);
        }
      }
      
    } catch (error) {
      console.error(`Error processing fund ${fund.id}: ${error.message}`);
    }
  }
  
  console.log(`Remaining funds complete: +${updated5Y} 5Y, +${updatedYTD} YTD`);
  return { updated5Y, updatedYTD };
}

async function quickReturnCalculation(fundId, level) {
  const limits = {
    comprehensive: 2500,
    standard: 1500,
    basic: 1000
  };
  
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT ${limits[level]}
  `, [fundId]);
  
  if (navData.rows.length < 200) {
    return { updated: false };
  }
  
  const records = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = records[0].value;
  const currentDate = records[0].date;
  
  let return5Y = null;
  let returnYTD = null;
  
  // Optimized 5Y calculation
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  for (let i = records.length - 1; i >= 0; i--) {
    const daysDiff = Math.abs((records[i].date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff <= 150) { // 5 month flexibility
      return5Y = ((currentNav - records[i].value) / records[i].value) * 100;
      break;
    }
  }
  
  // Optimized YTD calculation
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  for (let i = records.length - 1; i >= 0; i--) {
    const daysDiff = Math.abs((records[i].date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff <= 45) { // 1.5 month flexibility
      returnYTD = ((currentNav - records[i].value) / records[i].value) * 100;
      break;
    }
  }
  
  return {
    updated: return5Y !== null || returnYTD !== null,
    return5Y,
    returnYTD
  };
}

async function updateScores(fundId, analysis) {
  const updates = [];
  const values = [];
  let paramIndex = 1;
  
  if (analysis.return5Y !== null) {
    const score5Y = calculateScore(analysis.return5Y, '5y');
    updates.push(`return_5y_score = $${paramIndex++}`);
    values.push(score5Y);
  }
  
  if (analysis.returnYTD !== null) {
    const scoreYTD = calculateScore(analysis.returnYTD, 'ytd');
    updates.push(`return_ytd_score = $${paramIndex++}`);
    values.push(scoreYTD);
  }
  
  if (updates.length > 0) {
    values.push(fundId);
    await pool.query(`
      UPDATE fund_scores 
      SET ${updates.join(', ')}
      WHERE fund_id = $${paramIndex++}
    `, values);
  }
}

function calculateScore(returnValue, period) {
  if (period === '5y') {
    if (returnValue >= 400) return 100;
    if (returnValue >= 250) return 95;
    if (returnValue >= 150) return 90;
    if (returnValue >= 100) return 85;
    if (returnValue >= 75) return 80;
    if (returnValue >= 50) return 75;
    if (returnValue >= 25) return 65;
    if (returnValue >= 0) return 55;
    if (returnValue >= -25) return 40;
    return 25;
  } else {
    if (returnValue >= 40) return 100;
    if (returnValue >= 25) return 90;
    if (returnValue >= 15) return 80;
    if (returnValue >= 10) return 70;
    if (returnValue >= 5) return 60;
    if (returnValue >= 0) return 50;
    if (returnValue >= -10) return 35;
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
      ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_complete
    FROM fund_scores
  `);
  
  return result.rows[0];
}

efficientBulk5YAndYTDExpansion();