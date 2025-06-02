/**
 * Systematic 5Y and YTD Coverage Expansion
 * Processes funds in efficient batches to maximize coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function systematic5YAndYTDExpansion() {
  try {
    console.log('Starting systematic 5Y and YTD expansion...\n');
    
    // Process in manageable batches
    const batchSize = 50;
    let totalUpdated5Y = 0;
    let totalUpdatedYTD = 0;
    
    // Batch 1: Focus on high-data-quality funds first
    console.log('=== Batch 1: High-Quality Funds ===');
    const batch1Results = await processBatch({
      navCountMin: 2000,
      limit: batchSize,
      description: 'high-quality funds (2000+ NAV records)'
    });
    
    totalUpdated5Y += batch1Results.updated5Y;
    totalUpdatedYTD += batch1Results.updatedYTD;
    
    // Batch 2: Medium-quality funds
    console.log('\n=== Batch 2: Medium-Quality Funds ===');
    const batch2Results = await processBatch({
      navCountMin: 1000,
      navCountMax: 2000,
      limit: batchSize,
      description: 'medium-quality funds (1000-2000 NAV records)'
    });
    
    totalUpdated5Y += batch2Results.updated5Y;
    totalUpdatedYTD += batch2Results.updatedYTD;
    
    // Batch 3: Equity funds specifically
    console.log('\n=== Batch 3: Equity Funds ===');
    const batch3Results = await processBatch({
      category: 'Equity',
      navCountMin: 500,
      limit: batchSize,
      description: 'Equity category funds'
    });
    
    totalUpdated5Y += batch3Results.updated5Y;
    totalUpdatedYTD += batch3Results.updatedYTD;
    
    // Final coverage report
    const coverage = await getCurrentCoverage();
    
    console.log(`\n=== Systematic Expansion Results ===`);
    console.log(`Total 5Y updates in this run: ${totalUpdated5Y}`);
    console.log(`Total YTD updates in this run: ${totalUpdatedYTD}`);
    console.log(`\nCurrent System Coverage:`);
    console.log(`5Y Analysis: ${coverage.funds_5y}/${coverage.total_funds} (${coverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${coverage.funds_ytd}/${coverage.total_funds} (${coverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${coverage.complete_coverage}/${coverage.total_funds} (${coverage.pct_complete}%)`);
    
  } catch (error) {
    console.error('Error in systematic expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processBatch(options) {
  const { navCountMin = 500, navCountMax = null, category = null, limit = 50, description } = options;
  
  let whereClause = `
    EXISTS (
      SELECT 1 FROM nav_data nd 
      WHERE nd.fund_id = f.id 
      GROUP BY nd.fund_id
      HAVING COUNT(*) >= ${navCountMin}
      ${navCountMax ? `AND COUNT(*) < ${navCountMax}` : ''}
    )
  `;
  
  if (category) {
    whereClause += ` AND f.category = '${category}'`;
  }
  
  const eligibleFunds = await pool.query(`
    SELECT DISTINCT f.id, f.fund_name, f.category
    FROM funds f
    JOIN fund_scores fs ON f.id = fs.fund_id
    WHERE ${whereClause}
    AND (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
    ORDER BY f.id
    LIMIT ${limit}
  `);
  
  console.log(`Processing ${eligibleFunds.rows.length} ${description}...`);
  
  let updated5Y = 0;
  let updatedYTD = 0;
  
  for (const fund of eligibleFunds.rows) {
    try {
      const enhancements = await calculateTimeReturns(fund.id);
      
      if (enhancements.updated) {
        await updateFundAnalysis(fund.id, enhancements);
        
        if (enhancements.return5Y !== null) updated5Y++;
        if (enhancements.returnYTD !== null) updatedYTD++;
        
        if ((updated5Y + updatedYTD) % 10 === 0) {
          console.log(`  Progress: 5Y+${updated5Y}, YTD+${updatedYTD} | ${fund.fund_name.substring(0, 40)}...`);
        }
      }
      
    } catch (error) {
      console.error(`  Error processing ${fund.id}: ${error.message}`);
    }
  }
  
  console.log(`Batch complete: +${updated5Y} 5Y scores, +${updatedYTD} YTD scores`);
  return { updated5Y, updatedYTD };
}

async function calculateTimeReturns(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2000
  `, [fundId]);
  
  if (navData.rows.length < 100) {
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
  
  // Calculate 5-year return
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  let best5YMatch = null;
  let minDiff5Y = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff5Y && daysDiff <= 90) {
      minDiff5Y = daysDiff;
      best5YMatch = record;
    }
  }
  
  if (best5YMatch) {
    return5Y = ((currentNav - best5YMatch.value) / best5YMatch.value) * 100;
  }
  
  // Calculate YTD return
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  let bestYTDMatch = null;
  let minDiffYTD = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiffYTD && daysDiff <= 25) {
      minDiffYTD = daysDiff;
      bestYTDMatch = record;
    }
  }
  
  if (bestYTDMatch) {
    returnYTD = ((currentNav - bestYTDMatch.value) / bestYTDMatch.value) * 100;
  }
  
  return {
    updated: return5Y !== null || returnYTD !== null,
    return5Y,
    returnYTD
  };
}

async function updateFundAnalysis(fundId, enhancements) {
  const updateParts = [];
  const values = [];
  let paramIndex = 1;
  
  if (enhancements.return5Y !== null) {
    const score5Y = scoreReturn(enhancements.return5Y, 'fiveYear');
    updateParts.push(`return_5y_score = $${paramIndex++}`);
    values.push(score5Y);
  }
  
  if (enhancements.returnYTD !== null) {
    const scoreYTD = scoreReturn(enhancements.returnYTD, 'ytd');
    updateParts.push(`return_ytd_score = $${paramIndex++}`);
    values.push(scoreYTD);
  }
  
  if (updateParts.length > 0) {
    values.push(fundId);
    
    await pool.query(`
      UPDATE fund_scores 
      SET ${updateParts.join(', ')}
      WHERE fund_id = $${paramIndex++}
    `, values);
  }
}

function scoreReturn(returnValue, period) {
  if (period === 'fiveYear') {
    if (returnValue >= 200) return 100;
    if (returnValue >= 150) return 95;
    if (returnValue >= 100) return 90;
    if (returnValue >= 75) return 85;
    if (returnValue >= 50) return 75;
    if (returnValue >= 25) return 65;
    if (returnValue >= 0) return 50;
    if (returnValue >= -25) return 30;
    return 15;
  } else if (period === 'ytd') {
    if (returnValue >= 30) return 100;
    if (returnValue >= 20) return 90;
    if (returnValue >= 15) return 85;
    if (returnValue >= 10) return 75;
    if (returnValue >= 5) return 65;
    if (returnValue >= 0) return 50;
    if (returnValue >= -10) return 35;
    return 20;
  }
  
  return 50;
}

async function getCurrentCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
      COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
      COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_5y,
      ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_ytd,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 1) as pct_complete
    FROM fund_scores
  `);
  
  return result.rows[0];
}

systematic5YAndYTDExpansion();