/**
 * Aggressive 5Y and YTD Coverage Expansion
 * Maximizes coverage across all eligible funds with sufficient historical data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function aggressive5YAndYTDExpansion() {
  try {
    console.log('Starting aggressive 5Y and YTD expansion to maximize coverage...\n');
    
    // Phase 1: Equity funds with substantial data (highest priority)
    console.log('=== Phase 1: Equity Funds (High Priority) ===');
    const phase1Results = await processPhase({
      category: 'Equity',
      navCountMin: 1000,
      batchSize: 100,
      maxFunds: 500,
      description: 'Equity funds with 1000+ NAV records'
    });
    
    // Phase 2: Debt funds with good data coverage
    console.log('\n=== Phase 2: Debt Funds ===');
    const phase2Results = await processPhase({
      category: 'Debt',
      navCountMin: 800,
      batchSize: 100,
      maxFunds: 400,
      description: 'Debt funds with 800+ NAV records'
    });
    
    // Phase 3: Hybrid and other categories
    console.log('\n=== Phase 3: Hybrid & Other Categories ===');
    const phase3Results = await processPhase({
      categories: ['Hybrid', 'Other', 'Fund of Funds'],
      navCountMin: 500,
      batchSize: 100,
      maxFunds: 300,
      description: 'Hybrid and other category funds'
    });
    
    // Phase 4: ETF and specialized funds
    console.log('\n=== Phase 4: ETF & Specialized Funds ===');
    const phase4Results = await processPhase({
      categories: ['Gold ETF', 'Silver ETF', 'ETF', 'International'],
      navCountMin: 200,
      batchSize: 50,
      maxFunds: 200,
      description: 'ETF and specialized funds'
    });
    
    // Phase 5: Remaining eligible funds (comprehensive sweep)
    console.log('\n=== Phase 5: Remaining Eligible Funds ===');
    const phase5Results = await processPhase({
      navCountMin: 300,
      batchSize: 150,
      maxFunds: 1000,
      description: 'All remaining funds with sufficient data'
    });
    
    const totalResults = {
      updated5Y: phase1Results.updated5Y + phase2Results.updated5Y + phase3Results.updated5Y + phase4Results.updated5Y + phase5Results.updated5Y,
      updatedYTD: phase1Results.updatedYTD + phase2Results.updatedYTD + phase3Results.updatedYTD + phase4Results.updatedYTD + phase5Results.updatedYTD,
      processed: phase1Results.processed + phase2Results.processed + phase3Results.processed + phase4Results.processed + phase5Results.processed
    };
    
    // Final comprehensive coverage report
    const finalCoverage = await getCurrentCoverage();
    
    console.log(`\n=== AGGRESSIVE EXPANSION RESULTS ===`);
    console.log(`Total Processed: ${totalResults.processed} funds`);
    console.log(`5Y Analysis Added: ${totalResults.updated5Y} funds`);
    console.log(`YTD Analysis Added: ${totalResults.updatedYTD} funds`);
    console.log(`\nFINAL SYSTEM COVERAGE:`);
    console.log(`5Y Analysis: ${finalCoverage.funds_5y}/${finalCoverage.total_funds} (${finalCoverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${finalCoverage.funds_ytd}/${finalCoverage.total_funds} (${finalCoverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${finalCoverage.complete_coverage}/${finalCoverage.total_funds} (${finalCoverage.pct_complete}%)`);
    
    // Calculate coverage vs eligible funds
    const eligibleFunds = await getEligibleFundsCount();
    console.log(`\nCOVERAGE VS ELIGIBLE FUNDS:`);
    console.log(`5Y: ${finalCoverage.funds_5y}/${eligibleFunds.eligible_5y} (${(finalCoverage.funds_5y/eligibleFunds.eligible_5y*100).toFixed(1)}%)`);
    console.log(`YTD: ${finalCoverage.funds_ytd}/${eligibleFunds.eligible_ytd} (${(finalCoverage.funds_ytd/eligibleFunds.eligible_ytd*100).toFixed(1)}%)`);
    
  } catch (error) {
    console.error('Error in aggressive expansion:', error);
  } finally {
    await pool.end();
  }
}

async function processPhase(options) {
  const { 
    category = null, 
    categories = null,
    navCountMin = 500, 
    batchSize = 100, 
    maxFunds = 500,
    description 
  } = options;
  
  let categoryFilter = '';
  if (category) {
    categoryFilter = `AND f.category = '${category}'`;
  } else if (categories) {
    categoryFilter = `AND f.category IN ('${categories.join("','")}')`;
  }
  
  const eligibleFunds = await pool.query(`
    SELECT f.id, f.fund_name, f.category
    FROM funds f
    JOIN fund_scores fs ON f.id = fs.fund_id
    WHERE EXISTS (
      SELECT 1 FROM nav_data nd 
      WHERE nd.fund_id = f.id 
      GROUP BY nd.fund_id
      HAVING COUNT(*) >= ${navCountMin}
    )
    ${categoryFilter}
    AND (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
    ORDER BY f.id
    LIMIT ${maxFunds}
  `);
  
  console.log(`Processing ${eligibleFunds.rows.length} ${description}...`);
  
  let updated5Y = 0;
  let updatedYTD = 0;
  let processed = 0;
  
  // Process in batches
  for (let i = 0; i < eligibleFunds.rows.length; i += batchSize) {
    const batch = eligibleFunds.rows.slice(i, i + batchSize);
    const batchNum = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(eligibleFunds.rows.length / batchSize);
    
    console.log(`  Batch ${batchNum}/${totalBatches}: Processing ${batch.length} funds`);
    
    for (const fund of batch) {
      try {
        processed++;
        const analysis = await calculateComprehensiveReturns(fund.id);
        
        if (analysis.updated) {
          await updateFundReturns(fund.id, analysis);
          
          if (analysis.return5Y !== null) updated5Y++;
          if (analysis.returnYTD !== null) updatedYTD++;
        }
        
      } catch (error) {
        console.error(`    Error processing fund ${fund.id}: ${error.message}`);
      }
    }
    
    if (batchNum % 3 === 0 || batchNum === totalBatches) {
      console.log(`    Progress: +${updated5Y} 5Y, +${updatedYTD} YTD (${processed}/${eligibleFunds.rows.length} processed)`);
    }
  }
  
  console.log(`Phase complete: +${updated5Y} 5Y scores, +${updatedYTD} YTD scores from ${processed} funds`);
  return { updated5Y, updatedYTD, processed };
}

async function calculateComprehensiveReturns(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2500
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
  
  // Calculate 5-year return with flexible matching
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  let best5YMatch = null;
  let minDiff5Y = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff5Y && daysDiff <= 120) { // Allow up to 4 months flexibility
      minDiff5Y = daysDiff;
      best5YMatch = record;
    }
  }
  
  if (best5YMatch) {
    return5Y = ((currentNav - best5YMatch.value) / best5YMatch.value) * 100;
  }
  
  // Calculate YTD return with flexible matching
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  let bestYTDMatch = null;
  let minDiffYTD = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiffYTD && daysDiff <= 30) { // Allow up to 1 month flexibility
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

async function updateFundReturns(fundId, analysis) {
  const updateFields = [];
  const values = [];
  let paramIndex = 1;
  
  if (analysis.return5Y !== null) {
    const score5Y = calculatePerformanceScore(analysis.return5Y, '5y');
    updateFields.push(`return_5y_score = $${paramIndex++}`);
    values.push(score5Y);
  }
  
  if (analysis.returnYTD !== null) {
    const scoreYTD = calculatePerformanceScore(analysis.returnYTD, 'ytd');
    updateFields.push(`return_ytd_score = $${paramIndex++}`);
    values.push(scoreYTD);
  }
  
  if (updateFields.length > 0) {
    values.push(fundId);
    
    await pool.query(`
      UPDATE fund_scores 
      SET ${updateFields.join(', ')}
      WHERE fund_id = $${paramIndex++}
    `, values);
  }
}

function calculatePerformanceScore(returnValue, period) {
  if (period === '5y') {
    // Enhanced 5-year scoring with broader range
    if (returnValue >= 500) return 100;      // Exceptional: >500%
    if (returnValue >= 300) return 95;       // Outstanding: 300-500%
    if (returnValue >= 200) return 90;       // Excellent: 200-300%
    if (returnValue >= 150) return 85;       // Very good: 150-200%
    if (returnValue >= 100) return 80;       // Good: 100-150%
    if (returnValue >= 75) return 75;        // Above average: 75-100%
    if (returnValue >= 50) return 70;        // Average: 50-75%
    if (returnValue >= 25) return 60;        // Below average: 25-50%
    if (returnValue >= 0) return 50;         // Poor: 0-25%
    if (returnValue >= -25) return 35;       // Very poor: 0 to -25%
    return 20;                               // Extremely poor: <-25%
  } else if (period === 'ytd') {
    // Enhanced YTD scoring
    if (returnValue >= 50) return 100;       // Exceptional: >50% YTD
    if (returnValue >= 35) return 95;        // Outstanding: 35-50%
    if (returnValue >= 25) return 90;        // Excellent: 25-35%
    if (returnValue >= 20) return 85;        // Very good: 20-25%
    if (returnValue >= 15) return 80;        // Good: 15-20%
    if (returnValue >= 10) return 75;        // Above average: 10-15%
    if (returnValue >= 5) return 65;         // Average: 5-10%
    if (returnValue >= 0) return 55;         // Below average: 0-5%
    if (returnValue >= -10) return 40;       // Poor: 0 to -10%
    if (returnValue >= -20) return 25;       // Very poor: -10 to -20%
    return 15;                               // Extremely poor: <-20%
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
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
      ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd,
      ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_complete
    FROM fund_scores
  `);
  
  return result.rows[0];
}

async function getEligibleFundsCount() {
  const result = await pool.query(`
    SELECT 
      COUNT(CASE WHEN nd_5y.fund_id IS NOT NULL THEN 1 END) as eligible_5y,
      COUNT(CASE WHEN nd_ytd.fund_id IS NOT NULL THEN 1 END) as eligible_ytd
    FROM funds f
    LEFT JOIN (
      SELECT DISTINCT fund_id
      FROM nav_data 
      WHERE nav_date <= CURRENT_DATE - INTERVAL '5 years'
      GROUP BY fund_id
      HAVING COUNT(*) >= 1000
    ) nd_5y ON f.id = nd_5y.fund_id
    LEFT JOIN (
      SELECT DISTINCT fund_id
      FROM nav_data 
      WHERE nav_date >= DATE_TRUNC('year', CURRENT_DATE)
      GROUP BY fund_id
      HAVING COUNT(*) >= 100
    ) nd_ytd ON f.id = nd_ytd.fund_id
  `);
  
  return result.rows[0];
}

aggressive5YAndYTDExpansion();