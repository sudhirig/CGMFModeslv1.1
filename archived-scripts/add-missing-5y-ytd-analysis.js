/**
 * Add missing 5Y and YTD analysis to existing fund scores
 * Updates funds that already have scores but are missing these key metrics
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function addMissing5YAndYTDAnalysis() {
  try {
    console.log('Adding missing 5Y and YTD analysis to existing fund scores...\n');
    
    // Get funds that have scores but missing 5Y or YTD analysis
    const fundsNeedingUpdates = await pool.query(`
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        fs.score_date,
        COUNT(nd.nav_value) as nav_count,
        MIN(nd.nav_date) as earliest_date,
        MAX(nd.nav_date) as latest_date
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
      AND COUNT(nd.nav_value) >= 500
      GROUP BY f.id, f.fund_name, f.category, fs.score_date
      HAVING MIN(nd.nav_date) <= CURRENT_DATE - INTERVAL '2 years'
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 100
    `);
    
    console.log(`Found ${fundsNeedingUpdates.rows.length} funds needing 5Y/YTD updates`);
    
    let updated5Y = 0;
    let updatedYTD = 0;
    let processed = 0;
    
    for (const fund of fundsNeedingUpdates.rows) {
      try {
        processed++;
        console.log(`[${processed}/${fundsNeedingUpdates.rows.length}] ${fund.fund_name}`);
        console.log(`  Data range: ${fund.earliest_date} to ${fund.latest_date} (${fund.nav_count} records)`);
        
        const analysis = await calculateMissingReturns(fund);
        
        if (analysis.return5Y !== null || analysis.returnYTD !== null) {
          await updateExistingScore(fund.id, fund.score_date, analysis);
          
          if (analysis.return5Y !== null) updated5Y++;
          if (analysis.returnYTD !== null) updatedYTD++;
          
          const updates = [];
          if (analysis.return5Y !== null) updates.push(`5Y: ${analysis.return5Y.toFixed(2)}%`);
          if (analysis.returnYTD !== null) updates.push(`YTD: ${analysis.returnYTD.toFixed(2)}%`);
          
          console.log(`  ✓ Updated - ${updates.join(', ')}`);
        } else {
          console.log(`  ⚠ Insufficient data for calculations`);
        }
        
      } catch (error) {
        console.error(`  ✗ Error: ${error.message}`);
      }
    }
    
    console.log(`\n=== Analysis Enhancement Summary ===`);
    console.log(`Processed: ${processed} funds`);
    console.log(`Added 5Y analysis: ${updated5Y} funds`);
    console.log(`Added YTD analysis: ${updatedYTD} funds`);
    console.log(`Fund scoring now includes comprehensive time period coverage`);
    
  } catch (error) {
    console.error('Error adding missing 5Y and YTD analysis:', error);
  } finally {
    await pool.end();
  }
}

async function calculateMissingReturns(fund) {
  // Get comprehensive NAV data
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2500
  `, [fund.id]);
  
  if (navData.rows.length < 100) {
    return { return5Y: null, returnYTD: null };
  }
  
  const navRecords = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = navRecords[0].value;
  const currentDate = navRecords[0].date;
  
  let return5Y = null;
  let returnYTD = null;
  
  // Calculate 5-year return
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  // Find NAV closest to 5 years ago
  let closest5YRecord = null;
  let minDiff5Y = Infinity;
  
  for (const record of navRecords) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff5Y && daysDiff <= 45) { // Within 45 days
      minDiff5Y = daysDiff;
      closest5YRecord = record;
    }
  }
  
  if (closest5YRecord) {
    return5Y = ((currentNav - closest5YRecord.value) / closest5YRecord.value) * 100;
    console.log(`    5Y calculation: Current ${currentNav} vs ${closest5YRecord.date.toISOString().split('T')[0]} ${closest5YRecord.value}`);
  }
  
  // Calculate YTD return
  const currentYear = currentDate.getFullYear();
  const yearStart = new Date(currentYear, 0, 1);
  
  // Find NAV closest to start of current year
  let closestYTDRecord = null;
  let minDiffYTD = Infinity;
  
  for (const record of navRecords) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiffYTD && daysDiff <= 20) { // Within 20 days of year start
      minDiffYTD = daysDiff;
      closestYTDRecord = record;
    }
  }
  
  if (closestYTDRecord) {
    returnYTD = ((currentNav - closestYTDRecord.value) / closestYTDRecord.value) * 100;
    console.log(`    YTD calculation: Current ${currentNav} vs ${closestYTDRecord.date.toISOString().split('T')[0]} ${closestYTDRecord.value}`);
  }
  
  return { return5Y, returnYTD };
}

async function updateExistingScore(fundId, scoreDate, analysis) {
  const updateFields = [];
  const values = [];
  let paramIndex = 1;
  
  if (analysis.return5Y !== null) {
    const score5Y = calculateReturnScore(analysis.return5Y, '5y');
    updateFields.push(`return_5y_score = $${paramIndex++}`);
    values.push(score5Y);
  }
  
  if (analysis.returnYTD !== null) {
    const scoreYTD = calculateReturnScore(analysis.returnYTD, 'ytd');
    updateFields.push(`return_ytd_score = $${paramIndex++}`);
    values.push(scoreYTD);
  }
  
  if (updateFields.length > 0) {
    values.push(fundId, scoreDate);
    
    const query = `
      UPDATE fund_scores 
      SET ${updateFields.join(', ')}
      WHERE fund_id = $${paramIndex++} AND score_date = $${paramIndex++}
    `;
    
    await pool.query(query, values);
  }
}

function calculateReturnScore(returnValue, period) {
  // Define benchmarks for scoring
  const benchmarks = {
    '5y': {
      equity: { excellent: 80, good: 60, average: 40, poor: 20 },
      debt: { excellent: 40, good: 30, average: 20, poor: 10 },
      hybrid: { excellent: 60, good: 45, average: 30, poor: 15 }
    },
    'ytd': {
      equity: { excellent: 15, good: 8, average: 3, poor: -5 },
      debt: { excellent: 5, good: 3, average: 1, poor: -1 },
      hybrid: { excellent: 10, good: 5, average: 2, poor: -3 }
    }
  };
  
  // Use equity benchmarks as default
  const benchmark = benchmarks[period]?.equity || benchmarks[period]?.equity;
  
  if (returnValue >= benchmark.excellent) return 100;
  if (returnValue >= benchmark.good) return 75;
  if (returnValue >= benchmark.average) return 50;
  if (returnValue >= benchmark.poor) return 25;
  return 10;
}

addMissing5YAndYTDAnalysis();