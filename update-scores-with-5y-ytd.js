/**
 * Update existing fund scores to include proper 5Y and YTD analysis
 * Enhances the scoring system with comprehensive time period coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function updateScoresWith5YAndYTD() {
  try {
    console.log('Updating existing fund scores with 5Y and YTD analysis...\n');
    
    // Get funds with sufficient data for 5Y analysis
    const fundsFor5Y = await pool.query(`
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        COUNT(nd.nav_value) as nav_count,
        MIN(nd.nav_date) as earliest_date
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE nd.nav_date >= CURRENT_DATE - INTERVAL '6 years'
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(nd.nav_value) >= 1000
      AND MIN(nd.nav_date) <= CURRENT_DATE - INTERVAL '5 years'
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 100
    `);
    
    console.log(`Found ${fundsFor5Y.rows.length} funds eligible for 5Y analysis`);
    
    let updated5Y = 0;
    let updatedYTD = 0;
    
    for (const fund of fundsFor5Y.rows) {
      try {
        console.log(`Processing ${fund.fund_name} (${fund.nav_count} records)`);
        
        const enhancements = await calculate5YAndYTDReturns(fund);
        
        if (enhancements.return5Y !== null || enhancements.returnYTD !== null) {
          await updateFundScore(fund.id, enhancements);
          
          if (enhancements.return5Y !== null) updated5Y++;
          if (enhancements.returnYTD !== null) updatedYTD++;
          
          console.log(`✓ Updated - 5Y: ${enhancements.return5Y?.toFixed(1)}%, YTD: ${enhancements.returnYTD?.toFixed(1)}%`);
        } else {
          console.log(`⚠ Insufficient data for period calculations`);
        }
        
      } catch (error) {
        console.error(`✗ Error processing fund ${fund.id}: ${error.message}`);
      }
    }
    
    // Now update funds with YTD only (for funds with less than 5Y data)
    console.log(`\nProcessing YTD updates for additional funds...`);
    
    const fundsForYTD = await pool.query(`
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        COUNT(nd.nav_value) as nav_count
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE nd.nav_date >= CURRENT_DATE - INTERVAL '1 year'
      AND f.id NOT IN (
        SELECT DISTINCT fund_id FROM fund_scores WHERE return_5y_score IS NOT NULL
      )
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(nd.nav_value) >= 100
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 200
    `);
    
    console.log(`Found ${fundsForYTD.rows.length} additional funds for YTD analysis`);
    
    for (const fund of fundsForYTD.rows) {
      try {
        const enhancements = await calculate5YAndYTDReturns(fund);
        
        if (enhancements.returnYTD !== null) {
          await updateFundScore(fund.id, enhancements);
          updatedYTD++;
          
          if (updatedYTD % 20 === 0) {
            console.log(`✓ Progress: ${updatedYTD} funds updated with YTD analysis`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing YTD for fund ${fund.id}:`, error.message);
      }
    }
    
    console.log(`\n=== Enhancement Summary ===`);
    console.log(`Funds updated with 5Y analysis: ${updated5Y}`);
    console.log(`Funds updated with YTD analysis: ${updatedYTD}`);
    console.log(`Enhanced scoring now includes comprehensive time period coverage`);
    
  } catch (error) {
    console.error('Error updating scores with 5Y and YTD:', error);
  } finally {
    await pool.end();
  }
}

async function calculate5YAndYTDReturns(fund) {
  // Get NAV data for the fund
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2000
  `, [fund.id]);
  
  if (navData.rows.length < 50) {
    return { return5Y: null, returnYTD: null, score5Y: null, scoreYTD: null };
  }
  
  const navValues = navData.rows.map(n => parseFloat(n.nav_value));
  const dates = navData.rows.map(n => new Date(n.nav_date));
  const currentNav = navValues[0];
  const currentDate = dates[0];
  
  let return5Y = null;
  let returnYTD = null;
  
  // Calculate 5Y return (approximately 1825 days)
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  let closest5YNav = null;
  let minDiff5Y = Infinity;
  
  for (let i = 0; i < dates.length; i++) {
    const daysDiff = Math.abs((dates[i] - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff5Y && daysDiff <= 30) { // Within 30 days
      minDiff5Y = daysDiff;
      closest5YNav = navValues[i];
    }
  }
  
  if (closest5YNav) {
    return5Y = ((currentNav - closest5YNav) / closest5YNav) * 100;
  }
  
  // Calculate YTD return
  const currentYear = currentDate.getFullYear();
  const yearStart = new Date(currentYear, 0, 1);
  
  let closestYTDNav = null;
  let minDiffYTD = Infinity;
  
  for (let i = 0; i < dates.length; i++) {
    const daysDiff = Math.abs((dates[i] - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiffYTD && daysDiff <= 15) { // Within 15 days of year start
      minDiffYTD = daysDiff;
      closestYTDNav = navValues[i];
    }
  }
  
  if (closestYTDNav) {
    returnYTD = ((currentNav - closestYTDNav) / closestYTDNav) * 100;
  }
  
  // Calculate scores based on category benchmarks
  const score5Y = return5Y !== null ? calculatePeriodScore(return5Y, '5y', fund.category) : null;
  const scoreYTD = returnYTD !== null ? calculatePeriodScore(returnYTD, 'ytd', fund.category) : null;
  
  return {
    return5Y,
    returnYTD,
    score5Y,
    scoreYTD
  };
}

function calculatePeriodScore(returnValue, period, category) {
  const benchmarks = {
    'Equity': { 
      '5y': { excellent: 75, good: 50, average: 25, poor: 10 },
      'ytd': { excellent: 10, good: 5, average: 0, poor: -5 }
    },
    'Debt': { 
      '5y': { excellent: 35, good: 25, average: 15, poor: 5 },
      'ytd': { excellent: 3, good: 2, average: 1, poor: 0 }
    },
    'Hybrid': { 
      '5y': { excellent: 50, good: 35, average: 20, poor: 10 },
      'ytd': { excellent: 6, good: 3, average: 1, poor: -2 }
    }
  };
  
  const benchmark = benchmarks[category] || benchmarks['Equity'];
  const periodBenchmark = benchmark[period];
  
  if (!periodBenchmark) return 50;
  
  if (returnValue >= periodBenchmark.excellent) return 100;
  if (returnValue >= periodBenchmark.good) return 75;
  if (returnValue >= periodBenchmark.average) return 50;
  if (returnValue >= periodBenchmark.poor) return 25;
  return 10;
}

async function updateFundScore(fundId, enhancements) {
  const updateFields = [];
  const updateValues = [];
  let paramCount = 1;
  
  if (enhancements.score5Y !== null) {
    updateFields.push(`return_5y_score = $${paramCount++}`);
    updateValues.push(enhancements.score5Y);
  }
  
  if (enhancements.scoreYTD !== null) {
    updateFields.push(`return_ytd_score = $${paramCount++}`);
    updateValues.push(enhancements.scoreYTD);
  }
  
  if (updateFields.length > 0) {
    updateValues.push(fundId);
    updateValues.push(new Date().toISOString().split('T')[0]);
    
    const query = `
      UPDATE fund_scores 
      SET ${updateFields.join(', ')}
      WHERE fund_id = $${paramCount++} 
      AND score_date = $${paramCount++}
    `;
    
    await pool.query(query, updateValues);
  }
}

updateScoresWith5YAndYTD();