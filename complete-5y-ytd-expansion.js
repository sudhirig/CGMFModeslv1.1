/**
 * Complete 5Y and YTD Analysis Expansion
 * Systematically processes all eligible funds to achieve maximum coverage
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function complete5YAndYTDExpansion() {
  try {
    console.log('Starting comprehensive 5Y and YTD analysis expansion...\n');
    
    // Get funds eligible for 5Y analysis
    const funds5Y = await pool.query(`
      SELECT DISTINCT f.id, f.fund_name, f.category
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
      )
      AND (fs.return_5y_score IS NULL)
      AND EXISTS (
        SELECT 1 FROM nav_data nd2 
        WHERE nd2.fund_id = f.id 
        GROUP BY nd2.fund_id
        HAVING COUNT(*) >= 1000
      )
      ORDER BY f.id
      LIMIT 500
    `);
    
    console.log(`Found ${funds5Y.rows.length} funds eligible for 5Y analysis`);
    
    // Get funds eligible for YTD analysis
    const fundsYTD = await pool.query(`
      SELECT DISTINCT f.id, f.fund_name, f.category
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
        AND nd.nav_date <= DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '20 days'
      )
      AND (fs.return_ytd_score IS NULL)
      AND EXISTS (
        SELECT 1 FROM nav_data nd2 
        WHERE nd2.fund_id = f.id 
        GROUP BY nd2.fund_id
        HAVING COUNT(*) >= 100
      )
      ORDER BY f.id
      LIMIT 1000
    `);
    
    console.log(`Found ${fundsYTD.rows.length} funds eligible for YTD analysis\n`);
    
    let processed5Y = 0;
    let processedYTD = 0;
    let updated5Y = 0;
    let updatedYTD = 0;
    
    // Process 5Y analysis
    console.log('=== Processing 5Y Analysis ===');
    for (const fund of funds5Y.rows) {
      try {
        processed5Y++;
        const analysis = await calculate5YReturn(fund.id);
        
        if (analysis.return5Y !== null) {
          await update5YScore(fund.id, analysis);
          updated5Y++;
          
          if (updated5Y % 50 === 0) {
            console.log(`✓ 5Y Progress: ${updated5Y}/${funds5Y.rows.length} funds updated`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing 5Y for fund ${fund.id}: ${error.message}`);
      }
    }
    
    // Process YTD analysis
    console.log('\n=== Processing YTD Analysis ===');
    for (const fund of fundsYTD.rows) {
      try {
        processedYTD++;
        const analysis = await calculateYTDReturn(fund.id);
        
        if (analysis.returnYTD !== null) {
          await updateYTDScore(fund.id, analysis);
          updatedYTD++;
          
          if (updatedYTD % 100 === 0) {
            console.log(`✓ YTD Progress: ${updatedYTD}/${fundsYTD.rows.length} funds updated`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing YTD for fund ${fund.id}: ${error.message}`);
      }
    }
    
    // Final coverage report
    const finalCoverage = await pool.query(`
      SELECT 
        COUNT(*) as total_scores,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as scores_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as scores_ytd,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const coverage = finalCoverage.rows[0];
    
    console.log(`\n=== Expansion Results ===`);
    console.log(`5Y Analysis - Processed: ${processed5Y}, Updated: ${updated5Y}`);
    console.log(`YTD Analysis - Processed: ${processedYTD}, Updated: ${updatedYTD}`);
    console.log(`\nFinal Coverage:`);
    console.log(`5Y Analysis: ${coverage.scores_5y}/${coverage.total_scores} funds (${coverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${coverage.scores_ytd}/${coverage.total_scores} funds (${coverage.pct_ytd}%)`);
    
  } catch (error) {
    console.error('Error in comprehensive expansion:', error);
  } finally {
    await pool.end();
  }
}

async function calculate5YReturn(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2000
  `, [fundId]);
  
  if (navData.rows.length < 1000) {
    return { return5Y: null };
  }
  
  const records = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = records[0].value;
  const currentDate = records[0].date;
  
  // Find NAV closest to 5 years ago
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  let best5YMatch = null;
  let minDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff && daysDiff <= 60) {
      minDiff = daysDiff;
      best5YMatch = record;
    }
  }
  
  if (best5YMatch) {
    const return5Y = ((currentNav - best5YMatch.value) / best5YMatch.value) * 100;
    return { return5Y };
  }
  
  return { return5Y: null };
}

async function calculateYTDReturn(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 500
  `, [fundId]);
  
  if (navData.rows.length < 50) {
    return { returnYTD: null };
  }
  
  const records = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = records[0].value;
  const currentDate = records[0].date;
  
  // Find NAV closest to start of current year
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  let bestYTDMatch = null;
  let minDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff && daysDiff <= 20) {
      minDiff = daysDiff;
      bestYTDMatch = record;
    }
  }
  
  if (bestYTDMatch) {
    const returnYTD = ((currentNav - bestYTDMatch.value) / bestYTDMatch.value) * 100;
    return { returnYTD };
  }
  
  return { returnYTD: null };
}

async function update5YScore(fundId, analysis) {
  const score5Y = calculateReturnScore(analysis.return5Y, '5y');
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_5y_score = $1
    WHERE fund_id = $2
  `, [score5Y, fundId]);
}

async function updateYTDScore(fundId, analysis) {
  const scoreYTD = calculateReturnScore(analysis.returnYTD, 'ytd');
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_ytd_score = $1
    WHERE fund_id = $2
  `, [scoreYTD, fundId]);
}

function calculateReturnScore(returnValue, period) {
  if (period === '5y') {
    // 5-year return scoring
    if (returnValue >= 150) return 100;     // Exceptional: >150%
    if (returnValue >= 100) return 90;      // Excellent: 100-150%
    if (returnValue >= 75) return 80;       // Very good: 75-100%
    if (returnValue >= 50) return 70;       // Good: 50-75%
    if (returnValue >= 25) return 60;       // Average: 25-50%
    if (returnValue >= 0) return 50;        // Below average: 0-25%
    if (returnValue >= -25) return 30;      // Poor: 0 to -25%
    return 10;                              // Very poor: <-25%
  } else if (period === 'ytd') {
    // YTD return scoring
    if (returnValue >= 25) return 100;      // Exceptional: >25% YTD
    if (returnValue >= 15) return 90;       // Excellent: 15-25%
    if (returnValue >= 10) return 80;       // Very good: 10-15%
    if (returnValue >= 5) return 70;        // Good: 5-10%
    if (returnValue >= 0) return 60;        // Average: 0-5%
    if (returnValue >= -5) return 40;       // Below average: 0 to -5%
    if (returnValue >= -15) return 20;      // Poor: -5 to -15%
    return 10;                              // Very poor: <-15%
  }
  
  return 50; // Default score
}

complete5YAndYTDExpansion();