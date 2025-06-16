/**
 * Fix and add 5Y and YTD scoring to fund analysis
 * Simple approach to enhance existing scoring with missing time periods
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function fix5YAndYTDScoring() {
  try {
    console.log('Enhancing fund scores with 5Y and YTD analysis...\n');
    
    // Get funds with existing scores that need 5Y/YTD updates
    const scoredFunds = await pool.query(`
      SELECT 
        fs.fund_id,
        f.fund_name,
        f.category,
        fs.score_date,
        fs.return_5y_score,
        fs.return_ytd_score
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE (fs.return_5y_score IS NULL OR fs.return_ytd_score IS NULL)
      ORDER BY fs.fund_id
      LIMIT 50
    `);
    
    console.log(`Found ${scoredFunds.rows.length} fund scores needing 5Y/YTD analysis`);
    
    let updated = 0;
    
    for (const score of scoredFunds.rows) {
      try {
        console.log(`Processing ${score.fund_name}...`);
        
        // Check if fund has sufficient NAV data
        const navCheck = await pool.query(`
          SELECT 
            COUNT(*) as total_records,
            MIN(nav_date) as earliest_date,
            MAX(nav_date) as latest_date
          FROM nav_data 
          WHERE fund_id = $1
        `, [score.fund_id]);
        
        const navInfo = navCheck.rows[0];
        const totalRecords = parseInt(navInfo.total_records);
        const earliestDate = new Date(navInfo.earliest_date);
        const currentDate = new Date();
        
        console.log(`  ${totalRecords} NAV records from ${navInfo.earliest_date} to ${navInfo.latest_date}`);
        
        if (totalRecords < 200) {
          console.log(`  ⚠ Insufficient data (${totalRecords} records)`);
          continue;
        }
        
        const timeAnalysis = await calculateTimeReturns(score.fund_id);
        
        if (timeAnalysis.updated) {
          await updateFundScores(score.fund_id, score.score_date, timeAnalysis);
          updated++;
          console.log(`  ✓ Enhanced with 5Y: ${timeAnalysis.return5Y?.toFixed(1)}%, YTD: ${timeAnalysis.returnYTD?.toFixed(1)}%`);
        } else {
          console.log(`  ⚠ Could not calculate enhanced returns`);
        }
        
      } catch (error) {
        console.error(`  ✗ Error processing ${score.fund_name}: ${error.message}`);
      }
    }
    
    console.log(`\n=== Enhancement Results ===`);
    console.log(`Successfully enhanced: ${updated} fund scores`);
    console.log(`Fund analysis now includes comprehensive time period coverage`);
    
  } catch (error) {
    console.error('Error enhancing 5Y and YTD scoring:', error);
  } finally {
    await pool.end();
  }
}

async function calculateTimeReturns(fundId) {
  // Get recent NAV data for calculations
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
  let minDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff && daysDiff <= 60) {
      minDiff = daysDiff;
      best5YMatch = record;
    }
  }
  
  if (best5YMatch) {
    return5Y = ((currentNav - best5YMatch.value) / best5YMatch.value) * 100;
  }
  
  // Calculate YTD return
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  let bestYTDMatch = null;
  let minYTDDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minYTDDiff && daysDiff <= 15) {
      minYTDDiff = daysDiff;
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

async function updateFundScores(fundId, scoreDate, analysis) {
  const updateParts = [];
  const values = [];
  let paramIndex = 1;
  
  if (analysis.return5Y !== null) {
    const score5Y = calculateScore(analysis.return5Y, 'fiveYear');
    updateParts.push(`return_5y_score = $${paramIndex++}`);
    values.push(score5Y);
  }
  
  if (analysis.returnYTD !== null) {
    const scoreYTD = calculateScore(analysis.returnYTD, 'ytd');
    updateParts.push(`return_ytd_score = $${paramIndex++}`);
    values.push(scoreYTD);
  }
  
  if (updateParts.length > 0) {
    values.push(fundId, scoreDate);
    
    await pool.query(`
      UPDATE fund_scores 
      SET ${updateParts.join(', ')}
      WHERE fund_id = $${paramIndex++} AND score_date = $${paramIndex++}
    `, values);
  }
}

function calculateScore(returnValue, period) {
  // Conservative scoring based on return performance
  if (period === 'fiveYear') {
    // 5-year return scoring
    if (returnValue >= 100) return 100;      // Excellent: >100% over 5 years
    if (returnValue >= 75) return 85;        // Very good: 75-100%
    if (returnValue >= 50) return 70;        // Good: 50-75%
    if (returnValue >= 25) return 55;        // Average: 25-50%
    if (returnValue >= 0) return 40;         // Below average: 0-25%
    return 20;                               // Poor: negative returns
  } else if (period === 'ytd') {
    // YTD return scoring
    if (returnValue >= 20) return 100;       // Excellent: >20% YTD
    if (returnValue >= 10) return 80;        // Very good: 10-20%
    if (returnValue >= 5) return 65;         // Good: 5-10%
    if (returnValue >= 0) return 50;         // Average: 0-5%
    if (returnValue >= -5) return 35;        // Below average: 0 to -5%
    return 20;                               // Poor: <-5%
  }
  
  return 50; // Default score
}

fix5YAndYTDScoring();