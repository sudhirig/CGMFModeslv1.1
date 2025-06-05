/**
 * Maximize 5Y and YTD Coverage
 * Final push to achieve maximum possible coverage across all eligible funds
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function maximize5YAndYTDCoverage() {
  try {
    console.log('Maximizing 5Y and YTD coverage across all eligible funds...\n');
    
    // Get remaining eligible funds for 5Y analysis
    const remaining5Y = await pool.query(`
      SELECT f.id, f.fund_name, f.category, COUNT(nd.nav_value) as nav_count
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd2 
        WHERE nd2.fund_id = f.id 
        AND nd2.nav_date <= CURRENT_DATE - INTERVAL '4 years'
      )
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(nd.nav_value) >= 400
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 600
    `);
    
    console.log(`Found ${remaining5Y.rows.length} funds eligible for 5Y analysis`);
    
    // Get remaining eligible funds for YTD analysis
    const remainingYTD = await pool.query(`
      SELECT f.id, f.fund_name, f.category, COUNT(nd.nav_value) as nav_count
      FROM funds f
      JOIN fund_scores fs ON f.id = fs.fund_id
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd2 
        WHERE nd2.fund_id = f.id 
        AND nd2.nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 months'
      )
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(nd.nav_value) >= 100
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 800
    `);
    
    console.log(`Found ${remainingYTD.rows.length} funds eligible for YTD analysis\n`);
    
    let added5Y = 0;
    let addedYTD = 0;
    
    // Process 5Y analysis
    console.log('=== Processing 5Y Analysis ===');
    for (const fund of remaining5Y.rows) {
      try {
        const return5Y = await calculate5YReturn(fund.id);
        
        if (return5Y !== null) {
          await update5YScore(fund.id, return5Y);
          added5Y++;
          
          if (added5Y % 50 === 0) {
            console.log(`  5Y Progress: ${added5Y}/${remaining5Y.rows.length} funds processed`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing 5Y for fund ${fund.id}: ${error.message}`);
      }
    }
    
    // Process YTD analysis
    console.log('\n=== Processing YTD Analysis ===');
    for (const fund of remainingYTD.rows) {
      try {
        const returnYTD = await calculateYTDReturn(fund.id);
        
        if (returnYTD !== null) {
          await updateYTDScore(fund.id, returnYTD);
          addedYTD++;
          
          if (addedYTD % 75 === 0) {
            console.log(`  YTD Progress: ${addedYTD}/${remainingYTD.rows.length} funds processed`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing YTD for fund ${fund.id}: ${error.message}`);
      }
    }
    
    // Final coverage report
    const finalCoverage = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as funds_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as funds_ytd,
        COUNT(CASE WHEN return_5y_score IS NOT NULL AND return_ytd_score IS NOT NULL THEN 1 END) as complete_coverage,
        ROUND(COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_5y,
        ROUND(COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as pct_ytd
      FROM fund_scores
    `);
    
    const coverage = finalCoverage.rows[0];
    
    console.log(`\n=== MAXIMUM COVERAGE ACHIEVED ===`);
    console.log(`Added in this run: +${added5Y} 5Y scores, +${addedYTD} YTD scores`);
    console.log(`\nFinal System Coverage:`);
    console.log(`5Y Analysis: ${coverage.funds_5y}/${coverage.total_funds} funds (${coverage.pct_5y}%)`);
    console.log(`YTD Analysis: ${coverage.funds_ytd}/${coverage.total_funds} funds (${coverage.pct_ytd}%)`);
    console.log(`Complete Coverage: ${coverage.complete_coverage}/${coverage.total_funds} funds`);
    
    // Show top performers with complete analysis
    const topPerformers = await pool.query(`
      SELECT f.fund_name, f.category, fs.return_5y_score, fs.return_ytd_score, fs.total_score
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.return_5y_score IS NOT NULL AND fs.return_ytd_score IS NOT NULL
      ORDER BY fs.total_score DESC
      LIMIT 5
    `);
    
    console.log(`\nTop Performers with Complete Analysis:`);
    topPerformers.rows.forEach((fund, i) => {
      console.log(`${i+1}. ${fund.fund_name} (${fund.category})`);
      console.log(`   5Y Score: ${fund.return_5y_score}, YTD Score: ${fund.return_ytd_score}, Total: ${fund.total_score}`);
    });
    
  } catch (error) {
    console.error('Error maximizing coverage:', error);
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
  
  if (navData.rows.length < 300) return null;
  
  const records = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = records[0].value;
  const currentDate = records[0].date;
  const fiveYearsAgo = new Date(currentDate);
  fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
  
  // Find best match within 6 months of target date
  let bestMatch = null;
  let minDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - fiveYearsAgo) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff && daysDiff <= 180) {
      minDiff = daysDiff;
      bestMatch = record;
    }
  }
  
  if (bestMatch) {
    return ((currentNav - bestMatch.value) / bestMatch.value) * 100;
  }
  
  return null;
}

async function calculateYTDReturn(fundId) {
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 400
  `, [fundId]);
  
  if (navData.rows.length < 50) return null;
  
  const records = navData.rows.map(row => ({
    date: new Date(row.nav_date),
    value: parseFloat(row.nav_value)
  }));
  
  const currentNav = records[0].value;
  const currentDate = records[0].date;
  const yearStart = new Date(currentDate.getFullYear(), 0, 1);
  
  // Find best match within 2 months of year start
  let bestMatch = null;
  let minDiff = Infinity;
  
  for (const record of records) {
    const daysDiff = Math.abs((record.date - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDiff && daysDiff <= 60) {
      minDiff = daysDiff;
      bestMatch = record;
    }
  }
  
  if (bestMatch) {
    return ((currentNav - bestMatch.value) / bestMatch.value) * 100;
  }
  
  return null;
}

async function update5YScore(fundId, return5Y) {
  const score = score5YReturn(return5Y);
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_5y_score = $1
    WHERE fund_id = $2
  `, [score, fundId]);
}

async function updateYTDScore(fundId, returnYTD) {
  const score = scoreYTDReturn(returnYTD);
  
  await pool.query(`
    UPDATE fund_scores 
    SET return_ytd_score = $1
    WHERE fund_id = $2
  `, [score, fundId]);
}

function score5YReturn(returnValue) {
  if (returnValue >= 500) return 100;
  if (returnValue >= 300) return 95;
  if (returnValue >= 200) return 90;
  if (returnValue >= 150) return 85;
  if (returnValue >= 100) return 80;
  if (returnValue >= 75) return 75;
  if (returnValue >= 50) return 70;
  if (returnValue >= 25) return 60;
  if (returnValue >= 0) return 50;
  if (returnValue >= -20) return 35;
  return 20;
}

function scoreYTDReturn(returnValue) {
  if (returnValue >= 50) return 100;
  if (returnValue >= 30) return 90;
  if (returnValue >= 20) return 80;
  if (returnValue >= 15) return 75;
  if (returnValue >= 10) return 70;
  if (returnValue >= 5) return 60;
  if (returnValue >= 0) return 50;
  if (returnValue >= -10) return 35;
  return 20;
}

maximize5YAndYTDCoverage();