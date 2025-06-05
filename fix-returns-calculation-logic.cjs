/**
 * Fix Returns Calculation Logic
 * Targeted fix for the 19 funds with null returns but abundant authentic data
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function fixReturnsCalculationLogic() {
  try {
    console.log('=== Fixing Returns Calculation Logic ===');
    console.log('Targeted fix for 19 funds with null returns but abundant authentic AMFI data');
    
    // Step 1: Get the exact 19 funds with null returns
    const nullReturnsFunds = await pool.query(`
      SELECT fs.fund_id, f.fund_name, f.subcategory
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.score_date = CURRENT_DATE
        AND fs.historical_returns_total IS NULL
      ORDER BY fs.fund_id
    `);
    
    console.log(`\nProcessing ${nullReturnsFunds.rows.length} funds with null returns but complete NAV data...`);
    
    let fixed = 0;
    let failed = 0;
    
    for (const fund of nullReturnsFunds.rows) {
      try {
        console.log(`\nProcessing Fund ${fund.fund_id}: ${fund.fund_name}`);
        
        // Get authentic NAV data for this fund
        const navData = await pool.query(`
          SELECT nav_value, nav_date
          FROM nav_data 
          WHERE fund_id = $1 
            AND created_at > '2025-05-30 06:45:00'
            AND nav_value > 0
          ORDER BY nav_date ASC
        `, [fund.fund_id]);
        
        console.log(`  Retrieved ${navData.rows.length} authentic NAV records`);
        
        if (navData.rows.length >= 252) {
          const returns = await calculateAuthenticReturns(navData.rows);
          
          if (returns && returns.total >= 0) {
            await updateReturnsScoring(fund.fund_id, returns);
            console.log(`  ✓ Fixed: ${returns.total.toFixed(1)} points calculated from authentic data`);
            fixed++;
          } else {
            console.log(`  ⚠️  Calculation failed for fund ${fund.fund_id}`);
            failed++;
          }
        } else {
          console.log(`  ⚠️  Insufficient data: ${navData.rows.length} records`);
          failed++;
        }
        
      } catch (error) {
        console.error(`  Error processing fund ${fund.fund_id}:`, error.message);
        failed++;
      }
    }
    
    console.log(`\n✓ Fixed ${fixed} funds, ${failed} failed`);
    
    // Step 2: Verify the fixes
    await verifyFixes();
    
    console.log('\n✓ Returns calculation logic fix completed');
    
  } catch (error) {
    console.error('Fix process error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function calculateAuthenticReturns(navRecords) {
  try {
    // Convert to proper format with validation
    const navValues = navRecords.map(row => ({
      value: parseFloat(row.nav_value),
      date: new Date(row.nav_date)
    })).filter(nav => 
      nav.value > 0 && 
      isFinite(nav.value) && 
      nav.date instanceof Date && 
      !isNaN(nav.date.getTime())
    );
    
    if (navValues.length < 252) {
      return null;
    }
    
    // Sort by date to ensure proper chronological order
    navValues.sort((a, b) => a.date - b.date);
    
    const latest = navValues[navValues.length - 1];
    const periods = [
      { name: '3m', days: 90, maxScore: 8 },
      { name: '6m', days: 180, maxScore: 8 },
      { name: '1y', days: 365, maxScore: 8 },
      { name: '3y', days: 1095, maxScore: 8 },
      { name: '5y', days: 1825, maxScore: 8 }
    ];
    
    const returnScores = {};
    let totalScore = 0;
    
    console.log(`    Latest NAV: ${latest.value} on ${latest.date.toISOString().slice(0,10)}`);
    
    for (const period of periods) {
      const targetDate = new Date(latest.date);
      targetDate.setDate(targetDate.getDate() - period.days);
      
      // Find the NAV closest to target date
      let closestNav = null;
      let minDiff = Infinity;
      
      for (const nav of navValues) {
        const diff = Math.abs(nav.date - targetDate);
        if (diff < minDiff) {
          minDiff = diff;
          closestNav = nav;
        }
      }
      
      if (closestNav && closestNav.value > 0) {
        const daysDiff = Math.round((latest.date - closestNav.date) / (1000 * 60 * 60 * 24));
        const totalReturn = (latest.value - closestNav.value) / closestNav.value;
        
        if (isFinite(totalReturn) && daysDiff > 0) {
          // Calculate annualized return
          const annualizedReturn = Math.pow(1 + totalReturn, 365 / daysDiff) - 1;
          
          if (isFinite(annualizedReturn)) {
            const score = calculateAuthenticReturnScore(annualizedReturn * 100, period.maxScore);
            returnScores[period.name] = score;
            totalScore += score;
            
            console.log(`    ${period.name}: ${(totalReturn * 100).toFixed(2)}% (${daysDiff} days) = ${score.toFixed(1)} points`);
          } else {
            returnScores[period.name] = 0;
            console.log(`    ${period.name}: Invalid annualized return calculation`);
          }
        } else {
          returnScores[period.name] = 0;
          console.log(`    ${period.name}: Invalid return calculation (${totalReturn})`);
        }
      } else {
        returnScores[period.name] = 0;
        console.log(`    ${period.name}: No historical NAV found`);
      }
    }
    
    return {
      return_3m_score: returnScores['3m'] || 0,
      return_6m_score: returnScores['6m'] || 0,
      return_1y_score: returnScores['1y'] || 0,
      return_3y_score: returnScores['3y'] || 0,
      return_5y_score: returnScores['5y'] || 0,
      total: Math.min(40, Math.max(0, totalScore))
    };
    
  } catch (error) {
    console.error('Authentic returns calculation error:', error);
    return null;
  }
}

function calculateAuthenticReturnScore(annualizedReturnPercent, maxScore) {
  // Robust scoring function with validation
  if (!isFinite(annualizedReturnPercent)) return 0;
  
  const returnVal = Math.max(-50, Math.min(100, annualizedReturnPercent)); // Cap extreme values
  
  // Performance-based scoring thresholds
  if (returnVal >= 20) return maxScore;
  if (returnVal >= 15) return maxScore * 0.9;
  if (returnVal >= 12) return maxScore * 0.8;
  if (returnVal >= 8) return maxScore * 0.6;
  if (returnVal >= 5) return maxScore * 0.4;
  if (returnVal >= 0) return maxScore * 0.2;
  if (returnVal >= -5) return maxScore * 0.1;
  return 0;
}

async function updateReturnsScoring(fundId, returns) {
  // Update with authentic calculations
  await pool.query(`
    UPDATE fund_scores SET
      return_3m_score = $1,
      return_6m_score = $2,
      return_1y_score = $3,
      return_3y_score = $4,
      return_5y_score = $5,
      historical_returns_total = $6,
      total_score = COALESCE($6, 0) + COALESCE(risk_grade_total, 0) + COALESCE(fundamentals_total, 0)
    WHERE fund_id = $7 AND score_date = CURRENT_DATE
  `, [
    returns.return_3m_score,
    returns.return_6m_score,
    returns.return_1y_score,
    returns.return_3y_score,
    returns.return_5y_score,
    returns.total,
    fundId
  ]);
}

async function verifyFixes() {
  console.log('\nVerifying fixes...');
  
  const verification = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN historical_returns_total IS NULL THEN 1 END) as null_returns,
      COUNT(CASE WHEN historical_returns_total > 0 THEN 1 END) as positive_returns,
      COUNT(CASE WHEN historical_returns_total = 0 THEN 1 END) as zero_returns,
      ROUND(AVG(historical_returns_total), 2) as avg_returns,
      ROUND(AVG(total_score), 2) as avg_total_score,
      COUNT(CASE WHEN historical_returns_total IS NOT NULL AND risk_grade_total IS NOT NULL AND fundamentals_total IS NOT NULL THEN 1 END) as complete_100_point
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const result = verification.rows[0];
  
  console.log('Final Status:');
  console.log(`  Total Funds: ${result.total_funds}`);
  console.log(`  Null Returns: ${result.null_returns}`);
  console.log(`  Positive Returns: ${result.positive_returns}`);
  console.log(`  Zero Returns: ${result.zero_returns}`);
  console.log(`  Average Returns Score: ${result.avg_returns}/40 points`);
  console.log(`  Average Total Score: ${result.avg_total_score}/100 points`);
  console.log(`  Complete 100-Point Scoring: ${result.complete_100_point} funds`);
  
  const successRate = ((result.total_funds - result.null_returns) / result.total_funds * 100).toFixed(1);
  console.log(`  Success Rate: ${successRate}%`);
  
  if (result.null_returns === 0) {
    console.log('  ✓ ALL NULL VALUES RESOLVED - 100% SUCCESS');
  } else {
    console.log(`  ⚠️  ${result.null_returns} funds still have null returns`);
  }
}

if (require.main === module) {
  fixReturnsCalculationLogic()
    .then(() => {
      console.log('\n✓ Returns calculation logic fix completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fix failed:', error);
      process.exit(1);
    });
}

module.exports = { fixReturnsCalculationLogic };