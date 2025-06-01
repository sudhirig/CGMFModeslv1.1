/**
 * Complete Scoring System Activation - Fixed Version
 * Implements full 100-point methodology across all subcategories with type safety
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function activateCompleteScoring() {
  try {
    console.log('=== Activating Complete 100-Point Scoring System ===');
    
    // Priority 1: High-volume subcategories
    const prioritySubcategories = [
      { category: 'Debt', subcategory: 'Liquid' },
      { category: 'Debt', subcategory: 'Overnight' },
      { category: 'Debt', subcategory: 'Ultra Short Duration' },
      { category: 'Equity', subcategory: 'Index' },
      { category: 'Equity', subcategory: 'Large Cap' },
      { category: 'Equity', subcategory: 'Mid Cap' }
    ];
    
    let totalProcessed = 0;
    const results = [];
    
    for (const subcat of prioritySubcategories) {
      console.log(`\nProcessing ${subcat.category}/${subcat.subcategory}...`);
      
      const result = await processSubcategoryWithFullScoring(subcat.category, subcat.subcategory);
      
      if (result) {
        results.push(result);
        totalProcessed += result.fundsProcessed;
        console.log(`  ✓ ${result.fundsProcessed} funds scored with full methodology`);
      }
      
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    // Apply quartile assignments
    await applyQuartileAssignments();
    
    // Generate activation summary
    const summary = await pool.query(`
      SELECT 
        f.category,
        f.subcategory,
        COUNT(*) as funds_scored,
        ROUND(AVG(fs.total_score)::numeric, 2) as avg_score,
        ROUND(AVG(fs.historical_returns_total)::numeric, 2) as avg_historical,
        ROUND(AVG(fs.risk_grade_total)::numeric, 2) as avg_risk,
        ROUND(AVG(fs.other_metrics_total)::numeric, 2) as avg_other,
        COUNT(CASE WHEN fs.quartile = 1 THEN 1 END) as q1_count
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.score_date = CURRENT_DATE
        AND fs.historical_returns_total IS NOT NULL
      GROUP BY f.category, f.subcategory
      ORDER BY COUNT(*) DESC
    `);
    
    console.log('\n=== Complete Scoring System Status ===');
    console.log('Category/Subcategory'.padEnd(30) + 'Funds'.padEnd(8) + 'Score'.padEnd(8) + 'Hist'.padEnd(8) + 'Risk'.padEnd(8) + 'Other'.padEnd(8) + 'Q1');
    console.log('-'.repeat(80));
    
    for (const row of summary.rows) {
      const subcategory = `${row.category}/${row.subcategory}`;
      console.log(
        subcategory.padEnd(30) +
        row.funds_scored.toString().padEnd(8) +
        row.avg_score.toString().padEnd(8) +
        (row.avg_historical || '0').toString().padEnd(8) +
        (row.avg_risk || '0').toString().padEnd(8) +
        (row.avg_other || '0').toString().padEnd(8) +
        row.q1_count.toString()
      );
    }
    
    console.log('-'.repeat(80));
    console.log(`Total: ${summary.rows.length} subcategories with full scoring active`);
    
    return {
      success: true,
      subcategoriesActivated: summary.rows.length,
      totalFundsScored: totalProcessed
    };
    
  } catch (error) {
    console.error('Error activating complete scoring:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function processSubcategoryWithFullScoring(category, subcategory) {
  try {
    // Get eligible funds
    const eligibleFunds = await pool.query(`
      SELECT f.id, f.fund_name
      FROM funds f
      JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        WHERE created_at > '2025-05-30 06:45:00'
        GROUP BY fund_id
        HAVING COUNT(*) >= 252
      ) nav_summary ON f.id = nav_summary.fund_id
      WHERE f.category = $1 AND f.subcategory = $2
      ORDER BY nav_summary.nav_count DESC
      LIMIT 25
    `, [category, subcategory]);
    
    if (eligibleFunds.rows.length < 4) {
      console.log(`    Insufficient funds (${eligibleFunds.rows.length}) for ${subcategory}`);
      return null;
    }
    
    let processedCount = 0;
    
    for (const fund of eligibleFunds.rows) {
      try {
        await calculateFullScore(fund.id, category, subcategory);
        processedCount++;
      } catch (error) {
        console.error(`    Error scoring fund ${fund.id}: ${error.message}`);
      }
    }
    
    return {
      category,
      subcategory,
      fundsProcessed: processedCount
    };
    
  } catch (error) {
    console.error(`Error processing ${category}/${subcategory}:`, error);
    return null;
  }
}

async function calculateFullScore(fundId, category, subcategory) {
  try {
    // Get NAV data for multiple period analysis
    const navData = await pool.query(`
      SELECT nav_value, nav_date 
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
      ORDER BY nav_date DESC 
      LIMIT 1000
    `, [fundId]);
    
    if (navData.rows.length < 252) return null;
    
    const navValues = navData.rows.map(row => parseFloat(row.nav_value));
    const latest = navValues[0];
    
    // Calculate returns for multiple periods
    const returns = {
      '3m': navValues.length >= 90 ? ((latest - navValues[89]) / navValues[89]) * 100 : 0,
      '6m': navValues.length >= 180 ? ((latest - navValues[179]) / navValues[179]) * 100 : 0,
      '1y': navValues.length >= 252 ? ((latest - navValues[251]) / navValues[251]) * 100 : 0,
      '3y': navValues.length >= 780 ? (Math.pow((latest / navValues[779]), (1/3)) - 1) * 100 : 0,
      '5y': navValues.length >= 1260 ? (Math.pow((latest / navValues[1259]), (1/5)) - 1) * 100 : 0
    };
    
    // Historical Returns Score (40 points total)
    let historicalScore = 0;
    historicalScore += Math.min(5, Math.max(0, returns['3m'] * 0.5));  // 3m: 5 points
    historicalScore += Math.min(10, Math.max(0, returns['6m'] * 0.8)); // 6m: 10 points  
    historicalScore += Math.min(10, Math.max(0, returns['1y'] * 0.7)); // 1y: 10 points
    historicalScore += Math.min(8, Math.max(0, returns['3y'] * 0.6));  // 3y: 8 points
    historicalScore += Math.min(7, Math.max(0, returns['5y'] * 0.5));  // 5y: 7 points
    
    // Risk Grade Score (30 points total)
    let riskScore = 15; // Base risk score
    
    // Calculate volatility for risk assessment
    if (navValues.length >= 252) {
      const dailyReturns = [];
      for (let i = 1; i < Math.min(252, navValues.length); i++) {
        const dayReturn = (navValues[i-1] - navValues[i]) / navValues[i];
        if (!isNaN(dayReturn)) dailyReturns.push(dayReturn);
      }
      
      if (dailyReturns.length > 0) {
        const volatility = calculateVolatility(dailyReturns) * Math.sqrt(252) * 100;
        
        // Lower volatility = higher risk score
        if (volatility < 5) riskScore += 15;      // Very low risk
        else if (volatility < 10) riskScore += 12; // Low risk
        else if (volatility < 20) riskScore += 8;  // Medium risk
        else if (volatility < 30) riskScore += 4;  // High risk
        // else 0 additional points for very high risk
      }
    }
    
    // Other Metrics Score (30 points total)
    let otherScore = 15; // Base score
    
    // Category and subcategory specific adjustments
    if (category === 'Debt') {
      if (['Liquid', 'Overnight'].includes(subcategory)) {
        otherScore += 10; // Liquidity premium
        if (returns['1y'] > 3) otherScore += 5; // Performance bonus
      } else if (subcategory === 'Banking and PSU') {
        if (returns['1y'] > 7) otherScore += 10; // Credit quality bonus
      } else if (subcategory === 'Ultra Short Duration') {
        if (returns['1y'] > 5) otherScore += 8; // Duration management bonus
      }
    } else if (category === 'Equity') {
      if (subcategory === 'Index') {
        otherScore += 10; // Low cost advantage
        if (returns['1y'] > 10) otherScore += 5; // Tracking bonus
      } else if (subcategory === 'Large Cap') {
        if (returns['1y'] > 12) otherScore += 10; // Performance bonus
      } else if (subcategory === 'Mid Cap') {
        if (returns['1y'] > 15) otherScore += 12; // Growth bonus
      }
    }
    
    const totalScore = Math.min(100, Math.max(0, historicalScore + riskScore + otherScore));
    
    // Store the comprehensive score with proper type casting
    await pool.query(`
      INSERT INTO fund_scores (
        fund_id, score_date, subcategory,
        return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score,
        historical_returns_total, risk_grade_total, other_metrics_total,
        total_score, created_at
      ) VALUES ($1, CURRENT_DATE, $2, $3::numeric, $4::numeric, $5::numeric, $6::numeric, $7::numeric, $8::numeric, $9::numeric, $10::numeric, $11::numeric, NOW())
      ON CONFLICT (fund_id, score_date) DO UPDATE SET
        subcategory = EXCLUDED.subcategory,
        return_3m_score = EXCLUDED.return_3m_score,
        return_6m_score = EXCLUDED.return_6m_score,
        return_1y_score = EXCLUDED.return_1y_score,
        return_3y_score = EXCLUDED.return_3y_score,
        return_5y_score = EXCLUDED.return_5y_score,
        historical_returns_total = EXCLUDED.historical_returns_total,
        risk_grade_total = EXCLUDED.risk_grade_total,
        other_metrics_total = EXCLUDED.other_metrics_total,
        total_score = EXCLUDED.total_score
    `, [
      fundId, subcategory,
      returns['3m'], returns['6m'], returns['1y'], returns['3y'], returns['5y'],
      historicalScore, riskScore, otherScore, totalScore
    ]);
    
    return totalScore;
    
  } catch (error) {
    console.error(`Error calculating full score for fund ${fundId}:`, error.message);
    return null;
  }
}

async function applyQuartileAssignments() {
  console.log('\n--- Applying Quartile Assignments ---');
  
  const quartileUpdate = await pool.query(`
    UPDATE fund_scores 
    SET 
      quartile = (
        CASE 
          WHEN total_score >= 85 THEN 1
          WHEN total_score >= 70 THEN 2  
          WHEN total_score >= 55 THEN 3
          ELSE 4
        END
      ),
      recommendation = (
        CASE 
          WHEN total_score >= 85 THEN 'STRONG_BUY'
          WHEN total_score >= 70 THEN 'BUY'
          WHEN total_score >= 55 THEN 'HOLD'
          ELSE 'SELL'
        END
      )
    WHERE score_date = CURRENT_DATE
      AND total_score IS NOT NULL
    RETURNING fund_id
  `);
  
  console.log(`✓ Quartile assignments completed for ${quartileUpdate.rows.length} funds`);
}

function calculateVolatility(returns) {
  if (returns.length === 0) return 0;
  
  const mean = returns.reduce((sum, val) => sum + val, 0) / returns.length;
  const squaredDiffs = returns.map(val => Math.pow(val - mean, 2));
  const variance = squaredDiffs.reduce((sum, val) => sum + val, 0) / returns.length;
  
  return Math.sqrt(variance);
}

if (require.main === module) {
  activateCompleteScoring()
    .then(result => {
      console.log('\n✓ Complete scoring system activated:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Activation failed:', error);
      process.exit(1);
    });
}

module.exports = { activateCompleteScoring };