/**
 * Complete scoring for all remaining funds with sufficient NAV data
 * Process the 851 funds that have 30+ NAV records but no scores yet
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function completeRemainingScoring() {
  try {
    console.log('Starting comprehensive scoring for all remaining ready funds...');
    
    // Get all unscored funds with sufficient NAV data
    const unscoredQuery = `
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        f.subcategory,
        COUNT(nd.nav_value) as nav_count,
        MAX(nd.nav_date) as latest_nav_date
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.fund_id IS NULL
      GROUP BY f.id, f.fund_name, f.category, f.subcategory
      HAVING COUNT(nd.nav_value) >= 30
      ORDER BY COUNT(nd.nav_value) DESC
    `;
    
    const unscored = await pool.query(unscoredQuery);
    console.log(`Found ${unscored.rows.length} funds ready for scoring`);
    
    let batchSize = 25;
    let totalScored = 0;
    let totalFailed = 0;
    
    // Process in batches to avoid timeouts
    for (let i = 0; i < unscored.rows.length; i += batchSize) {
      const batch = unscored.rows.slice(i, i + batchSize);
      console.log(`\nProcessing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(unscored.rows.length/batchSize)}: ${batch.length} funds`);
      
      for (const fund of batch) {
        try {
          await scoreSingleFund(fund);
          totalScored++;
          console.log(`✓ [${totalScored}/${unscored.rows.length}] Scored ${fund.fund_name}`);
        } catch (error) {
          totalFailed++;
          console.error(`✗ Failed to score fund ${fund.id}: ${error.message}`);
        }
        
        // Small delay between funds
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      
      // Longer delay between batches
      await new Promise(resolve => setTimeout(resolve, 200));
    }
    
    console.log(`\nScoring complete!`);
    console.log(`Successfully scored: ${totalScored} funds`);
    console.log(`Failed: ${totalFailed} funds`);
    console.log(`Total funds with scores: ${totalScored + 13293}`);
    
  } catch (error) {
    console.error('Error in comprehensive scoring:', error);
  } finally {
    await pool.end();
  }
}

async function scoreSingleFund(fund) {
  // Get NAV data for calculations
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 1000
  `, [fund.id]);
  
  if (navData.rows.length < 30) {
    throw new Error('Insufficient NAV data');
  }
  
  const navValues = navData.rows.map(n => parseFloat(n.nav_value));
  const dates = navData.rows.map(n => new Date(n.nav_date));
  
  // Calculate returns for available periods
  const currentNav = navValues[0];
  const returns = {};
  
  const periods = [
    { name: '1m', days: 30, weight: 0.1 },
    { name: '3m', days: 90, weight: 0.2 },
    { name: '6m', days: 180, weight: 0.25 },
    { name: '1y', days: 365, weight: 0.3 },
    { name: '3y', days: 1095, weight: 0.15 }
  ];
  
  for (const period of periods) {
    if (navValues.length > period.days) {
      const pastNav = navValues[period.days];
      returns[period.name] = ((currentNav - pastNav) / pastNav) * 100;
    }
  }
  
  // Calculate risk metrics
  const dailyReturns = [];
  for (let i = 1; i < Math.min(navValues.length, 500); i++) {
    const dailyReturn = (navValues[i-1] - navValues[i]) / navValues[i];
    dailyReturns.push(dailyReturn);
  }
  
  const avgReturn = dailyReturns.reduce((sum, r) => sum + r, 0) / dailyReturns.length;
  const variance = dailyReturns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / dailyReturns.length;
  const volatility = Math.sqrt(variance * 252) * 100;
  
  // Calculate maximum drawdown
  let maxDrawdown = 0;
  let peak = navValues[navValues.length - 1];
  
  for (let i = navValues.length - 1; i >= 0; i--) {
    if (navValues[i] > peak) {
      peak = navValues[i];
    } else {
      const drawdown = (peak - navValues[i]) / peak * 100;
      if (drawdown > maxDrawdown) {
        maxDrawdown = drawdown;
      }
    }
  }
  
  // Calculate component scores
  const returnScores = calculateReturnScores(returns, fund.category);
  const riskScores = calculateRiskScores(volatility, maxDrawdown);
  const qualityScores = calculateQualityScores(fund.nav_count, dailyReturns.length);
  
  const totalScore = returnScores.total + riskScores.total + qualityScores.total;
  const quartile = totalScore >= 75 ? 1 : totalScore >= 50 ? 2 : totalScore >= 25 ? 3 : 4;
  
  const recommendation = quartile <= 2 ? 'BUY' : quartile === 3 ? 'HOLD' : 'SELL';
  
  // Insert comprehensive score record
  await pool.query(`
    INSERT INTO fund_scores (
      fund_id, score_date, 
      return_3m_score, return_6m_score, return_1y_score, return_3y_score,
      historical_returns_total,
      std_dev_1y_score, max_drawdown_score,
      risk_grade_total,
      aum_size_score, expense_ratio_score,
      other_metrics_total,
      total_score, quartile, recommendation,
      subcategory, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
  `, [
    fund.id,
    new Date().toISOString().split('T')[0],
    returnScores.return_3m || 0,
    returnScores.return_6m || 0,
    returnScores.return_1y || 0,
    returnScores.return_3y || 0,
    returnScores.total,
    riskScores.volatility,
    riskScores.drawdown,
    riskScores.total,
    qualityScores.data_quality,
    qualityScores.consistency,
    qualityScores.total,
    totalScore,
    quartile,
    recommendation,
    fund.subcategory,
    new Date()
  ]);
}

function calculateReturnScores(returns, category) {
  const scores = {};
  let total = 0;
  
  // Category-based benchmarks
  const benchmarks = {
    'Equity': { excellent: 15, good: 10, average: 5, poor: 0 },
    'Debt': { excellent: 8, good: 6, average: 4, poor: 2 },
    'Hybrid': { excellent: 12, good: 8, average: 4, poor: 0 }
  };
  
  const benchmark = benchmarks[category] || benchmarks['Equity'];
  
  for (const [period, returnValue] of Object.entries(returns)) {
    let score = 0;
    if (returnValue >= benchmark.excellent) score = 100;
    else if (returnValue >= benchmark.good) score = 75;
    else if (returnValue >= benchmark.average) score = 50;
    else if (returnValue >= benchmark.poor) score = 25;
    else score = 10;
    
    scores[`return_${period}`] = score;
    total += score * 0.2; // Weight each period equally
  }
  
  scores.total = Math.min(40, total); // Cap at 40 points
  return scores;
}

function calculateRiskScores(volatility, maxDrawdown) {
  // Lower volatility and drawdown = higher scores
  const volatilityScore = Math.max(0, Math.min(100, 100 - volatility * 2));
  const drawdownScore = Math.max(0, Math.min(100, 100 - maxDrawdown * 3));
  
  return {
    volatility: volatilityScore,
    drawdown: drawdownScore,
    total: (volatilityScore + drawdownScore) * 0.15 // 30 points total for risk
  };
}

function calculateQualityScores(navCount, dailyReturnsCount) {
  // Data quality based on available data
  const dataQualityScore = Math.min(100, (navCount / 500) * 100);
  const consistencyScore = Math.min(100, (dailyReturnsCount / 252) * 100);
  
  return {
    data_quality: dataQualityScore,
    consistency: consistencyScore,
    total: (dataQualityScore + consistencyScore) * 0.15 // 30 points total
  };
}

completeRemainingScoring();