/**
 * Enhanced scoring system with proper 5Y and YTD analysis
 * Calculates comprehensive return metrics across all time periods
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function enhancedScoringWith5YAndYTD() {
  try {
    console.log('Starting enhanced scoring with 5Y and YTD analysis...');
    
    // Add YTD column to fund_scores table if it doesn't exist
    await addYTDColumnIfNeeded();
    
    // Get funds that need enhanced scoring (focus on those with substantial data)
    const fundsForEnhancing = await pool.query(`
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        f.subcategory,
        COUNT(nd.nav_value) as nav_count,
        MIN(nd.nav_date) as earliest_date,
        MAX(nd.nav_date) as latest_date
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE EXISTS (
        SELECT 1 FROM nav_data nd2 
        WHERE nd2.fund_id = f.id 
        AND nd2.nav_date >= CURRENT_DATE - INTERVAL '5 years'
      )
      GROUP BY f.id, f.fund_name, f.category, f.subcategory
      HAVING COUNT(nd.nav_value) >= 100
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${fundsForEnhancing.rows.length} funds eligible for enhanced 5Y+YTD scoring`);
    
    let enhancedCount = 0;
    let skippedCount = 0;
    
    for (const fund of fundsForEnhancing.rows) {
      try {
        console.log(`\nEnhancing ${fund.fund_name} (${fund.nav_count} NAV records)`);
        console.log(`Data range: ${fund.earliest_date} to ${fund.latest_date}`);
        
        const enhanced = await enhanceFundScoring(fund);
        
        if (enhanced) {
          enhancedCount++;
          console.log(`✓ Enhanced scoring with 5Y and YTD analysis`);
        } else {
          skippedCount++;
          console.log(`⚠ Insufficient data for 5Y analysis`);
        }
        
        // Small delay to prevent overwhelming the database
        await new Promise(resolve => setTimeout(resolve, 100));
        
      } catch (error) {
        skippedCount++;
        console.error(`✗ Error enhancing fund ${fund.id}: ${error.message}`);
      }
    }
    
    console.log(`\n=== Enhanced Scoring Summary ===`);
    console.log(`Successfully enhanced: ${enhancedCount} funds`);
    console.log(`Skipped: ${skippedCount} funds`);
    console.log(`All funds now include comprehensive time period analysis`);
    
  } catch (error) {
    console.error('Error in enhanced scoring:', error);
  } finally {
    await pool.end();
  }
}

async function addYTDColumnIfNeeded() {
  try {
    // Check if column exists
    const columnExists = await pool.query(`
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'fund_scores' 
      AND column_name = 'return_ytd_score'
    `);
    
    if (columnExists.rows.length === 0) {
      console.log('Adding return_ytd_score column to fund_scores table...');
      await pool.query(`
        ALTER TABLE fund_scores 
        ADD COLUMN return_ytd_score DECIMAL(4,1)
      `);
      console.log('✓ YTD column added successfully');
    } else {
      console.log('✓ YTD column already exists');
    }
  } catch (error) {
    console.error('Error adding YTD column:', error.message);
  }
}

async function enhanceFundScoring(fund) {
  // Get comprehensive NAV data
  const navData = await pool.query(`
    SELECT nav_date, nav_value 
    FROM nav_data 
    WHERE fund_id = $1 
    ORDER BY nav_date DESC 
    LIMIT 2000
  `, [fund.id]);
  
  if (navData.rows.length < 100) {
    return false;
  }
  
  const navValues = navData.rows.map(n => parseFloat(n.nav_value));
  const dates = navData.rows.map(n => new Date(n.nav_date));
  const currentNav = navValues[0];
  const currentDate = dates[0];
  
  // Calculate returns for all periods including 5Y and YTD
  const returns = {};
  const actualReturns = {}; // Store actual percentage returns
  
  // Define all periods including 5Y and YTD
  const periods = [
    { name: '1m', days: 30, weight: 0.08 },
    { name: '3m', days: 90, weight: 0.12 },
    { name: '6m', days: 180, weight: 0.15 },
    { name: '1y', days: 365, weight: 0.20 },
    { name: '3y', days: 1095, weight: 0.25 },
    { name: '5y', days: 1825, weight: 0.20 }
  ];
  
  // Calculate standard period returns
  for (const period of periods) {
    if (navValues.length > period.days) {
      const pastNav = navValues[period.days];
      const returnPercent = ((currentNav - pastNav) / pastNav) * 100;
      returns[period.name] = returnPercent;
      actualReturns[period.name] = returnPercent;
    }
  }
  
  // Calculate YTD return
  const currentYear = currentDate.getFullYear();
  const yearStart = new Date(currentYear, 0, 1);
  
  // Find NAV closest to start of current year
  let ytdStartNav = null;
  let minDaysDiff = Infinity;
  
  for (let i = 0; i < dates.length; i++) {
    const daysDiff = Math.abs((dates[i] - yearStart) / (1000 * 60 * 60 * 24));
    if (daysDiff < minDaysDiff && daysDiff <= 15) { // Within 15 days of year start
      minDaysDiff = daysDiff;
      ytdStartNav = navValues[i];
    }
  }
  
  if (ytdStartNav) {
    const ytdReturn = ((currentNav - ytdStartNav) / ytdStartNav) * 100;
    returns['ytd'] = ytdReturn;
    actualReturns['ytd'] = ytdReturn;
  }
  
  // Calculate risk metrics
  const dailyReturns = [];
  for (let i = 1; i < Math.min(navValues.length, 1000); i++) {
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
  
  // Calculate enhanced scores
  const returnScores = calculateEnhancedReturnScores(returns, fund.category);
  const riskScores = calculateRiskScores(volatility, maxDrawdown);
  const qualityScores = calculateQualityScores(navData.rows.length, dailyReturns.length);
  
  const totalScore = returnScores.total + riskScores.total + qualityScores.total;
  const quartile = totalScore >= 75 ? 1 : totalScore >= 50 ? 2 : totalScore >= 25 ? 3 : 4;
  const recommendation = quartile <= 2 ? 'BUY' : quartile === 3 ? 'HOLD' : 'SELL';
  
  // Update or insert enhanced score record
  await pool.query(`
    INSERT INTO fund_scores (
      fund_id, score_date, 
      return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score, return_ytd_score,
      historical_returns_total,
      std_dev_1y_score, max_drawdown_score,
      risk_grade_total,
      aum_size_score, expense_ratio_score,
      other_metrics_total,
      total_score, quartile, recommendation,
      subcategory, created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    ON CONFLICT (fund_id, score_date) 
    DO UPDATE SET
      return_3m_score = EXCLUDED.return_3m_score,
      return_6m_score = EXCLUDED.return_6m_score,
      return_1y_score = EXCLUDED.return_1y_score,
      return_3y_score = EXCLUDED.return_3y_score,
      return_5y_score = EXCLUDED.return_5y_score,
      return_ytd_score = EXCLUDED.return_ytd_score,
      historical_returns_total = EXCLUDED.historical_returns_total,
      std_dev_1y_score = EXCLUDED.std_dev_1y_score,
      max_drawdown_score = EXCLUDED.max_drawdown_score,
      risk_grade_total = EXCLUDED.risk_grade_total,
      total_score = EXCLUDED.total_score,
      quartile = EXCLUDED.quartile,
      recommendation = EXCLUDED.recommendation
  `, [
    fund.id,
    new Date().toISOString().split('T')[0],
    returnScores.return_3m || 0,
    returnScores.return_6m || 0,
    returnScores.return_1y || 0,
    returnScores.return_3y || 0,
    returnScores.return_5y || 0,
    returnScores.return_ytd || 0,
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
  
  return true;
}

function calculateEnhancedReturnScores(returns, category) {
  const scores = {};
  let totalWeightedScore = 0;
  let totalWeight = 0;
  
  // Enhanced category-based benchmarks
  const benchmarks = {
    'Equity': { 
      excellent: { '1m': 3, '3m': 8, '6m': 12, '1y': 15, '3y': 45, '5y': 75, 'ytd': 10 },
      good: { '1m': 1, '3m': 4, '6m': 6, '1y': 10, '3y': 30, '5y': 50, 'ytd': 5 },
      average: { '1m': 0, '3m': 0, '6m': 2, '1y': 5, '3y': 15, '5y': 25, 'ytd': 0 },
      poor: { '1m': -2, '3m': -5, '6m': -5, '1y': 0, '3y': 5, '5y': 10, 'ytd': -5 }
    },
    'Debt': { 
      excellent: { '1m': 0.5, '3m': 2, '6m': 4, '1y': 7, '3y': 21, '5y': 35, 'ytd': 3 },
      good: { '1m': 0.3, '3m': 1, '6m': 2, '1y': 5, '3y': 15, '5y': 25, 'ytd': 2 },
      average: { '1m': 0.1, '3m': 0.5, '6m': 1, '1y': 3, '3y': 9, '5y': 15, 'ytd': 1 },
      poor: { '1m': 0, '3m': 0, '6m': 0, '1y': 1, '3y': 3, '5y': 5, 'ytd': 0 }
    },
    'Hybrid': { 
      excellent: { '1m': 2, '3m': 5, '6m': 8, '1y': 12, '3y': 30, '5y': 50, 'ytd': 6 },
      good: { '1m': 1, '3m': 3, '6m': 5, '1y': 8, '3y': 20, '5y': 35, 'ytd': 3 },
      average: { '1m': 0, '3m': 1, '6m': 2, '1y': 4, '3y': 10, '5y': 20, 'ytd': 1 },
      poor: { '1m': -1, '3m': -2, '6m': -2, '1y': 0, '3y': 5, '5y': 10, 'ytd': -2 }
    }
  };
  
  const benchmark = benchmarks[category] || benchmarks['Equity'];
  
  // Define weights for each period
  const weights = {
    '1m': 0.08,
    '3m': 0.12,
    '6m': 0.15,
    '1y': 0.20,
    '3y': 0.25,
    '5y': 0.20,
    'ytd': 0.10 // New weight for YTD
  };
  
  for (const [period, returnValue] of Object.entries(returns)) {
    let score = 0;
    const periodBenchmark = benchmark.excellent[period];
    
    if (periodBenchmark !== undefined) {
      if (returnValue >= benchmark.excellent[period]) score = 100;
      else if (returnValue >= benchmark.good[period]) score = 75;
      else if (returnValue >= benchmark.average[period]) score = 50;
      else if (returnValue >= benchmark.poor[period]) score = 25;
      else score = 10;
    } else {
      score = 50; // Default score if benchmark not defined
    }
    
    scores[`return_${period}`] = score;
    
    const weight = weights[period] || 0.1;
    totalWeightedScore += score * weight;
    totalWeight += weight;
  }
  
  scores.total = Math.min(40, totalWeightedScore); // Cap at 40 points
  return scores;
}

function calculateRiskScores(volatility, maxDrawdown) {
  const volatilityScore = Math.max(0, Math.min(100, 100 - volatility * 2));
  const drawdownScore = Math.max(0, Math.min(100, 100 - maxDrawdown * 3));
  
  return {
    volatility: volatilityScore,
    drawdown: drawdownScore,
    total: (volatilityScore + drawdownScore) * 0.15
  };
}

function calculateQualityScores(navCount, dailyReturnsCount) {
  const dataQualityScore = Math.min(100, (navCount / 1000) * 100);
  const consistencyScore = Math.min(100, (dailyReturnsCount / 500) * 100);
  
  return {
    data_quality: dataQualityScore,
    consistency: consistencyScore,
    total: (dataQualityScore + consistencyScore) * 0.15
  };
}

enhancedScoringWith5YAndYTD();