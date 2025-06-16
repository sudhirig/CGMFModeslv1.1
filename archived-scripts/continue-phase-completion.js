/**
 * Continue Phase Completion
 * Resumes processing to complete remaining Phases 3-4 systematically
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function continuePhaseCompletion() {
  try {
    console.log('=== CONTINUING PHASE COMPLETION ===\n');
    
    // Check current status
    const currentStatus = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as sharpe_done,
        COUNT(CASE WHEN beta_score IS NOT NULL THEN 1 END) as beta_done,
        COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as consistency_done,
        COUNT(CASE WHEN overall_rating IS NOT NULL THEN 1 END) as rating_done
      FROM fund_scores
    `);
    
    const status = currentStatus.rows[0];
    console.log(`Current Progress:`);
    console.log(`- Phase 3 Sharpe: ${status.sharpe_done} funds`);
    console.log(`- Phase 3 Beta: ${status.beta_done} funds`);
    console.log(`- Phase 4 Consistency: ${status.consistency_done} funds`);
    console.log(`- Phase 4 Rating: ${status.rating_done} funds\n`);
    
    // Continue Phase 3 if needed
    if (status.sharpe_done < 100) {
      console.log('Continuing Phase 3: Advanced Ratios...');
      await continuePhase3();
    }
    
    // Continue Phase 4 if needed
    if (status.consistency_done < 500) {
      console.log('Continuing Phase 4: Quality Metrics...');
      await continuePhase4();
    }
    
    // Final status
    const finalStatus = await pool.query(`
      SELECT 
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as final_sharpe,
        COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as final_consistency
      FROM fund_scores
    `);
    
    const final = finalStatus.rows[0];
    console.log(`\n=== COMPLETION STATUS ===`);
    console.log(`Phase 3 Advanced Ratios: ${final.final_sharpe} funds`);
    console.log(`Phase 4 Quality Metrics: ${final.final_consistency} funds`);
    
    return { success: true, finalSharpe: final.final_sharpe, finalConsistency: final.final_consistency };
    
  } catch (error) {
    console.error('Error continuing phase completion:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function continuePhase3() {
  try {
    let completed = 0;
    
    for (let batch = 1; batch <= 10; batch++) {
      const funds = await pool.query(`
        SELECT fs.fund_id, f.category
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.sharpe_ratio_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '15 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 150
        )
        ORDER BY fs.fund_id
        LIMIT 25
      `);
      
      if (funds.rows.length === 0) break;
      
      console.log(`  Phase 3 Batch ${batch}: Processing ${funds.rows.length} funds`);
      
      for (const fund of funds.rows) {
        const success = await calculateRatiosQuick(fund);
        if (success) completed++;
      }
      
      console.log(`    Completed: ${completed} total funds`);
    }
    
    return completed;
    
  } catch (error) {
    console.error('Error in Phase 3 continuation:', error);
    return 0;
  }
}

async function continuePhase4() {
  try {
    let completed = 0;
    
    for (let batch = 1; batch <= 15; batch++) {
      const funds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE fs.consistency_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '1 year'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 100
        )
        ORDER BY fs.fund_id
        LIMIT 50
      `);
      
      if (funds.rows.length === 0) break;
      
      console.log(`  Phase 4 Batch ${batch}: Processing ${funds.rows.length} funds`);
      
      for (const fund of funds.rows) {
        const success = await calculateQualityQuick(fund);
        if (success) completed++;
      }
      
      console.log(`    Completed: ${completed} total funds`);
    }
    
    return completed;
    
  } catch (error) {
    console.error('Error in Phase 4 continuation:', error);
    return 0;
  }
}

async function calculateRatiosQuick(fund) {
  try {
    const returns = await pool.query(`
      WITH daily_returns AS (
        SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
               LAG(nav_value) OVER (ORDER BY nav_date) as ret
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '12 months'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        AVG(ret) as mean_return,
        STDDEV(ret) as volatility
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.2
    `, [fund.fund_id]);
    
    if (returns.rows.length === 0) return false;
    
    const meanRet = parseFloat(returns.rows[0].mean_return) || 0;
    const vol = parseFloat(returns.rows[0].volatility) || 0;
    
    if (vol <= 0) return false;
    
    const annualReturn = meanRet * 252;
    const annualVol = vol * Math.sqrt(252);
    const sharpe = (annualReturn - 0.06) / annualVol;
    const beta = calculateBeta(annualVol, fund.category);
    
    if (!isFinite(sharpe) || !isFinite(beta)) return false;
    
    await pool.query(`
      UPDATE fund_scores 
      SET sharpe_ratio_1y = $1, sharpe_ratio_score = $2,
          beta_1y = $3, beta_score = $4,
          sharpe_calculation_date = CURRENT_DATE,
          beta_calculation_date = CURRENT_DATE
      WHERE fund_id = $5
    `, [sharpe.toFixed(3), getSharpeScore(sharpe), beta.toFixed(3), getBetaScore(beta), fund.fund_id]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function calculateQualityQuick(fund) {
  try {
    const analysis = await pool.query(`
      WITH daily_returns AS (
        SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
               LAG(nav_value) OVER (ORDER BY nav_date) as ret
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '1 year'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        STDDEV(ret) as volatility,
        COUNT(CASE WHEN ret > 0 THEN 1 END)::FLOAT / COUNT(*) as positive_ratio
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.3
    `, [fund.fund_id]);
    
    if (analysis.rows.length === 0) return false;
    
    const vol = parseFloat(analysis.rows[0].volatility) || 0;
    const posRatio = parseFloat(analysis.rows[0].positive_ratio) || 0;
    
    let consistency = 50;
    if (vol <= 0.02) consistency += 25;
    else if (vol <= 0.05) consistency += 15;
    else if (vol > 0.1) consistency -= 20;
    
    if (posRatio >= 0.6) consistency += 20;
    else if (posRatio >= 0.5) consistency += 10;
    else if (posRatio < 0.4) consistency -= 15;
    
    consistency = Math.max(10, Math.min(100, consistency));
    
    const rating = await calculateOverallRating(fund.fund_id);
    
    await pool.query(`
      UPDATE fund_scores 
      SET consistency_score = $1, overall_rating = $2,
          consistency_calculation_date = CURRENT_DATE,
          rating_calculation_date = CURRENT_DATE
      WHERE fund_id = $3
    `, [consistency, rating, fund.fund_id]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function calculateOverallRating(fundId) {
  try {
    const scores = await pool.query(`
      SELECT return_1y_score, return_3y_score, std_dev_1y_score, sharpe_ratio_score
      FROM fund_scores WHERE fund_id = $1
    `, [fundId]);
    
    if (scores.rows.length === 0) return null;
    
    const s = scores.rows[0];
    let total = 0, weight = 0;
    
    if (s.return_1y_score) { total += s.return_1y_score * 0.3; weight += 0.3; }
    if (s.return_3y_score) { total += s.return_3y_score * 0.25; weight += 0.25; }
    if (s.std_dev_1y_score) { total += s.std_dev_1y_score * 0.2; weight += 0.2; }
    if (s.sharpe_ratio_score) { total += s.sharpe_ratio_score * 0.15; weight += 0.15; }
    
    return weight >= 0.4 ? Math.round(total / weight) : null;
    
  } catch (error) {
    return null;
  }
}

function calculateBeta(volatility, category) {
  const benchmarks = {
    'Equity': 0.22, 'Debt': 0.05, 'Hybrid': 0.12, 'ETF': 0.18,
    'International': 0.25, 'Solution Oriented': 0.15, 'Fund of Funds': 0.20, 'Other': 0.15
  };
  const expected = benchmarks[category] || 0.18;
  return Math.min(3.0, Math.max(0.2, volatility / expected));
}

function getSharpeScore(ratio) {
  if (ratio >= 2.0) return 95;
  if (ratio >= 1.5) return 88;
  if (ratio >= 1.0) return 80;
  if (ratio >= 0.5) return 70;
  if (ratio >= 0.0) return 55;
  return 35;
}

function getBetaScore(beta) {
  if (beta >= 0.8 && beta <= 1.2) return 95;
  if (beta >= 0.6 && beta <= 1.5) return 85;
  if (beta >= 0.4 && beta <= 1.8) return 75;
  return 65;
}

continuePhaseCompletion();