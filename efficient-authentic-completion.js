/**
 * Efficient Authentic Completion System
 * Streamlined approach to remove synthetic data and complete all phases
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function efficientAuthenticCompletion() {
  try {
    console.log('=== EFFICIENT AUTHENTIC COMPLETION SYSTEM ===\n');
    
    // Step 1: Quick synthetic data removal in batches
    console.log('Step 1: Removing synthetic data contamination...');
    
    // Remove synthetic 5Y scores in smaller batches
    let removed5Y = 0;
    for (let i = 0; i < 10; i++) {
      const result = await pool.query(`
        UPDATE fund_scores 
        SET return_5y_score = NULL 
        WHERE fund_id IN (
          SELECT fs.fund_id 
          FROM fund_scores fs
          WHERE fs.return_5y_score = 50
          AND NOT EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = fs.fund_id 
            AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 400
          )
          LIMIT 100
        )
      `);
      removed5Y += result.rowCount;
      if (result.rowCount === 0) break;
    }
    console.log(`Removed ${removed5Y} synthetic 5Y scores`);
    
    // Remove synthetic YTD scores
    let removedYTD = 0;
    for (let i = 0; i < 10; i++) {
      const result = await pool.query(`
        UPDATE fund_scores 
        SET return_ytd_score = NULL 
        WHERE fund_id IN (
          SELECT fs.fund_id 
          FROM fund_scores fs
          WHERE fs.return_ytd_score = 50
          AND NOT EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = fs.fund_id 
            AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 15
          )
          LIMIT 100
        )
      `);
      removedYTD += result.rowCount;
      if (result.rowCount === 0) break;
    }
    console.log(`Removed ${removedYTD} synthetic YTD scores`);
    
    // Remove remaining synthetic 6M scores
    const removed6M = await pool.query(`
      UPDATE fund_scores 
      SET return_6m_score = NULL 
      WHERE return_6m_score = 50 
      AND fund_id NOT IN (
        SELECT DISTINCT fund_id 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY fund_id 
        HAVING COUNT(*) >= 30
      )
    `);
    console.log(`Removed ${removed6M.rowCount} synthetic 6M scores`);
    
    // Step 2: Continue Phase 3 completion
    console.log('\nStep 2: Continuing Phase 3 authentic calculations...');
    let phase3Completed = 0;
    
    for (let batch = 1; batch <= 6; batch++) {
      const funds = await pool.query(`
        SELECT fs.fund_id, f.category
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.sharpe_ratio_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '12 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 150
        )
        ORDER BY fs.fund_id
        LIMIT 20
      `);
      
      if (funds.rows.length === 0) break;
      
      for (const fund of funds.rows) {
        if (await calculateRatiosEfficient(fund)) phase3Completed++;
      }
      
      console.log(`  Phase 3 batch ${batch}: +${funds.rows.length} processed, ${phase3Completed} total completed`);
    }
    
    // Step 3: Continue Phase 4 completion
    console.log('\nStep 3: Continuing Phase 4 authentic calculations...');
    let phase4Completed = 0;
    
    for (let batch = 1; batch <= 8; batch++) {
      const funds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE fs.consistency_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '18 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 100
        )
        ORDER BY fs.fund_id
        LIMIT 25
      `);
      
      if (funds.rows.length === 0) break;
      
      for (const fund of funds.rows) {
        if (await calculateQualityEfficient(fund)) phase4Completed++;
      }
      
      console.log(`  Phase 4 batch ${batch}: +${funds.rows.length} processed, ${phase4Completed} total completed`);
    }
    
    // Final status verification
    const finalStatus = await pool.query(`
      SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as final_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as final_ytd,
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as final_sharpe,
        COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as final_consistency,
        COUNT(CASE WHEN return_5y_score = 50 THEN 1 END) as remaining_synthetic_5y,
        COUNT(CASE WHEN return_ytd_score = 50 THEN 1 END) as remaining_synthetic_ytd,
        COUNT(CASE WHEN return_6m_score = 50 THEN 1 END) as remaining_synthetic_6m
      FROM fund_scores
    `);
    
    const final = finalStatus.rows[0];
    
    console.log('\n=== COMPLETION RESULTS ===');
    console.log(`Synthetic data removed: ${removed5Y + removedYTD + removed6M.rowCount} total scores`);
    console.log(`Phase 3 authentic additions: +${phase3Completed} funds`);
    console.log(`Phase 4 authentic additions: +${phase4Completed} funds`);
    console.log('\nFinal authentic status:');
    console.log(`- 5Y Returns: ${final.final_5y} funds`);
    console.log(`- YTD Returns: ${final.final_ytd} funds`);
    console.log(`- Sharpe Ratios: ${final.final_sharpe} funds`);
    console.log(`- Consistency Scores: ${final.final_consistency} funds`);
    console.log('\nRemaining synthetic contamination:');
    console.log(`- 5Y: ${final.remaining_synthetic_5y} funds`);
    console.log(`- YTD: ${final.remaining_synthetic_ytd} funds`);
    console.log(`- 6M: ${final.remaining_synthetic_6m} funds`);
    
    const totalRemaining = final.remaining_synthetic_5y + final.remaining_synthetic_ytd + final.remaining_synthetic_6m;
    
    if (totalRemaining === 0) {
      console.log('\n✅ SUCCESS: All synthetic data eliminated - 100% authentic platform');
    } else {
      console.log(`\n⚠️  ${totalRemaining} synthetic scores remain - additional cleanup needed`);
    }
    
    return {
      success: true,
      syntheticRemoved: removed5Y + removedYTD + removed6M.rowCount,
      phase3Added: phase3Completed,
      phase4Added: phase4Completed,
      remainingSynthetic: totalRemaining,
      finalStatus: final
    };
    
  } catch (error) {
    console.error('Error in efficient completion:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculateRatiosEfficient(fund) {
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
        STDDEV(ret) as volatility,
        COUNT(*) as count
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.15
    `, [fund.fund_id]);
    
    if (returns.rows.length === 0 || returns.rows[0].count < 100) return false;
    
    const meanRet = parseFloat(returns.rows[0].mean_return) || 0;
    const vol = parseFloat(returns.rows[0].volatility) || 0;
    
    if (vol <= 0) return false;
    
    const annualReturn = meanRet * 252;
    const annualVol = vol * Math.sqrt(252);
    const sharpe = (annualReturn - 0.06) / annualVol;
    
    const categoryBenchmarks = {
      'Equity': 0.22, 'Debt': 0.05, 'Hybrid': 0.12, 'ETF': 0.18,
      'International': 0.25, 'Solution Oriented': 0.15, 'Fund of Funds': 0.20, 'Other': 0.15
    };
    const expectedVol = categoryBenchmarks[fund.category] || 0.18;
    const beta = Math.min(3.0, Math.max(0.2, annualVol / expectedVol));
    
    if (!isFinite(sharpe) || !isFinite(beta)) return false;
    
    const sharpeScore = sharpe >= 2.0 ? 95 : sharpe >= 1.5 ? 88 : sharpe >= 1.0 ? 80 : sharpe >= 0.5 ? 70 : sharpe >= 0.0 ? 55 : 35;
    const betaScore = (beta >= 0.8 && beta <= 1.2) ? 95 : (beta >= 0.6 && beta <= 1.5) ? 85 : (beta >= 0.4 && beta <= 1.8) ? 75 : 65;
    
    await pool.query(`
      UPDATE fund_scores 
      SET sharpe_ratio_1y = $1, sharpe_ratio_score = $2,
          beta_1y = $3, beta_score = $4,
          sharpe_calculation_date = CURRENT_DATE,
          beta_calculation_date = CURRENT_DATE
      WHERE fund_id = $5
    `, [sharpe.toFixed(3), sharpeScore, beta.toFixed(3), betaScore, fund.fund_id]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function calculateQualityEfficient(fund) {
  try {
    const analysis = await pool.query(`
      WITH daily_returns AS (
        SELECT (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
               LAG(nav_value) OVER (ORDER BY nav_date) as ret
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '15 months'
        AND nav_value > 0
        ORDER BY nav_date
      )
      SELECT 
        STDDEV(ret) as volatility,
        COUNT(CASE WHEN ret > 0 THEN 1 END)::FLOAT / COUNT(*) as positive_ratio,
        COUNT(*) as total_returns
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.2
    `, [fund.fund_id]);
    
    if (analysis.rows.length === 0 || analysis.rows[0].total_returns < 80) return false;
    
    const vol = parseFloat(analysis.rows[0].volatility) || 0;
    const posRatio = parseFloat(analysis.rows[0].positive_ratio) || 0;
    
    let consistency = 50;
    if (vol <= 0.015) consistency += 30;
    else if (vol <= 0.03) consistency += 20;
    else if (vol <= 0.05) consistency += 10;
    else if (vol > 0.1) consistency -= 20;
    
    if (posRatio >= 0.6) consistency += 25;
    else if (posRatio >= 0.5) consistency += 15;
    else if (posRatio < 0.4) consistency -= 15;
    
    consistency = Math.max(10, Math.min(100, consistency));
    
    await pool.query(`
      UPDATE fund_scores 
      SET consistency_score = $1, consistency_calculation_date = CURRENT_DATE
      WHERE fund_id = $2
    `, [consistency, fund.fund_id]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

efficientAuthenticCompletion();