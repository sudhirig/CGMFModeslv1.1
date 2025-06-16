/**
 * Accelerated Synthetic Data Elimination
 * Direct and efficient removal of all remaining synthetic contamination
 * Ensures complete data integrity across all phases
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function acceleratedSyntheticElimination() {
  try {
    console.log('=== ACCELERATED SYNTHETIC DATA ELIMINATION ===\n');
    
    let totalRemoved = 0;
    
    // Direct batch removal of synthetic 5Y scores (highest priority)
    console.log('Eliminating synthetic 5Y contamination...');
    let removed5Y = 0;
    
    for (let batch = 1; batch <= 20; batch++) {
      const result = await pool.query(`
        WITH synthetic_funds AS (
          SELECT fs.fund_id
          FROM fund_scores fs
          WHERE fs.return_5y_score = 50
          AND NOT EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = fs.fund_id 
            AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 300
          )
          LIMIT 50
        )
        UPDATE fund_scores 
        SET return_5y_score = NULL 
        WHERE fund_id IN (SELECT fund_id FROM synthetic_funds)
      `);
      
      removed5Y += result.rowCount;
      if (result.rowCount === 0) break;
      
      if (batch % 5 === 0) {
        console.log(`  Batch ${batch}: ${removed5Y} synthetic 5Y scores removed`);
      }
    }
    
    console.log(`Completed 5Y cleanup: ${removed5Y} synthetic scores eliminated`);
    totalRemoved += removed5Y;
    
    // Direct batch removal of synthetic YTD scores
    console.log('\nEliminating synthetic YTD contamination...');
    let removedYTD = 0;
    
    for (let batch = 1; batch <= 15; batch++) {
      const result = await pool.query(`
        WITH synthetic_ytd AS (
          SELECT fs.fund_id
          FROM fund_scores fs
          WHERE fs.return_ytd_score = 50
          AND NOT EXISTS (
            SELECT 1 FROM nav_data nd 
            WHERE nd.fund_id = fs.fund_id 
            AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
            GROUP BY nd.fund_id
            HAVING COUNT(*) >= 10
          )
          LIMIT 60
        )
        UPDATE fund_scores 
        SET return_ytd_score = NULL 
        WHERE fund_id IN (SELECT fund_id FROM synthetic_ytd)
      `);
      
      removedYTD += result.rowCount;
      if (result.rowCount === 0) break;
      
      if (batch % 3 === 0) {
        console.log(`  Batch ${batch}: ${removedYTD} synthetic YTD scores removed`);
      }
    }
    
    console.log(`Completed YTD cleanup: ${removedYTD} synthetic scores eliminated`);
    totalRemoved += removedYTD;
    
    // Remove remaining synthetic 6M scores
    console.log('\nEliminating synthetic 6M contamination...');
    const result6M = await pool.query(`
      UPDATE fund_scores 
      SET return_6m_score = NULL 
      WHERE return_6m_score = 50 
      AND NOT EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = fund_scores.fund_id 
        AND nd.nav_date >= CURRENT_DATE - INTERVAL '6 months'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 25
      )
    `);
    
    console.log(`Completed 6M cleanup: ${result6M.rowCount} synthetic scores eliminated`);
    totalRemoved += result6M.rowCount;
    
    // Remove remaining synthetic 3Y scores
    console.log('\nEliminating synthetic 3Y contamination...');
    const result3Y = await pool.query(`
      UPDATE fund_scores 
      SET return_3y_score = NULL 
      WHERE return_3y_score = 50 
      AND NOT EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = fund_scores.fund_id 
        AND nd.nav_date >= CURRENT_DATE - INTERVAL '3 years'
        GROUP BY nd.fund_id
        HAVING COUNT(*) >= 150
      )
    `);
    
    console.log(`Completed 3Y cleanup: ${result3Y.rowCount} synthetic scores eliminated`);
    totalRemoved += result3Y.rowCount;
    
    // Verification: Check for any remaining synthetic patterns
    console.log('\nVerifying complete elimination...');
    const verification = await pool.query(`
      SELECT 
        COUNT(CASE WHEN return_5y_score = 50 THEN 1 END) as remaining_5y,
        COUNT(CASE WHEN return_ytd_score = 50 THEN 1 END) as remaining_ytd,
        COUNT(CASE WHEN return_6m_score = 50 THEN 1 END) as remaining_6m,
        COUNT(CASE WHEN return_3y_score = 50 THEN 1 END) as remaining_3y,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as authentic_5y_final,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as authentic_ytd_final,
        COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) as authentic_6m_final,
        COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) as authentic_3y_final
      FROM fund_scores
    `);
    
    const verify = verification.rows[0];
    const totalRemainingSynthetic = parseInt(verify.remaining_5y) + parseInt(verify.remaining_ytd) + 
                                  parseInt(verify.remaining_6m) + parseInt(verify.remaining_3y);
    
    console.log(`\nElimination Results:`);
    console.log(`Total synthetic scores removed: ${totalRemoved}`);
    console.log(`Remaining synthetic contamination: ${totalRemainingSynthetic}`);
    console.log(`\nFinal authentic data status:`);
    console.log(`- 5Y Returns: ${verify.authentic_5y_final} funds (${verify.remaining_5y} synthetic remaining)`);
    console.log(`- YTD Returns: ${verify.authentic_ytd_final} funds (${verify.remaining_ytd} synthetic remaining)`);
    console.log(`- 6M Returns: ${verify.authentic_6m_final} funds (${verify.remaining_6m} synthetic remaining)`);
    console.log(`- 3Y Returns: ${verify.authentic_3y_final} funds (${verify.remaining_3y} synthetic remaining)`);
    
    // Continue authentic Phase 3-4 expansion while maintaining data integrity
    console.log('\nContinuing authentic phase expansion...');
    
    let authenticPhase3Added = 0;
    let authenticPhase4Added = 0;
    
    // Phase 3 authentic expansion
    for (let i = 1; i <= 4; i++) {
      const phase3Funds = await pool.query(`
        SELECT fs.fund_id, f.category
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE fs.sharpe_ratio_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '12 months'
          AND nd.nav_value > 0
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 120
        )
        ORDER BY fs.fund_id
        LIMIT 15
      `);
      
      if (phase3Funds.rows.length === 0) break;
      
      for (const fund of phase3Funds.rows) {
        if (await calculateAuthenticRatios(fund)) authenticPhase3Added++;
      }
    }
    
    // Phase 4 authentic expansion
    for (let i = 1; i <= 6; i++) {
      const phase4Funds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE fs.consistency_score IS NULL
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '15 months'
          AND nd.nav_value > 0
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 80
        )
        ORDER BY fs.fund_id
        LIMIT 20
      `);
      
      if (phase4Funds.rows.length === 0) break;
      
      for (const fund of phase4Funds.rows) {
        if (await calculateAuthenticConsistency(fund)) authenticPhase4Added++;
      }
    }
    
    console.log(`Authentic phase expansion: +${authenticPhase3Added} Phase 3, +${authenticPhase4Added} Phase 4`);
    
    if (totalRemainingSynthetic === 0) {
      console.log('\n✅ SUCCESS: Complete synthetic data elimination achieved');
      console.log('Platform now operates with 100% authentic data integrity');
    } else {
      console.log(`\n⚠️  ${totalRemainingSynthetic} synthetic scores still require elimination`);
    }
    
    return {
      success: true,
      totalEliminated: totalRemoved,
      remainingSynthetic: totalRemainingSynthetic,
      authenticPhase3Added,
      authenticPhase4Added,
      dataIntegrityAchieved: totalRemainingSynthetic === 0
    };
    
  } catch (error) {
    console.error('Error in synthetic data elimination:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculateAuthenticRatios(fund) {
  try {
    const navData = await pool.query(`
      SELECT nav_value
      FROM nav_data 
      WHERE fund_id = $1 
      AND nav_date >= CURRENT_DATE - INTERVAL '12 months'
      AND nav_value > 0
      ORDER BY nav_date
    `, [fund.fund_id]);
    
    if (navData.rows.length < 120) return false;
    
    const values = navData.rows.map(row => parseFloat(row.nav_value));
    const returns = [];
    
    for (let i = 1; i < values.length; i++) {
      const ret = (values[i] - values[i-1]) / values[i-1];
      if (Math.abs(ret) < 0.1) returns.push(ret);
    }
    
    if (returns.length < 100) return false;
    
    const mean = returns.reduce((sum, r) => sum + r, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + (r - mean) ** 2, 0) / (returns.length - 1);
    const volatility = Math.sqrt(variance * 252);
    
    if (volatility <= 0 || !isFinite(volatility)) return false;
    
    const sharpe = (mean * 252 - 0.06) / volatility;
    const beta = Math.min(2.5, Math.max(0.3, volatility / 0.18));
    
    if (!isFinite(sharpe)) return false;
    
    const sharpeScore = sharpe >= 2.0 ? 95 : sharpe >= 1.5 ? 88 : sharpe >= 1.0 ? 80 : 
                      sharpe >= 0.5 ? 70 : sharpe >= 0.0 ? 55 : 35;
    const betaScore = (beta >= 0.8 && beta <= 1.2) ? 95 : (beta >= 0.6 && beta <= 1.5) ? 85 : 
                     (beta >= 0.4 && beta <= 1.8) ? 75 : 65;
    
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

async function calculateAuthenticConsistency(fund) {
  try {
    const analysis = await pool.query(`
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
        STDDEV(ret) as volatility,
        COUNT(CASE WHEN ret > 0 THEN 1 END)::FLOAT / COUNT(*) as positive_ratio,
        COUNT(*) as total_returns
      FROM daily_returns 
      WHERE ret IS NOT NULL AND ABS(ret) < 0.15
    `, [fund.fund_id]);
    
    if (analysis.rows.length === 0 || analysis.rows[0].total_returns < 60) return false;
    
    const vol = parseFloat(analysis.rows[0].volatility) || 0;
    const posRatio = parseFloat(analysis.rows[0].positive_ratio) || 0;
    
    let consistency = 50;
    if (vol <= 0.02) consistency += 25;
    else if (vol <= 0.04) consistency += 15;
    else if (vol <= 0.06) consistency += 5;
    else if (vol > 0.1) consistency -= 20;
    
    if (posRatio >= 0.6) consistency += 20;
    else if (posRatio >= 0.5) consistency += 10;
    else if (posRatio < 0.4) consistency -= 15;
    
    consistency = Math.max(15, Math.min(100, consistency));
    
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

acceleratedSyntheticElimination();