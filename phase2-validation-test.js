/**
 * Phase 2 Validation Test
 * Quick validation of advanced risk analytics implementation
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function validatePhase2Results() {
  console.log('Phase 2 Validation: Advanced Risk Analytics');
  console.log('=========================================\n');

  try {
    // Check risk metrics coverage
    const coverageCheck = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN sharpe_ratio IS NOT NULL THEN 1 END) as funds_with_sharpe,
        COUNT(CASE WHEN beta IS NOT NULL THEN 1 END) as funds_with_beta,
        COUNT(CASE WHEN alpha IS NOT NULL THEN 1 END) as funds_with_alpha,
        ROUND(AVG(CASE WHEN sharpe_ratio IS NOT NULL THEN sharpe_ratio END), 4) as avg_sharpe,
        ROUND(AVG(CASE WHEN beta IS NOT NULL THEN beta END), 4) as avg_beta,
        ROUND(AVG(CASE WHEN alpha IS NOT NULL THEN alpha END), 4) as avg_alpha
      FROM fund_scores_corrected
      WHERE fund_id IN (
        SELECT DISTINCT fund_id 
        FROM nav_data 
        WHERE nav_date >= '2023-01-01'
        GROUP BY fund_id
        HAVING COUNT(*) >= 100
      )
    `);

    const coverage = coverageCheck.rows[0];
    console.log('Risk Analytics Coverage:');
    console.log(`  Total eligible funds: ${coverage.total_funds}`);
    console.log(`  Funds with Sharpe ratios: ${coverage.funds_with_sharpe}`);
    console.log(`  Funds with Beta: ${coverage.funds_with_beta}`);
    console.log(`  Funds with Alpha: ${coverage.funds_with_alpha}`);
    console.log(`  Average Sharpe ratio: ${coverage.avg_sharpe}`);
    console.log(`  Average Beta: ${coverage.avg_beta}`);
    console.log(`  Average Alpha: ${coverage.avg_alpha}`);

    // Validate data quality
    const qualityCheck = await pool.query(`
      SELECT 
        COUNT(CASE WHEN sharpe_ratio < -5 OR sharpe_ratio > 5 THEN 1 END) as extreme_sharpe,
        COUNT(CASE WHEN beta < 0.1 OR beta > 3.0 THEN 1 END) as extreme_beta,
        COUNT(CASE WHEN alpha < -0.5 OR alpha > 0.5 THEN 1 END) as extreme_alpha,
        MIN(sharpe_ratio) as min_sharpe,
        MAX(sharpe_ratio) as max_sharpe,
        MIN(beta) as min_beta,
        MAX(beta) as max_beta,
        MIN(alpha) as min_alpha,
        MAX(alpha) as max_alpha
      FROM fund_scores_corrected
      WHERE sharpe_ratio IS NOT NULL OR beta IS NOT NULL OR alpha IS NOT NULL
    `);

    const quality = qualityCheck.rows[0];
    console.log('\nData Quality Assessment:');
    console.log(`  Extreme Sharpe ratios: ${quality.extreme_sharpe}`);
    console.log(`  Extreme Beta values: ${quality.extreme_beta}`);
    console.log(`  Extreme Alpha values: ${quality.extreme_alpha}`);
    console.log(`  Sharpe range: ${quality.min_sharpe} to ${quality.max_sharpe}`);
    console.log(`  Beta range: ${quality.min_beta} to ${quality.max_beta}`);
    console.log(`  Alpha range: ${quality.min_alpha} to ${quality.max_alpha}`);

    // Sample fund analysis
    const sampleFunds = await pool.query(`
      SELECT f.fund_name, fsc.total_score, fsc.sharpe_ratio, fsc.beta, fsc.alpha
      FROM funds f
      JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id
      WHERE fsc.sharpe_ratio IS NOT NULL 
      AND fsc.beta IS NOT NULL 
      AND fsc.alpha IS NOT NULL
      ORDER BY fsc.total_score DESC
      LIMIT 5
    `);

    console.log('\nTop Funds with Complete Risk Metrics:');
    sampleFunds.rows.forEach((fund, idx) => {
      console.log(`  ${idx + 1}. ${fund.fund_name.substring(0, 40)}...`);
      console.log(`     Score: ${fund.total_score}, Sharpe: ${fund.sharpe_ratio}, Beta: ${fund.beta}, Alpha: ${fund.alpha}`);
    });

    const validationPassed = 
      parseInt(coverage.funds_with_sharpe) >= 50 &&
      parseInt(quality.extreme_sharpe) === 0 &&
      parseInt(quality.extreme_beta) === 0 &&
      parseInt(quality.extreme_alpha) === 0;

    console.log(`\nPhase 2 Status: ${validationPassed ? 'PASSED ✅' : 'NEEDS ATTENTION ⚠️'}`);
    
    if (validationPassed) {
      console.log('Advanced Risk Analytics successfully implemented with authentic data');
    } else {
      console.log('Issues detected in risk analytics implementation');
    }

    return validationPassed;

  } catch (error) {
    console.error('Validation Error:', error);
    return false;
  } finally {
    await pool.end();
  }
}

validatePhase2Results().catch(console.error);