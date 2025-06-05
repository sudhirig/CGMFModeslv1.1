/**
 * Fix Recommendation Logic Implementation
 * Populate null recommendation values using comprehensive scoring methodology
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function fixRecommendationLogic() {
  try {
    console.log('=== Fixing Recommendation Logic ===');
    console.log('Populating null recommendations using authentic scoring data');
    
    // Step 1: Analyze current recommendation gaps
    await analyzeRecommendationGaps();
    
    // Step 2: Implement comprehensive recommendation logic
    await implementRecommendationLogic();
    
    // Step 3: Validate recommendation distribution
    await validateRecommendationDistribution();
    
    console.log('\n✓ Recommendation logic fix completed');
    
  } catch (error) {
    console.error('Recommendation fix error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function analyzeRecommendationGaps() {
  console.log('\n1. Analyzing Recommendation Gaps...');
  
  // Identify funds without recommendations by score range
  const gapAnalysis = await pool.query(`
    SELECT 
      CASE 
        WHEN total_score >= 70 THEN 'Excellent (70+)'
        WHEN total_score >= 60 THEN 'Good (60-69)'
        WHEN total_score >= 50 THEN 'Average (50-59)'
        WHEN total_score >= 40 THEN 'Below Average (40-49)'
        ELSE 'Poor (<40)'
      END as score_range,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN recommendation IS NULL THEN 1 END) as missing_recommendations,
      COUNT(CASE WHEN recommendation IS NOT NULL THEN 1 END) as has_recommendations,
      ROUND(COUNT(CASE WHEN recommendation IS NULL THEN 1 END) * 100.0 / COUNT(*), 1) as missing_percentage
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
    GROUP BY 
      CASE 
        WHEN total_score >= 70 THEN 'Excellent (70+)'
        WHEN total_score >= 60 THEN 'Good (60-69)'
        WHEN total_score >= 50 THEN 'Average (50-59)'
        WHEN total_score >= 40 THEN 'Below Average (40-49)'
        ELSE 'Poor (<40)'
      END
    ORDER BY AVG(total_score) DESC
  `);
  
  console.log('  Recommendation Gaps by Score Range:');
  console.log('  Score Range'.padEnd(20) + 'Total'.padEnd(8) + 'Missing'.padEnd(10) + 'Has Rec'.padEnd(10) + 'Missing %');
  console.log('  ' + '-'.repeat(65));
  
  for (const gap of gapAnalysis.rows) {
    console.log(
      `  ${gap.score_range}`.padEnd(20) +
      gap.total_funds.toString().padEnd(8) +
      gap.missing_recommendations.toString().padEnd(10) +
      gap.has_recommendations.toString().padEnd(10) +
      gap.missing_percentage + '%'
    );
  }
  
  // Check quartile-based recommendation gaps
  const quartileGaps = await pool.query(`
    SELECT 
      quartile,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN recommendation IS NULL THEN 1 END) as missing_recommendations,
      ROUND(AVG(total_score), 2) as avg_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND quartile IS NOT NULL
    GROUP BY quartile
    ORDER BY quartile
  `);
  
  console.log('\n  Recommendation Gaps by Quartile:');
  console.log('  Quartile'.padEnd(12) + 'Total'.padEnd(8) + 'Missing'.padEnd(10) + 'Avg Score');
  console.log('  ' + '-'.repeat(45));
  
  for (const q of quartileGaps.rows) {
    const quartileName = ['', 'Q1 (Top)', 'Q2', 'Q3', 'Q4 (Bottom)'][q.quartile];
    console.log(
      `  ${quartileName}`.padEnd(12) +
      q.total_funds.toString().padEnd(8) +
      q.missing_recommendations.toString().padEnd(10) +
      q.avg_score.toString()
    );
  }
}

async function implementRecommendationLogic() {
  console.log('\n2. Implementing Comprehensive Recommendation Logic...');
  
  // Implement multi-factor recommendation logic based on:
  // - Total score (primary factor)
  // - Quartile ranking (peer comparison)
  // - Risk-adjusted performance
  // - Consistency metrics
  
  const recommendationUpdate = await pool.query(`
    UPDATE fund_scores 
    SET recommendation = CASE
      -- STRONG_BUY: Top performers with excellent fundamentals
      WHEN total_score >= 70 OR 
           (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) OR
           (total_score >= 62 AND historical_returns_total >= 25 AND volatility_1y_percent < 15)
      THEN 'STRONG_BUY'
      
      -- BUY: Good performers with solid metrics
      WHEN total_score >= 60 OR
           (total_score >= 55 AND quartile <= 2 AND fundamentals_total >= 20) OR
           (total_score >= 52 AND sharpe_ratio_1y > 1.2 AND max_drawdown_percent < 20)
      THEN 'BUY'
      
      -- HOLD: Average performers, existing holdings worth keeping
      WHEN total_score >= 50 OR
           (total_score >= 45 AND quartile <= 3 AND risk_grade_total >= 20) OR
           (total_score >= 42 AND volatility_1y_percent < 25 AND fundamentals_total >= 18)
      THEN 'HOLD'
      
      -- SELL: Below average with concerning metrics
      WHEN total_score >= 35 OR
           (total_score >= 30 AND risk_grade_total >= 15)
      THEN 'SELL'
      
      -- STRONG_SELL: Poor performers with high risk
      ELSE 'STRONG_SELL'
    END
    WHERE score_date = CURRENT_DATE
      AND recommendation IS NULL
    RETURNING fund_id, total_score, recommendation
  `);
  
  console.log(`  ✓ Updated recommendations for ${recommendationUpdate.rowCount} funds`);
  
  // Show sample of updated recommendations
  const sampleUpdates = await pool.query(`
    SELECT 
      f.fund_name,
      fs.total_score,
      fs.recommendation,
      fs.quartile,
      fs.historical_returns_total,
      fs.risk_grade_total
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.fund_id IN (
        SELECT fund_id FROM fund_scores 
        WHERE score_date = CURRENT_DATE 
        ORDER BY total_score DESC 
        LIMIT 5
      )
    ORDER BY fs.total_score DESC
  `);
  
  console.log('\n  Sample Updated Recommendations (Top 5 funds):');
  console.log('  Fund Name'.padEnd(35) + 'Score'.padEnd(8) + 'Rec'.padEnd(12) + 'Q'.padEnd(4) + 'Returns');
  console.log('  ' + '-'.repeat(70));
  
  for (const sample of sampleUpdates.rows) {
    console.log(
      `  ${sample.fund_name.substring(0, 34)}`.padEnd(35) +
      sample.total_score.toString().padEnd(8) +
      sample.recommendation.padEnd(12) +
      sample.quartile.toString().padEnd(4) +
      sample.historical_returns_total.toString()
    );
  }
}

async function validateRecommendationDistribution() {
  console.log('\n3. Validating Recommendation Distribution...');
  
  // Check final recommendation distribution
  const finalDistribution = await pool.query(`
    SELECT 
      recommendation,
      COUNT(*) as fund_count,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
      ROUND(AVG(total_score), 2) as avg_score,
      ROUND(MIN(total_score), 2) as min_score,
      ROUND(MAX(total_score), 2) as max_score,
      ROUND(AVG(historical_returns_total), 2) as avg_returns,
      ROUND(AVG(risk_grade_total), 2) as avg_risk
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND recommendation IS NOT NULL
    GROUP BY recommendation
    ORDER BY AVG(total_score) DESC
  `);
  
  console.log('  Final Recommendation Distribution:');
  console.log('  Recommendation'.padEnd(15) + 'Count'.padEnd(8) + 'Percent'.padEnd(10) + 'Avg Score'.padEnd(12) + 'Score Range');
  console.log('  ' + '-'.repeat(70));
  
  for (const rec of finalDistribution.rows) {
    console.log(
      `  ${rec.recommendation}`.padEnd(15) +
      rec.fund_count.toString().padEnd(8) +
      rec.percentage.toString().padEnd(10) +
      rec.avg_score.toString().padEnd(12) +
      `${rec.min_score}-${rec.max_score}`
    );
  }
  
  // Check for any remaining null recommendations
  const nullCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN recommendation IS NULL THEN 1 END) as remaining_nulls,
      COUNT(CASE WHEN recommendation IS NOT NULL THEN 1 END) as has_recommendations
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const check = nullCheck.rows[0];
  
  console.log('\n  Recommendation Completion Status:');
  console.log(`    Total Funds: ${check.total_funds}`);
  console.log(`    Has Recommendations: ${check.has_recommendations}/${check.total_funds} (${Math.round(check.has_recommendations/check.total_funds*100)}%)`);
  console.log(`    Remaining Nulls: ${check.remaining_nulls} ${check.remaining_nulls === 0 ? '✓' : '⚠️'}`);
  
  // Validate recommendation logic consistency
  const logicValidation = await pool.query(`
    SELECT 
      'Logic Validation' as check_type,
      COUNT(CASE WHEN recommendation = 'STRONG_BUY' AND total_score < 60 THEN 1 END) as strong_buy_low_score,
      COUNT(CASE WHEN recommendation = 'STRONG_SELL' AND total_score > 50 THEN 1 END) as strong_sell_high_score,
      COUNT(CASE WHEN recommendation = 'BUY' AND quartile = 4 THEN 1 END) as buy_bottom_quartile,
      COUNT(CASE WHEN recommendation = 'SELL' AND quartile = 1 THEN 1 END) as sell_top_quartile
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND recommendation IS NOT NULL
  `);
  
  const validation = logicValidation.rows[0];
  
  console.log('\n  Recommendation Logic Validation:');
  console.log(`    STRONG_BUY with low score (<60): ${validation.strong_buy_low_score} ${validation.strong_buy_low_score === 0 ? '✓' : '⚠️'}`);
  console.log(`    STRONG_SELL with high score (>50): ${validation.strong_sell_high_score} ${validation.strong_sell_high_score === 0 ? '✓' : '⚠️'}`);
  console.log(`    BUY in bottom quartile: ${validation.buy_bottom_quartile} ${validation.buy_bottom_quartile === 0 ? '✓' : '⚠️'}`);
  console.log(`    SELL in top quartile: ${validation.sell_top_quartile} ${validation.sell_top_quartile === 0 ? '✓' : '⚠️'}`);
  
  if (check.remaining_nulls === 0) {
    console.log('\n  ✓ SUCCESS: All funds now have recommendation values');
    console.log('  ✓ Recommendation logic based on authentic scoring data');
    console.log('  ✓ Multi-factor analysis: score + quartile + risk metrics');
  }
}

if (require.main === module) {
  fixRecommendationLogic()
    .then(() => {
      console.log('\n✓ Recommendation logic fix completed successfully');
      process.exit(0);
    })
    .catch(error => {
      console.error('Recommendation fix failed:', error);
      process.exit(1);
    });
}

module.exports = { fixRecommendationLogic };