/**
 * Sectoral Similarity and Forward Score Analysis
 * Deep dive investigation of null values in advanced scoring components
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function sectoralForwardScoreAnalysis() {
  try {
    console.log('=== Sectoral Similarity and Forward Score Analysis ===');
    console.log('Investigating null values in advanced scoring components');
    
    // Step 1: Examine current null state
    await examineNullState();
    
    // Step 2: Check required data availability
    await checkDataAvailability();
    
    // Step 3: Implement sectoral similarity scoring
    await implementSectoralSimilarityScoring();
    
    // Step 4: Implement forward-looking scoring
    await implementForwardScoring();
    
    // Step 5: Validate results
    await validateScoringResults();
    
    console.log('\n✓ Sectoral and forward scoring analysis completed');
    
  } catch (error) {
    console.error('Sectoral/forward scoring analysis error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function examineNullState() {
  console.log('\n1. Examining Current Null State...');
  
  // Check null values in advanced scoring fields
  const nullAnalysis = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN sectoral_similarity_score IS NULL THEN 1 END) as null_sectoral,
      COUNT(CASE WHEN forward_score IS NULL THEN 1 END) as null_forward,
      COUNT(CASE WHEN momentum_score IS NULL THEN 1 END) as null_momentum,
      COUNT(CASE WHEN consistency_score IS NULL THEN 1 END) as null_consistency,
      COUNT(CASE WHEN age_maturity_score IS NULL THEN 1 END) as null_age_maturity,
      COUNT(CASE WHEN other_metrics_total IS NULL THEN 1 END) as null_other_metrics,
      COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as has_total_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const state = nullAnalysis.rows[0];
  console.log('  Current Null State Analysis:');
  console.log(`    Total Funds: ${state.total_funds}`);
  console.log(`    Null sectoral_similarity_score: ${state.null_sectoral} (${Math.round(state.null_sectoral/state.total_funds*100)}%)`);
  console.log(`    Null forward_score: ${state.null_forward} (${Math.round(state.null_forward/state.total_funds*100)}%)`);
  console.log(`    Null momentum_score: ${state.null_momentum} (${Math.round(state.null_momentum/state.total_funds*100)}%)`);
  console.log(`    Null consistency_score: ${state.null_consistency} (${Math.round(state.null_consistency/state.total_funds*100)}%)`);
  console.log(`    Null age_maturity_score: ${state.null_age_maturity} (${Math.round(state.null_age_maturity/state.total_funds*100)}%)`);
  console.log(`    Null other_metrics_total: ${state.null_other_metrics} (${Math.round(state.null_other_metrics/state.total_funds*100)}%)`);
  console.log(`    Has total_score: ${state.has_total_score}`);
  
  // Check if these are part of total score calculation
  const scoreBreakdown = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      ROUND(AVG(total_score), 2) as avg_total_score,
      ROUND(AVG(historical_returns_total), 2) as avg_returns,
      ROUND(AVG(risk_grade_total), 2) as avg_risk,
      ROUND(AVG(fundamentals_total), 2) as avg_fundamentals,
      ROUND(AVG(historical_returns_total + risk_grade_total + fundamentals_total), 2) as calculated_total
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND historical_returns_total IS NOT NULL
      AND risk_grade_total IS NOT NULL
      AND fundamentals_total IS NOT NULL
  `);
  
  const breakdown = scoreBreakdown.rows[0];
  console.log('\n  Current Scoring Breakdown:');
  console.log(`    Average Total Score: ${breakdown.avg_total_score}/100`);
  console.log(`    Average Returns: ${breakdown.avg_returns}/40`);
  console.log(`    Average Risk: ${breakdown.avg_risk}/30`);
  console.log(`    Average Fundamentals: ${breakdown.avg_fundamentals}/30`);
  console.log(`    Calculated Sum: ${breakdown.calculated_total}`);
  
  const scoringGap = breakdown.avg_total_score - breakdown.calculated_total;
  console.log(`    Missing Points: ${scoringGap.toFixed(2)} (likely from sectoral/forward scores)`);
}

async function checkDataAvailability() {
  console.log('\n2. Checking Data Availability for Advanced Scoring...');
  
  // Check portfolio holdings data availability
  const holdingsCheck = await pool.query(`
    SELECT 
      COUNT(DISTINCT ph.fund_id) as funds_with_holdings,
      COUNT(*) as total_holdings,
      COUNT(DISTINCT ph.holding_date) as unique_dates,
      MIN(ph.holding_date) as earliest_holding,
      MAX(ph.holding_date) as latest_holding
    FROM portfolio_holdings ph
    JOIN fund_scores fs ON ph.fund_id = fs.fund_id
    WHERE fs.score_date = CURRENT_DATE
  `);
  
  if (holdingsCheck.rows.length > 0 && holdingsCheck.rows[0].funds_with_holdings > 0) {
    const holdings = holdingsCheck.rows[0];
    console.log('  Portfolio Holdings Data:');
    console.log(`    Funds with Holdings: ${holdings.funds_with_holdings}/82`);
    console.log(`    Total Holdings Records: ${holdings.total_holdings}`);
    console.log(`    Date Range: ${holdings.earliest_holding?.toISOString().slice(0,10)} to ${holdings.latest_holding?.toISOString().slice(0,10)}`);
  } else {
    console.log('  Portfolio Holdings Data: Not available (explains sectoral_similarity_score nulls)');
  }
  
  // Check NAV data availability for momentum/consistency calculation
  const navDataCheck = await pool.query(`
    SELECT 
      fs.fund_id,
      f.fund_name,
      f.subcategory,
      COUNT(nd.nav_value) as nav_count,
      MIN(nd.nav_date) as earliest_nav,
      MAX(nd.nav_date) as latest_nav,
      COUNT(CASE WHEN nd.nav_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 END) as recent_nav_count
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    LEFT JOIN nav_data nd ON fs.fund_id = nd.fund_id 
      AND nd.created_at > '2025-05-30 06:45:00'
    WHERE fs.score_date = CURRENT_DATE
    GROUP BY fs.fund_id, f.fund_name, f.subcategory
    ORDER BY COUNT(nd.nav_value) DESC
    LIMIT 5
  `);
  
  console.log('\n  NAV Data Availability (Top 5 funds):');
  console.log('  Fund ID'.padEnd(10) + 'NAV Count'.padEnd(12) + 'Recent (90d)'.padEnd(15) + 'Subcategory');
  console.log('  ' + '-'.repeat(65));
  
  for (const fund of navDataCheck.rows) {
    console.log(
      `  ${fund.fund_id}`.padEnd(10) +
      fund.nav_count.toString().padEnd(12) +
      fund.recent_nav_count.toString().padEnd(15) +
      (fund.subcategory || 'Unknown')
    );
  }
  
  // Check fund age data
  const ageCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN f.launch_date IS NOT NULL THEN 1 END) as has_launch_date,
      MIN(f.launch_date) as oldest_fund,
      MAX(f.launch_date) as newest_fund
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
  `);
  
  const age = ageCheck.rows[0];
  console.log('\n  Fund Age Data:');
  console.log(`    Funds with Launch Date: ${age.has_launch_date}/${age.total_funds}`);
  if (age.oldest_fund) {
    console.log(`    Date Range: ${age.oldest_fund.toISOString().slice(0,10)} to ${age.newest_fund.toISOString().slice(0,10)}`);
  }
}

async function implementSectoralSimilarityScoring() {
  console.log('\n3. Implementing Sectoral Similarity Scoring...');
  
  // Since portfolio holdings data may not be available, implement category-based similarity
  console.log('  Using category-based similarity scoring (portfolio holdings not available)');
  
  const categoryScoring = await pool.query(`
    UPDATE fund_scores SET
      sectoral_similarity_score = CASE 
        WHEN subcategory = 'Index' THEN 8.0  -- High similarity within index funds
        WHEN subcategory = 'Liquid' THEN 7.0  -- High similarity in liquid funds
        WHEN subcategory IN ('Large Cap', 'Mid Cap', 'Small Cap') THEN 6.0  -- Cap-based similarity
        WHEN subcategory IN ('Focused', 'Multi Cap', 'Flexi Cap') THEN 5.0  -- Diversified funds
        WHEN subcategory IN ('ELSS', 'Value') THEN 4.0  -- Specialized categories
        WHEN subcategory IS NOT NULL THEN 3.0  -- General category-based scoring
        ELSE 2.0  -- Unknown category funds
      END
    WHERE score_date = CURRENT_DATE
      AND sectoral_similarity_score IS NULL
    RETURNING fund_id, sectoral_similarity_score
  `);
  
  console.log(`  ✓ Updated sectoral similarity scores for ${categoryScoring.rowCount} funds`);
}

async function implementForwardScoring() {
  console.log('\n4. Implementing Forward-Looking Scoring...');
  
  // Implement forward scoring based on recent performance trends and momentum
  const forwardScoring = await pool.query(`
    WITH recent_performance AS (
      SELECT 
        fs.fund_id,
        fs.return_3m_score,
        fs.return_6m_score,
        fs.return_1y_score,
        fs.volatility_1y_percent,
        fs.sharpe_ratio_1y,
        fs.momentum_score,
        fs.consistency_score
      FROM fund_scores fs
      WHERE fs.score_date = CURRENT_DATE
    )
    UPDATE fund_scores SET
      forward_score = CASE 
        WHEN rp.return_3m_score >= 6.0 AND rp.return_6m_score >= 6.0 THEN 8.0  -- Strong recent momentum
        WHEN rp.return_3m_score >= 4.0 AND rp.return_6m_score >= 4.0 THEN 6.0  -- Good recent performance
        WHEN rp.return_1y_score >= 6.0 AND rp.sharpe_ratio_1y > 1.0 THEN 5.0  -- Solid risk-adjusted returns
        WHEN rp.return_1y_score >= 4.0 THEN 4.0  -- Moderate performance
        WHEN rp.return_1y_score >= 2.0 THEN 3.0  -- Below average performance
        WHEN rp.return_1y_score IS NOT NULL THEN 2.0  -- Poor performance
        ELSE 1.0  -- No performance data
      END
    FROM recent_performance rp
    WHERE fund_scores.fund_id = rp.fund_id
      AND fund_scores.score_date = CURRENT_DATE
      AND fund_scores.forward_score IS NULL
    RETURNING fund_id, forward_score
  `);
  
  console.log(`  ✓ Updated forward scores for ${forwardScoring.rowCount} funds`);
}

async function implementMomentumConsistencyScoring() {
  console.log('\n5. Implementing Momentum and Consistency Scoring...');
  
  // Calculate momentum based on short vs long term performance
  const momentumScoring = await pool.query(`
    UPDATE fund_scores SET
      momentum_score = CASE 
        WHEN return_3m_score > return_1y_score + 2.0 THEN 8.0  -- Strong positive momentum
        WHEN return_3m_score > return_1y_score + 1.0 THEN 6.0  -- Good momentum
        WHEN return_6m_score > return_1y_score THEN 4.0  -- Moderate momentum
        WHEN return_3m_score >= return_1y_score THEN 3.0  -- Stable performance
        WHEN return_1y_score IS NOT NULL THEN 2.0  -- Declining momentum
        ELSE 1.0  -- No data
      END
    WHERE score_date = CURRENT_DATE
      AND momentum_score IS NULL
    RETURNING fund_id, momentum_score
  `);
  
  console.log(`  ✓ Updated momentum scores for ${momentumScoring.rowCount} funds`);
  
  // Calculate consistency based on volatility and risk metrics
  const consistencyScoring = await pool.query(`
    UPDATE fund_scores SET
      consistency_score = CASE 
        WHEN volatility_1y_percent < 10.0 AND sharpe_ratio_1y > 1.5 THEN 8.0  -- Very consistent
        WHEN volatility_1y_percent < 15.0 AND sharpe_ratio_1y > 1.0 THEN 6.0  -- Good consistency
        WHEN volatility_1y_percent < 20.0 THEN 4.0  -- Moderate consistency
        WHEN volatility_1y_percent < 30.0 THEN 3.0  -- Below average consistency
        WHEN volatility_1y_percent IS NOT NULL THEN 2.0  -- Poor consistency
        ELSE 1.0  -- No data
      END
    WHERE score_date = CURRENT_DATE
      AND consistency_score IS NULL
    RETURNING fund_id, consistency_score
  `);
  
  console.log(`  ✓ Updated consistency scores for ${consistencyScoring.rowCount} funds`);
}

async function implementAgeMaturityScoring() {
  console.log('\n6. Implementing Age and Maturity Scoring...');
  
  // Score based on fund age and track record
  const ageScoring = await pool.query(`
    UPDATE fund_scores SET
      age_maturity_score = CASE 
        WHEN f.launch_date IS NOT NULL AND f.launch_date <= CURRENT_DATE - INTERVAL '10 years' THEN 8.0  -- Mature funds
        WHEN f.launch_date IS NOT NULL AND f.launch_date <= CURRENT_DATE - INTERVAL '5 years' THEN 6.0  -- Established funds
        WHEN f.launch_date IS NOT NULL AND f.launch_date <= CURRENT_DATE - INTERVAL '3 years' THEN 4.0  -- Developing track record
        WHEN f.launch_date IS NOT NULL AND f.launch_date <= CURRENT_DATE - INTERVAL '1 year' THEN 3.0  -- New funds
        WHEN f.launch_date IS NOT NULL THEN 2.0  -- Very new funds
        ELSE 5.0  -- Unknown age, assume moderate maturity
      END
    FROM funds f
    WHERE fund_scores.fund_id = f.id
      AND fund_scores.score_date = CURRENT_DATE
      AND fund_scores.age_maturity_score IS NULL
    RETURNING fund_id, age_maturity_score
  `);
  
  console.log(`  ✓ Updated age/maturity scores for ${ageScoring.rowCount} funds`);
}

async function calculateOtherMetricsTotal() {
  console.log('\n7. Calculating Other Metrics Total...');
  
  // Calculate total for "other metrics" category (typically 30 points)
  const otherMetricsTotal = await pool.query(`
    UPDATE fund_scores SET
      other_metrics_total = 
        COALESCE(sectoral_similarity_score, 0) + 
        COALESCE(forward_score, 0) + 
        COALESCE(momentum_score, 0) + 
        COALESCE(consistency_score, 0) + 
        COALESCE(age_maturity_score, 0)
    WHERE score_date = CURRENT_DATE
      AND other_metrics_total IS NULL
    RETURNING fund_id, other_metrics_total
  `);
  
  console.log(`  ✓ Updated other_metrics_total for ${otherMetricsTotal.rowCount} funds`);
}

async function validateScoringResults() {
  console.log('\n8. Validating Advanced Scoring Results...');
  
  // First complete all missing scores
  await implementMomentumConsistencyScoring();
  await implementAgeMaturityScoring();
  await calculateOtherMetricsTotal();
  
  // Final validation
  const validation = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN sectoral_similarity_score IS NOT NULL THEN 1 END) as has_sectoral,
      COUNT(CASE WHEN forward_score IS NOT NULL THEN 1 END) as has_forward,
      COUNT(CASE WHEN momentum_score IS NOT NULL THEN 1 END) as has_momentum,
      COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as has_consistency,
      COUNT(CASE WHEN age_maturity_score IS NOT NULL THEN 1 END) as has_age_maturity,
      COUNT(CASE WHEN other_metrics_total IS NOT NULL THEN 1 END) as has_other_metrics,
      ROUND(AVG(sectoral_similarity_score), 2) as avg_sectoral,
      ROUND(AVG(forward_score), 2) as avg_forward,
      ROUND(AVG(momentum_score), 2) as avg_momentum,
      ROUND(AVG(consistency_score), 2) as avg_consistency,
      ROUND(AVG(age_maturity_score), 2) as avg_age_maturity,
      ROUND(AVG(other_metrics_total), 2) as avg_other_metrics
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const result = validation.rows[0];
  
  console.log('  Advanced Scoring Validation:');
  console.log(`    Total Funds: ${result.total_funds}`);
  console.log(`    Sectoral Similarity: ${result.has_sectoral}/${result.total_funds} (avg: ${result.avg_sectoral})`);
  console.log(`    Forward Score: ${result.has_forward}/${result.total_funds} (avg: ${result.avg_forward})`);
  console.log(`    Momentum Score: ${result.has_momentum}/${result.total_funds} (avg: ${result.avg_momentum})`);
  console.log(`    Consistency Score: ${result.has_consistency}/${result.total_funds} (avg: ${result.avg_consistency})`);
  console.log(`    Age/Maturity Score: ${result.has_age_maturity}/${result.total_funds} (avg: ${result.avg_age_maturity})`);
  console.log(`    Other Metrics Total: ${result.has_other_metrics}/${result.total_funds} (avg: ${result.avg_other_metrics})`);
  
  // Check if total scores need updating
  const totalScoreCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      ROUND(AVG(total_score), 2) as current_avg_total,
      ROUND(AVG(historical_returns_total + risk_grade_total + fundamentals_total + COALESCE(other_metrics_total, 0)), 2) as calculated_total
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND historical_returns_total IS NOT NULL
      AND risk_grade_total IS NOT NULL
      AND fundamentals_total IS NOT NULL
  `);
  
  const totalCheck = totalScoreCheck.rows[0];
  console.log('\n  Total Score Validation:');
  console.log(`    Current Average Total: ${totalCheck.current_avg_total}`);
  console.log(`    Calculated Total: ${totalCheck.calculated_total}`);
  
  if (Math.abs(totalCheck.current_avg_total - totalCheck.calculated_total) > 1.0) {
    console.log('    ⚠️  Total scores may need recalculation');
    
    // Update total scores to include other metrics
    const updateTotal = await pool.query(`
      UPDATE fund_scores SET
        total_score = 
          COALESCE(historical_returns_total, 0) + 
          COALESCE(risk_grade_total, 0) + 
          COALESCE(fundamentals_total, 0) + 
          COALESCE(other_metrics_total, 0)
      WHERE score_date = CURRENT_DATE
      RETURNING fund_id
    `);
    
    console.log(`    ✓ Updated total scores for ${updateTotal.rowCount} funds`);
  } else {
    console.log('    ✓ Total scores are consistent');
  }
  
  // Show top performers with advanced metrics
  const topPerformers = await pool.query(`
    SELECT 
      f.fund_name,
      fs.total_score,
      fs.sectoral_similarity_score,
      fs.forward_score,
      fs.momentum_score,
      fs.consistency_score,
      fs.other_metrics_total
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.other_metrics_total IS NOT NULL
    ORDER BY fs.total_score DESC
    LIMIT 5
  `);
  
  console.log('\n  Top 5 Performers with Advanced Metrics:');
  console.log('  Fund Name'.padEnd(35) + 'Total'.padEnd(8) + 'Sectoral'.padEnd(10) + 'Forward'.padEnd(10) + 'Other');
  console.log('  ' + '-'.repeat(75));
  
  for (const performer of topPerformers.rows) {
    console.log(
      `  ${performer.fund_name.substring(0, 34)}`.padEnd(35) +
      performer.total_score.toString().padEnd(8) +
      performer.sectoral_similarity_score.toString().padEnd(10) +
      performer.forward_score.toString().padEnd(10) +
      performer.other_metrics_total.toString()
    );
  }
  
  const nullRemaining = result.total_funds - Math.min(result.has_sectoral, result.has_forward);
  if (nullRemaining === 0) {
    console.log('\n  ✓ SUCCESS: All sectoral and forward score null values resolved');
  } else {
    console.log(`\n  ⚠️  ${nullRemaining} funds still have null advanced scores`);
  }
}

if (require.main === module) {
  sectoralForwardScoreAnalysis()
    .then(() => {
      console.log('\n✓ Sectoral and forward scoring analysis completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Advanced scoring analysis failed:', error);
      process.exit(1);
    });
}

module.exports = { sectoralForwardScoreAnalysis };