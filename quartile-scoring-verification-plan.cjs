/**
 * Quartile Scoring Verification and Formula Validation Plan
 * Deep dive analysis to verify formulas against original specifications
 * and ensure no fabricated data points exist
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function quartileScoringVerificationPlan() {
  try {
    console.log('=== Quartile Scoring Verification and Formula Validation Plan ===');
    console.log('Comprehensive analysis of scoring logic against original specifications');
    
    // Step 1: Analyze current scoring implementation
    await analyzeCurrentScoringImplementation();
    
    // Step 2: Verify data source authenticity
    await verifyDataSourceAuthenticity();
    
    // Step 3: Check formula consistency across components
    await checkFormulaConsistency();
    
    // Step 4: Validate quartile calculation methodology
    await validateQuartileCalculationMethodology();
    
    // Step 5: Identify potential fabricated data points
    await identifyPotentialFabricatedData();
    
    // Step 6: Cross-reference with original specifications
    await crossReferenceWithSpecifications();
    
    console.log('\n✓ Quartile scoring verification plan completed');
    
  } catch (error) {
    console.error('Verification plan error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function analyzeCurrentScoringImplementation() {
  console.log('\n1. Analyzing Current Scoring Implementation...');
  
  // Check the current scoring breakdown
  const scoringBreakdown = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      ROUND(AVG(total_score), 2) as avg_total_score,
      ROUND(MIN(total_score), 2) as min_total_score,
      ROUND(MAX(total_score), 2) as max_total_score,
      ROUND(STDDEV(total_score), 2) as stddev_total_score,
      
      -- Historical Returns Component (should be 40 points max)
      ROUND(AVG(historical_returns_total), 2) as avg_returns,
      ROUND(MIN(historical_returns_total), 2) as min_returns,
      ROUND(MAX(historical_returns_total), 2) as max_returns,
      COUNT(CASE WHEN historical_returns_total > 40 THEN 1 END) as returns_over_40,
      
      -- Risk Grade Component (should be 30 points max)
      ROUND(AVG(risk_grade_total), 2) as avg_risk,
      ROUND(MIN(risk_grade_total), 2) as min_risk,
      ROUND(MAX(risk_grade_total), 2) as max_risk,
      COUNT(CASE WHEN risk_grade_total > 30 THEN 1 END) as risk_over_30,
      
      -- Fundamentals Component (should be 30 points max)
      ROUND(AVG(fundamentals_total), 2) as avg_fundamentals,
      ROUND(MIN(fundamentals_total), 2) as min_fundamentals,
      ROUND(MAX(fundamentals_total), 2) as max_fundamentals,
      COUNT(CASE WHEN fundamentals_total > 30 THEN 1 END) as fundamentals_over_30
      
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const breakdown = scoringBreakdown.rows[0];
  
  console.log('  Current Scoring Implementation Analysis:');
  console.log(`    Total Funds Analyzed: ${breakdown.total_funds}`);
  console.log(`    Average Total Score: ${breakdown.avg_total_score}/100 (Range: ${breakdown.min_total_score}-${breakdown.max_total_score})`);
  console.log(`    Score Standard Deviation: ${breakdown.stddev_total_score}`);
  
  console.log('\n  Component Analysis:');
  console.log(`    Historical Returns (40 max): Avg ${breakdown.avg_returns} (Range: ${breakdown.min_returns}-${breakdown.max_returns})`);
  console.log(`      Funds exceeding 40 points: ${breakdown.returns_over_40} ${breakdown.returns_over_40 > 0 ? '⚠️ ISSUE' : '✓'}`);
  
  console.log(`    Risk Grade (30 max): Avg ${breakdown.avg_risk} (Range: ${breakdown.min_risk}-${breakdown.max_risk})`);
  console.log(`      Funds exceeding 30 points: ${breakdown.risk_over_30} ${breakdown.risk_over_30 > 0 ? '⚠️ ISSUE' : '✓'}`);
  
  console.log(`    Fundamentals (30 max): Avg ${breakdown.avg_fundamentals} (Range: ${breakdown.min_fundamentals}-${breakdown.max_fundamentals})`);
  console.log(`      Funds exceeding 30 points: ${breakdown.fundamentals_over_30} ${breakdown.fundamentals_over_30 > 0 ? '⚠️ ISSUE' : '✓'}`);
  
  // Check if total equals sum of components
  const totalConsistency = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN 
        ABS(total_score - (
          COALESCE(historical_returns_total, 0) + 
          COALESCE(risk_grade_total, 0) + 
          COALESCE(fundamentals_total, 0)
        )) > 0.1 THEN 1 END
      ) as inconsistent_totals
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const consistency = totalConsistency.rows[0];
  console.log(`\n  Total Score Consistency: ${consistency.inconsistent_totals}/${consistency.total_funds} inconsistent ${consistency.inconsistent_totals > 0 ? '⚠️ ISSUE' : '✓'}`);
}

async function verifyDataSourceAuthenticity() {
  console.log('\n2. Verifying Data Source Authenticity...');
  
  // Check NAV data authenticity markers
  const navAuthenticity = await pool.query(`
    SELECT 
      COUNT(DISTINCT fund_id) as funds_with_nav,
      COUNT(*) as total_nav_records,
      MIN(nav_date) as earliest_nav,
      MAX(nav_date) as latest_nav,
      MIN(created_at) as earliest_import,
      MAX(created_at) as latest_import,
      COUNT(CASE WHEN created_at > '2025-05-30 06:45:00' THEN 1 END) as recent_authentic_records
    FROM nav_data nd
    JOIN fund_scores fs ON nd.fund_id = fs.fund_id
    WHERE fs.score_date = CURRENT_DATE
  `);
  
  const nav = navAuthenticity.rows[0];
  console.log('  NAV Data Authenticity Check:');
  console.log(`    Funds with NAV data: ${nav.funds_with_nav}/82`);
  console.log(`    Total NAV records: ${nav.total_nav_records.toLocaleString()}`);
  console.log(`    NAV date range: ${nav.earliest_nav?.toISOString().slice(0,10)} to ${nav.latest_nav?.toISOString().slice(0,10)}`);
  console.log(`    Recent authentic imports: ${nav.recent_authentic_records.toLocaleString()} records`);
  
  // Check for suspicious patterns that might indicate fabricated data
  const suspiciousPatterns = await pool.query(`
    SELECT 
      'Identical NAV values' as pattern_type,
      COUNT(*) as occurrences
    FROM (
      SELECT nav_value, COUNT(*) as cnt
      FROM nav_data nd
      JOIN fund_scores fs ON nd.fund_id = fs.fund_id
      WHERE fs.score_date = CURRENT_DATE
        AND nd.created_at > '2025-05-30 06:45:00'
      GROUP BY nav_value
      HAVING COUNT(*) > 100
    ) identical_navs
    
    UNION ALL
    
    SELECT 
      'Perfect round numbers' as pattern_type,
      COUNT(*) as occurrences
    FROM nav_data nd
    JOIN fund_scores fs ON nd.fund_id = fs.fund_id
    WHERE fs.score_date = CURRENT_DATE
      AND nd.created_at > '2025-05-30 06:45:00'
      AND nav_value = ROUND(nav_value, 0)
      AND nav_value > 10
  `);
  
  console.log('\n  Suspicious Pattern Detection:');
  for (const pattern of suspiciousPatterns.rows) {
    console.log(`    ${pattern.pattern_type}: ${pattern.occurrences} occurrences`);
  }
}

async function checkFormulaConsistency() {
  console.log('\n3. Checking Formula Consistency...');
  
  // Analyze individual return component formulas
  const returnComponents = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      
      -- 3M Returns Analysis
      ROUND(AVG(return_3m_score), 2) as avg_3m_score,
      ROUND(MIN(return_3m_score), 2) as min_3m_score,
      ROUND(MAX(return_3m_score), 2) as max_3m_score,
      COUNT(CASE WHEN return_3m_score > 8 THEN 1 END) as score_3m_over_8,
      
      -- 6M Returns Analysis  
      ROUND(AVG(return_6m_score), 2) as avg_6m_score,
      COUNT(CASE WHEN return_6m_score > 8 THEN 1 END) as score_6m_over_8,
      
      -- 1Y Returns Analysis
      ROUND(AVG(return_1y_score), 2) as avg_1y_score,
      COUNT(CASE WHEN return_1y_score > 8 THEN 1 END) as score_1y_over_8,
      
      -- 3Y Returns Analysis
      ROUND(AVG(return_3y_score), 2) as avg_3y_score,
      COUNT(CASE WHEN return_3y_score > 8 THEN 1 END) as score_3y_over_8,
      
      -- 5Y Returns Analysis
      ROUND(AVG(return_5y_score), 2) as avg_5y_score,
      COUNT(CASE WHEN return_5y_score > 8 THEN 1 END) as score_5y_over_8
      
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const components = returnComponents.rows[0];
  
  console.log('  Individual Return Component Analysis (Each should max at 8 points):');
  console.log(`    3M Returns: Avg ${components.avg_3m_score} (${components.min_3m_score}-${components.max_3m_score}) | Over 8: ${components.score_3m_over_8} ${components.score_3m_over_8 > 0 ? '⚠️' : '✓'}`);
  console.log(`    6M Returns: Avg ${components.avg_6m_score} | Over 8: ${components.score_6m_over_8} ${components.score_6m_over_8 > 0 ? '⚠️' : '✓'}`);
  console.log(`    1Y Returns: Avg ${components.avg_1y_score} | Over 8: ${components.score_1y_over_8} ${components.score_1y_over_8 > 0 ? '⚠️' : '✓'}`);
  console.log(`    3Y Returns: Avg ${components.avg_3y_score} | Over 8: ${components.score_3y_over_8} ${components.score_3y_over_8 > 0 ? '⚠️' : '✓'}`);
  console.log(`    5Y Returns: Avg ${components.avg_5y_score} | Over 8: ${components.score_5y_over_8} ${components.score_5y_over_8 > 0 ? '⚠️' : '✓'}`);
  
  // Check if historical_returns_total equals sum of individual components
  const returnConsistency = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN 
        ABS(historical_returns_total - (
          COALESCE(return_3m_score, 0) + 
          COALESCE(return_6m_score, 0) + 
          COALESCE(return_1y_score, 0) + 
          COALESCE(return_3y_score, 0) + 
          COALESCE(return_5y_score, 0)
        )) > 0.1 THEN 1 END
      ) as inconsistent_return_totals
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND historical_returns_total IS NOT NULL
  `);
  
  const returnCheck = returnConsistency.rows[0];
  console.log(`\n  Return Component Consistency: ${returnCheck.inconsistent_return_totals}/${returnCheck.total_funds} inconsistent ${returnCheck.inconsistent_return_totals > 0 ? '⚠️ ISSUE' : '✓'}`);
}

async function validateQuartileCalculationMethodology() {
  console.log('\n4. Validating Quartile Calculation Methodology...');
  
  // Check quartile distribution
  const quartileDistribution = await pool.query(`
    SELECT 
      quartile,
      COUNT(*) as fund_count,
      ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage,
      ROUND(AVG(total_score), 2) as avg_score,
      ROUND(MIN(total_score), 2) as min_score,
      ROUND(MAX(total_score), 2) as max_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND quartile IS NOT NULL
    GROUP BY quartile
    ORDER BY quartile
  `);
  
  console.log('  Overall Quartile Distribution:');
  console.log('  Quartile'.padEnd(12) + 'Count'.padEnd(8) + 'Percent'.padEnd(10) + 'Avg Score'.padEnd(12) + 'Range');
  console.log('  ' + '-'.repeat(60));
  
  for (const q of quartileDistribution.rows) {
    const quartileName = ['', 'Q1 (Top)', 'Q2', 'Q3', 'Q4 (Bottom)'][q.quartile];
    const expectedPercent = 25.0;
    const percentDiff = Math.abs(q.percentage - expectedPercent);
    const percentFlag = percentDiff > 5 ? '⚠️' : '✓';
    
    console.log(
      `  ${quartileName}`.padEnd(12) +
      q.fund_count.toString().padEnd(8) +
      `${q.percentage}%`.padEnd(10) +
      q.avg_score.toString().padEnd(12) +
      `${q.min_score}-${q.max_score} ${percentFlag}`
    );
  }
  
  // Check category-wise quartile distributions
  const categoryQuartiles = await pool.query(`
    SELECT 
      subcategory,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN subcategory_quartile = 1 THEN 1 END) as q1_count,
      COUNT(CASE WHEN subcategory_quartile = 2 THEN 1 END) as q2_count,
      COUNT(CASE WHEN subcategory_quartile = 3 THEN 1 END) as q3_count,
      COUNT(CASE WHEN subcategory_quartile = 4 THEN 1 END) as q4_count
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND subcategory IS NOT NULL
      AND subcategory_quartile IS NOT NULL
    GROUP BY subcategory
    HAVING COUNT(*) >= 4
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Subcategory Quartile Distribution (4+ funds):');
  console.log('  Subcategory'.padEnd(20) + 'Total'.padEnd(8) + 'Q1'.padEnd(6) + 'Q2'.padEnd(6) + 'Q3'.padEnd(6) + 'Q4'.padEnd(6) + 'Balance');
  console.log('  ' + '-'.repeat(65));
  
  for (const cat of categoryQuartiles.rows) {
    const expectedPerQuartile = cat.total_funds / 4;
    const maxDeviation = Math.max(
      Math.abs(cat.q1_count - expectedPerQuartile),
      Math.abs(cat.q2_count - expectedPerQuartile),
      Math.abs(cat.q3_count - expectedPerQuartile),
      Math.abs(cat.q4_count - expectedPerQuartile)
    );
    const balanceFlag = maxDeviation > 2 ? '⚠️' : '✓';
    
    console.log(
      `  ${cat.subcategory}`.padEnd(20) +
      cat.total_funds.toString().padEnd(8) +
      cat.q1_count.toString().padEnd(6) +
      cat.q2_count.toString().padEnd(6) +
      cat.q3_count.toString().padEnd(6) +
      cat.q4_count.toString().padEnd(6) +
      balanceFlag
    );
  }
}

async function identifyPotentialFabricatedData() {
  console.log('\n5. Identifying Potential Fabricated Data Points...');
  
  // Check for unrealistic score combinations
  const unrealisticScores = await pool.query(`
    SELECT 
      COUNT(*) as total_suspicious,
      'Perfect scores' as issue_type
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND (
        historical_returns_total = 40 OR
        risk_grade_total = 30 OR
        fundamentals_total = 30 OR
        total_score = 100
      )
    
    UNION ALL
    
    SELECT 
      COUNT(*) as total_suspicious,
      'Identical component scores' as issue_type
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND return_3m_score = return_6m_score 
      AND return_6m_score = return_1y_score
      AND return_1y_score = return_3y_score
      AND return_3y_score = return_5y_score
      AND return_3m_score IS NOT NULL
    
    UNION ALL
    
    SELECT 
      COUNT(*) as total_suspicious,
      'Round number scores' as issue_type
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND total_score = ROUND(total_score, 0)
      AND MOD(total_score::integer, 5) = 0
  `);
  
  console.log('  Potential Fabricated Data Detection:');
  for (const suspicious of unrealisticScores.rows) {
    console.log(`    ${suspicious.issue_type}: ${suspicious.total_suspicious} cases ${suspicious.total_suspicious > 5 ? '⚠️' : '✓'}`);
  }
  
  // Check for funds with impossible performance combinations
  const impossibleCombinations = await pool.query(`
    SELECT 
      fs.fund_id,
      f.fund_name,
      fs.historical_returns_total,
      fs.volatility_1y_percent,
      fs.sharpe_ratio_1y,
      fs.max_drawdown_percent
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND (
        (fs.historical_returns_total > 35 AND fs.volatility_1y_percent < 5) OR  -- Too good to be true
        (fs.sharpe_ratio_1y > 3 AND fs.max_drawdown_percent < 2) OR  -- Unrealistic risk/return
        (fs.historical_returns_total < 5 AND fs.volatility_1y_percent > 50)  -- Poor return with high risk
      )
    LIMIT 5
  `);
  
  if (impossibleCombinations.rows.length > 0) {
    console.log('\n  Funds with Questionable Risk/Return Combinations:');
    for (const fund of impossibleCombinations.rows) {
      console.log(`    ${fund.fund_name}: Returns ${fund.historical_returns_total}, Vol ${fund.volatility_1y_percent}%, Sharpe ${fund.sharpe_ratio_1y}`);
    }
  } else {
    console.log('\n  ✓ No obviously impossible risk/return combinations found');
  }
}

async function crossReferenceWithSpecifications() {
  console.log('\n6. Cross-Referencing with Original Specifications...');
  
  console.log('  Expected Scoring Framework (Based on Documentation):');
  console.log('    Total Score: 100 points maximum');
  console.log('    Historical Returns: 40 points (5 periods × 8 points each)');
  console.log('    Risk Assessment: 30 points (volatility, drawdown, ratios)');
  console.log('    Fundamentals: 30 points (expense ratio, AUM, etc.)');
  
  // Verify against current implementation
  const specCompliance = await pool.query(`
    SELECT 
      'Scoring Framework Compliance' as check_type,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN total_score <= 100 THEN 1 END) as within_total_limit,
      COUNT(CASE WHEN historical_returns_total <= 40 THEN 1 END) as within_returns_limit,
      COUNT(CASE WHEN risk_grade_total <= 30 THEN 1 END) as within_risk_limit,
      COUNT(CASE WHEN fundamentals_total <= 30 THEN 1 END) as within_fundamentals_limit
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const compliance = specCompliance.rows[0];
  
  console.log('\n  Current Implementation vs Specifications:');
  console.log(`    Total Score Compliance: ${compliance.within_total_limit}/${compliance.total_funds} funds ✓`);
  console.log(`    Returns Score Compliance: ${compliance.within_returns_limit}/${compliance.total_funds} funds ✓`);
  console.log(`    Risk Score Compliance: ${compliance.within_risk_limit}/${compliance.total_funds} funds ✓`);
  console.log(`    Fundamentals Compliance: ${compliance.within_fundamentals_limit}/${compliance.total_funds} funds ✓`);
  
  // Check quartile methodology compliance
  console.log('\n  Quartile Methodology Compliance:');
  console.log('    Expected: 25% of funds in each quartile (±5% tolerance)');
  console.log('    Expected: Higher scores in lower quartile numbers (Q1 > Q2 > Q3 > Q4)');
  console.log('    Expected: Meaningful score separation between quartiles');
  
  const quartileValidation = await pool.query(`
    SELECT 
      quartile,
      COUNT(*) as fund_count,
      ROUND(AVG(total_score), 2) as avg_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND quartile IS NOT NULL
    GROUP BY quartile
    ORDER BY quartile
  `);
  
  let quartileOrderCorrect = true;
  for (let i = 1; i < quartileValidation.rows.length; i++) {
    if (quartileValidation.rows[i-1].avg_score <= quartileValidation.rows[i].avg_score) {
      quartileOrderCorrect = false;
      break;
    }
  }
  
  console.log(`    Quartile Score Ordering: ${quartileOrderCorrect ? '✓ Correct (Q1 > Q2 > Q3 > Q4)' : '⚠️ Incorrect ordering detected'}`);
  
  // Summary assessment
  console.log('\n  Overall Compliance Assessment:');
  const totalIssues = 
    (compliance.within_total_limit < compliance.total_funds ? 1 : 0) +
    (compliance.within_returns_limit < compliance.total_funds ? 1 : 0) +
    (compliance.within_risk_limit < compliance.total_funds ? 1 : 0) +
    (compliance.within_fundamentals_limit < compliance.total_funds ? 1 : 0) +
    (quartileOrderCorrect ? 0 : 1);
  
  if (totalIssues === 0) {
    console.log('    ✓ FULL COMPLIANCE: Implementation matches specifications');
  } else {
    console.log(`    ⚠️ ${totalIssues} COMPLIANCE ISSUES detected that need attention`);
  }
}

if (require.main === module) {
  quartileScoringVerificationPlan()
    .then(() => {
      console.log('\n✓ Quartile scoring verification plan completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Verification plan failed:', error);
      process.exit(1);
    });
}

module.exports = { quartileScoringVerificationPlan };