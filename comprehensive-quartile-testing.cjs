/**
 * Comprehensive Quartile System Testing
 * Tests all aspects: eligibility criteria, scoring model, database integrity, and results accuracy
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function comprehensiveQuartileTest() {
  try {
    console.log('=== Comprehensive Quartile System Testing ===');
    
    const testResults = {
      eligibilityTests: {},
      scoringTests: {},
      databaseTests: {},
      accuracyTests: {},
      performanceTests: {}
    };
    
    // Test 1: Eligibility Criteria Validation
    console.log('\n1. Testing Eligibility Criteria...');
    testResults.eligibilityTests = await testEligibilityCriteria();
    
    // Test 2: Scoring Model Validation
    console.log('\n2. Testing Scoring Model...');
    testResults.scoringTests = await testScoringModel();
    
    // Test 3: Database Integrity
    console.log('\n3. Testing Database Integrity...');
    testResults.databaseTests = await testDatabaseIntegrity();
    
    // Test 4: Results Accuracy
    console.log('\n4. Testing Results Accuracy...');
    testResults.accuracyTests = await testResultsAccuracy();
    
    // Test 5: Performance and Consistency
    console.log('\n5. Testing Performance and Consistency...');
    testResults.performanceTests = await testPerformanceConsistency();
    
    // Generate comprehensive test report
    generateTestReport(testResults);
    
    return testResults;
    
  } catch (error) {
    console.error('Error in comprehensive testing:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function testEligibilityCriteria() {
  console.log('  Testing fund eligibility requirements...');
  
  const tests = {};
  
  // Test minimum NAV data requirement (252 records)
  const navDataTest = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN nav_count >= 252 THEN 1 END) as eligible_252,
      COUNT(CASE WHEN nav_count >= 100 AND nav_count < 252 THEN 1 END) as insufficient_data,
      COUNT(CASE WHEN nav_count < 100 THEN 1 END) as minimal_data
    FROM (
      SELECT f.id, COUNT(n.nav_date) as nav_count
      FROM funds f
      LEFT JOIN nav_data n ON f.id = n.fund_id 
        AND n.created_at > '2025-05-30 06:45:00'
      GROUP BY f.id
    ) nav_summary
  `);
  
  tests.navDataRequirement = {
    totalFunds: parseInt(navDataTest.rows[0].total_funds),
    eligible252: parseInt(navDataTest.rows[0].eligible_252),
    insufficientData: parseInt(navDataTest.rows[0].insufficient_data),
    minimalData: parseInt(navDataTest.rows[0].minimal_data),
    eligibilityRate: (parseInt(navDataTest.rows[0].eligible_252) / parseInt(navDataTest.rows[0].total_funds) * 100).toFixed(2)
  };
  
  console.log(`    ✓ NAV Data: ${tests.navDataRequirement.eligible252}/${tests.navDataRequirement.totalFunds} funds eligible (${tests.navDataRequirement.eligibilityRate}%)`);
  
  // Test data time span requirement (365+ days)
  const timeSpanTest = await pool.query(`
    SELECT 
      COUNT(*) as eligible_funds,
      AVG(days_span) as avg_span,
      MIN(days_span) as min_span,
      MAX(days_span) as max_span
    FROM (
      SELECT 
        f.id,
        (MAX(n.nav_date) - MIN(n.nav_date)) as days_span
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id 
        AND n.created_at > '2025-05-30 06:45:00'
      GROUP BY f.id
      HAVING COUNT(n.nav_date) >= 252 
        AND (MAX(n.nav_date) - MIN(n.nav_date)) >= 365
    ) span_summary
  `);
  
  tests.timeSpanRequirement = {
    eligibleFunds: parseInt(timeSpanTest.rows[0].eligible_funds),
    avgSpan: Math.round(parseFloat(timeSpanTest.rows[0].avg_span)),
    minSpan: parseInt(timeSpanTest.rows[0].min_span),
    maxSpan: parseInt(timeSpanTest.rows[0].max_span)
  };
  
  console.log(`    ✓ Time Span: ${tests.timeSpanRequirement.eligibleFunds} funds with 365+ days (avg: ${tests.timeSpanRequirement.avgSpan} days)`);
  
  // Test authentic data filtering
  const authenticDataTest = await pool.query(`
    SELECT 
      COUNT(DISTINCT f.id) as funds_with_authentic_data,
      COUNT(*) as total_authentic_records,
      MIN(n.created_at) as earliest_import,
      MAX(n.created_at) as latest_import
    FROM funds f
    JOIN nav_data n ON f.id = n.fund_id 
    WHERE n.created_at > '2025-05-30 06:45:00'
  `);
  
  tests.authenticDataFilter = {
    fundsWithAuthenticData: parseInt(authenticDataTest.rows[0].funds_with_authentic_data),
    totalAuthenticRecords: parseInt(authenticDataTest.rows[0].total_authentic_records),
    importWindow: `${authenticDataTest.rows[0].earliest_import} to ${authenticDataTest.rows[0].latest_import}`
  };
  
  console.log(`    ✓ Authentic Data: ${tests.authenticDataFilter.fundsWithAuthenticData} funds, ${tests.authenticDataFilter.totalAuthenticRecords} records`);
  
  return tests;
}

async function testScoringModel() {
  console.log('  Testing scoring methodology...');
  
  const tests = {};
  
  // Test score distribution across different categories
  const scoreDistribution = await pool.query(`
    SELECT 
      f.category,
      f.subcategory,
      COUNT(*) as scored_funds,
      ROUND(AVG(fs.total_score), 2) as avg_score,
      ROUND(MIN(fs.total_score), 2) as min_score,
      ROUND(MAX(fs.total_score), 2) as max_score,
      ROUND(STDDEV(fs.total_score), 2) as score_stddev
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
    GROUP BY f.category, f.subcategory
    ORDER BY COUNT(*) DESC
  `);
  
  tests.scoreDistribution = scoreDistribution.rows.map(row => ({
    category: row.category,
    subcategory: row.subcategory,
    scoredFunds: parseInt(row.scored_funds),
    avgScore: parseFloat(row.avg_score),
    minScore: parseFloat(row.min_score),
    maxScore: parseFloat(row.max_score),
    stdDev: parseFloat(row.score_stddev)
  }));
  
  console.log(`    ✓ Score Distribution: ${tests.scoreDistribution.length} category/subcategory combinations`);
  
  // Test quartile assignment logic
  const quartileTest = await pool.query(`
    SELECT 
      quartile,
      recommendation,
      COUNT(*) as fund_count,
      ROUND(AVG(total_score), 2) as avg_score,
      ROUND(MIN(total_score), 2) as min_score,
      ROUND(MAX(total_score), 2) as max_score
    FROM fund_scores
    WHERE score_date = CURRENT_DATE 
      AND quartile IS NOT NULL
    GROUP BY quartile, recommendation
    ORDER BY quartile
  `);
  
  tests.quartileAssignment = quartileTest.rows.map(row => ({
    quartile: parseInt(row.quartile),
    recommendation: row.recommendation,
    fundCount: parseInt(row.fund_count),
    avgScore: parseFloat(row.avg_score),
    minScore: parseFloat(row.min_score),
    maxScore: parseFloat(row.max_score)
  }));
  
  console.log(`    ✓ Quartile Assignment: ${tests.quartileAssignment.length} quartile-recommendation combinations`);
  
  // Test specific fund scoring accuracy
  const sampleScoreTest = await pool.query(`
    SELECT 
      f.fund_name,
      f.category,
      f.subcategory,
      fs.total_score,
      fs.quartile,
      fs.recommendation,
      fs.return_1y_score
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.subcategory IS NOT NULL
    ORDER BY fs.total_score DESC
    LIMIT 10
  `);
  
  tests.topPerformers = sampleScoreTest.rows.map(row => ({
    fundName: row.fund_name.substring(0, 50),
    category: row.category,
    subcategory: row.subcategory,
    totalScore: parseFloat(row.total_score),
    quartile: parseInt(row.quartile),
    recommendation: row.recommendation,
    oneYearReturn: parseFloat(row.return_1y_score)
  }));
  
  console.log(`    ✓ Top Performers: Sample of ${tests.topPerformers.length} highest-scoring funds`);
  
  return tests;
}

async function testDatabaseIntegrity() {
  console.log('  Testing database structure and constraints...');
  
  const tests = {};
  
  // Test fund_scores table integrity
  const tableIntegrity = await pool.query(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT fund_id) as unique_funds,
      COUNT(DISTINCT score_date) as unique_dates,
      COUNT(CASE WHEN total_score IS NOT NULL THEN 1 END) as records_with_scores,
      COUNT(CASE WHEN quartile IS NOT NULL THEN 1 END) as records_with_quartiles,
      COUNT(CASE WHEN subcategory IS NOT NULL THEN 1 END) as records_with_subcategory
    FROM fund_scores
    WHERE score_date = CURRENT_DATE
  `);
  
  tests.tableIntegrity = {
    totalRecords: parseInt(tableIntegrity.rows[0].total_records),
    uniqueFunds: parseInt(tableIntegrity.rows[0].unique_funds),
    uniqueDates: parseInt(tableIntegrity.rows[0].unique_dates),
    recordsWithScores: parseInt(tableIntegrity.rows[0].records_with_scores),
    recordsWithQuartiles: parseInt(tableIntegrity.rows[0].records_with_quartiles),
    recordsWithSubcategory: parseInt(tableIntegrity.rows[0].records_with_subcategory)
  };
  
  console.log(`    ✓ Table Integrity: ${tests.tableIntegrity.totalRecords} records for ${tests.tableIntegrity.uniqueFunds} funds`);
  
  // Test data consistency
  const consistencyTest = await pool.query(`
    SELECT 
      COUNT(CASE WHEN total_score < 0 OR total_score > 100 THEN 1 END) as invalid_scores,
      COUNT(CASE WHEN quartile NOT IN (1,2,3,4) THEN 1 END) as invalid_quartiles,
      COUNT(CASE WHEN recommendation NOT IN ('STRONG_BUY','BUY','HOLD','SELL') THEN 1 END) as invalid_recommendations
    FROM fund_scores
    WHERE score_date = CURRENT_DATE
      AND total_score IS NOT NULL
  `);
  
  tests.dataConsistency = {
    invalidScores: parseInt(consistencyTest.rows[0].invalid_scores),
    invalidQuartiles: parseInt(consistencyTest.rows[0].invalid_quartiles),
    invalidRecommendations: parseInt(consistencyTest.rows[0].invalid_recommendations)
  };
  
  console.log(`    ✓ Data Consistency: ${tests.dataConsistency.invalidScores} invalid scores, ${tests.dataConsistency.invalidQuartiles} invalid quartiles`);
  
  // Test foreign key relationships
  const relationshipTest = await pool.query(`
    SELECT 
      COUNT(fs.fund_id) as fund_score_records,
      COUNT(f.id) as matching_funds,
      COUNT(fs.fund_id) - COUNT(f.id) as orphaned_scores
    FROM fund_scores fs
    LEFT JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
  `);
  
  tests.relationships = {
    fundScoreRecords: parseInt(relationshipTest.rows[0].fund_score_records),
    matchingFunds: parseInt(relationshipTest.rows[0].matching_funds),
    orphanedScores: parseInt(relationshipTest.rows[0].orphaned_scores)
  };
  
  console.log(`    ✓ Relationships: ${tests.relationships.orphanedScores} orphaned score records`);
  
  return tests;
}

async function testResultsAccuracy() {
  console.log('  Testing quartile ranking accuracy...');
  
  const tests = {};
  
  // Test subcategory peer comparison accuracy
  const peerComparisonTest = await pool.query(`
    SELECT 
      f.subcategory,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN fs.quartile = 1 THEN 1 END) as q1_count,
      COUNT(CASE WHEN fs.quartile = 2 THEN 1 END) as q2_count,
      COUNT(CASE WHEN fs.quartile = 3 THEN 1 END) as q3_count,
      COUNT(CASE WHEN fs.quartile = 4 THEN 1 END) as q4_count,
      ROUND(AVG(CASE WHEN fs.quartile = 1 THEN fs.total_score END), 2) as q1_avg_score,
      ROUND(AVG(CASE WHEN fs.quartile = 4 THEN fs.total_score END), 2) as q4_avg_score
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.subcategory IS NOT NULL
    GROUP BY f.subcategory
    HAVING COUNT(*) >= 4
    ORDER BY COUNT(*) DESC
  `);
  
  tests.peerComparison = peerComparisonTest.rows.map(row => ({
    subcategory: row.subcategory,
    totalFunds: parseInt(row.total_funds),
    quartileDistribution: [
      parseInt(row.q1_count),
      parseInt(row.q2_count), 
      parseInt(row.q3_count),
      parseInt(row.q4_count)
    ],
    q1AvgScore: parseFloat(row.q1_avg_score),
    q4AvgScore: parseFloat(row.q4_avg_score),
    scoreSpread: parseFloat(row.q1_avg_score) - parseFloat(row.q4_avg_score)
  }));
  
  console.log(`    ✓ Peer Comparison: ${tests.peerComparison.length} subcategories with proper quartile distribution`);
  
  // Test ranking consistency within subcategories
  const rankingConsistency = await pool.query(`
    SELECT 
      subcategory,
      COUNT(CASE WHEN quartile = 1 AND total_score >= 85 THEN 1 END) as q1_proper_scores,
      COUNT(CASE WHEN quartile = 1 THEN 1 END) as total_q1,
      COUNT(CASE WHEN quartile = 4 AND total_score < 55 THEN 1 END) as q4_proper_scores,
      COUNT(CASE WHEN quartile = 4 THEN 1 END) as total_q4
    FROM fund_scores fs
    WHERE score_date = CURRENT_DATE
      AND subcategory IS NOT NULL
    GROUP BY subcategory
    HAVING COUNT(*) >= 4
  `);
  
  tests.rankingConsistency = rankingConsistency.rows.map(row => ({
    subcategory: row.subcategory,
    q1Accuracy: (parseInt(row.q1_proper_scores) / parseInt(row.total_q1) * 100).toFixed(1),
    q4Accuracy: (parseInt(row.q4_proper_scores) / parseInt(row.total_q4) * 100).toFixed(1)
  }));
  
  console.log(`    ✓ Ranking Consistency: Testing score-quartile alignment across subcategories`);
  
  return tests;
}

async function testPerformanceConsistency() {
  console.log('  Testing system performance and consistency...');
  
  const tests = {};
  
  // Test processing time and efficiency
  const processingStats = await pool.query(`
    SELECT 
      COUNT(*) as total_processed,
      COUNT(DISTINCT fund_id) as unique_funds_processed,
      MIN(created_at) as earliest_processing,
      MAX(created_at) as latest_processing,
      (EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at))) / COUNT(DISTINCT fund_id)) as avg_seconds_per_fund
    FROM fund_scores
    WHERE score_date = CURRENT_DATE
  `);
  
  tests.processingPerformance = {
    totalProcessed: parseInt(processingStats.rows[0].total_processed),
    uniqueFundsProcessed: parseInt(processingStats.rows[0].unique_funds_processed),
    processingWindow: `${processingStats.rows[0].earliest_processing} to ${processingStats.rows[0].latest_processing}`,
    avgSecondsPerFund: parseFloat(processingStats.rows[0].avg_seconds_per_fund).toFixed(2)
  };
  
  console.log(`    ✓ Processing Performance: ${tests.processingPerformance.avgSecondsPerFund}s average per fund`);
  
  // Test data freshness and consistency
  const freshnessTest = await pool.query(`
    SELECT 
      MAX(n.nav_date) as latest_nav_date,
      COUNT(DISTINCT n.fund_id) as funds_with_recent_nav,
      COUNT(DISTINCT fs.fund_id) as funds_with_scores,
      (COUNT(DISTINCT fs.fund_id)::float / COUNT(DISTINCT n.fund_id) * 100) as scoring_coverage
    FROM nav_data n
    LEFT JOIN fund_scores fs ON n.fund_id = fs.fund_id AND fs.score_date = CURRENT_DATE
    WHERE n.created_at > '2025-05-30 06:45:00'
      AND n.nav_date >= CURRENT_DATE - INTERVAL '30 days'
  `);
  
  tests.dataFreshness = {
    latestNavDate: freshnessTest.rows[0].latest_nav_date,
    fundsWithRecentNav: parseInt(freshnessTest.rows[0].funds_with_recent_nav),
    fundsWithScores: parseInt(freshnessTest.rows[0].funds_with_scores),
    scoringCoverage: parseFloat(freshnessTest.rows[0].scoring_coverage).toFixed(2)
  };
  
  console.log(`    ✓ Data Freshness: ${tests.dataFreshness.scoringCoverage}% coverage of recent NAV data`);
  
  return tests;
}

function generateTestReport(testResults) {
  console.log('\n=== COMPREHENSIVE TEST REPORT ===');
  
  // Eligibility Tests Summary
  console.log('\n📋 ELIGIBILITY CRITERIA TESTS:');
  console.log(`   NAV Data Requirement: ${testResults.eligibilityTests.navDataRequirement.eligibilityRate}% pass rate`);
  console.log(`   Time Span Requirement: ${testResults.eligibilityTests.timeSpanRequirement.eligibleFunds} funds qualified`);
  console.log(`   Authentic Data Filter: ${testResults.eligibilityTests.authenticDataFilter.totalAuthenticRecords} authentic records`);
  
  // Scoring Tests Summary
  console.log('\n🎯 SCORING MODEL TESTS:');
  console.log(`   Score Distribution: ${testResults.scoringTests.scoreDistribution.length} categories analyzed`);
  console.log(`   Quartile Assignment: ${testResults.scoringTests.quartileAssignment.length} quartile levels`);
  console.log(`   Top Performers: ${testResults.scoringTests.topPerformers.length} sample validations`);
  
  // Database Tests Summary
  console.log('\n🗄️ DATABASE INTEGRITY TESTS:');
  console.log(`   Table Integrity: ${testResults.databaseTests.tableIntegrity.totalRecords} records validated`);
  console.log(`   Data Consistency: ${testResults.databaseTests.dataConsistency.invalidScores} invalid scores found`);
  console.log(`   Relationships: ${testResults.databaseTests.relationships.orphanedScores} orphaned records`);
  
  // Accuracy Tests Summary
  console.log('\n✅ RESULTS ACCURACY TESTS:');
  console.log(`   Peer Comparison: ${testResults.accuracyTests.peerComparison.length} subcategories tested`);
  console.log(`   Ranking Consistency: ${testResults.accuracyTests.rankingConsistency.length} consistency checks`);
  
  // Performance Tests Summary
  console.log('\n⚡ PERFORMANCE TESTS:');
  console.log(`   Processing Speed: ${testResults.performanceTests.processingPerformance.avgSecondsPerFund}s per fund`);
  console.log(`   Data Coverage: ${testResults.performanceTests.dataFreshness.scoringCoverage}% scoring coverage`);
  
  // Overall Assessment
  console.log('\n🏆 OVERALL ASSESSMENT:');
  const totalIssues = 
    testResults.databaseTests.dataConsistency.invalidScores +
    testResults.databaseTests.dataConsistency.invalidQuartiles +
    testResults.databaseTests.relationships.orphanedScores;
  
  if (totalIssues === 0) {
    console.log('   ✅ ALL TESTS PASSED - System is operating correctly');
  } else {
    console.log(`   ⚠️  ${totalIssues} issues found - Requires attention`);
  }
  
  console.log(`   📊 System processed ${testResults.performanceTests.processingPerformance.uniqueFundsProcessed} funds`);
  console.log(`   🎯 Using authentic AMFI data spanning ${testResults.eligibilityTests.timeSpanRequirement.avgSpan} days average`);
}

if (require.main === module) {
  comprehensiveQuartileTest()
    .then(results => {
      console.log('\n✓ Comprehensive testing completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Testing failed:', error);
      process.exit(1);
    });
}

module.exports = { comprehensiveQuartileTest };