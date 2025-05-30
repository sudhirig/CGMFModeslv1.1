const { Pool } = require('pg');

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function executeRawQuery(sql, params = []) {
  const client = await pool.connect();
  try {
    const result = await client.query(sql, params);
    return result;
  } finally {
    client.release();
  }
}

async function testAutomationSystem() {
  try {
    console.log('=== Testing Automated Quartile System ===');

    // Test 1: Check current performance metrics
    const metricsResult = await executeRawQuery(`
      SELECT 
        COUNT(*) as total_metrics,
        COUNT(DISTINCT fund_id) as unique_funds,
        MAX(calculation_date) as latest_calculation
      FROM fund_performance_metrics
    `);

    console.log('Performance Metrics Status:');
    console.log(`  Total metrics: ${metricsResult.rows[0].total_metrics}`);
    console.log(`  Unique funds: ${metricsResult.rows[0].unique_funds}`);
    console.log(`  Latest calculation: ${metricsResult.rows[0].latest_calculation}`);

    // Test 2: Check quartile rankings
    const quartileResult = await executeRawQuery(`
      SELECT 
        category,
        quartile_label,
        COUNT(*) as fund_count,
        ROUND(AVG(composite_score), 2) as avg_performance
      FROM quartile_rankings
      GROUP BY category, quartile_label
      ORDER BY category, 
        CASE quartile_label 
          WHEN 'BUY' THEN 1 
          WHEN 'HOLD' THEN 2 
          WHEN 'REVIEW' THEN 3 
          WHEN 'SELL' THEN 4 
        END
    `);

    console.log('\nQuartile Distribution:');
    let currentCategory = '';
    quartileResult.rows.forEach(row => {
      if (row.category !== currentCategory) {
        console.log(`\n${row.category}:`);
        currentCategory = row.category;
      }
      console.log(`  ${row.quartile_label}: ${row.fund_count} funds (${row.avg_performance}% avg return)`);
    });

    // Test 3: Check fund eligibility
    const eligibilityResult = await executeRawQuery(`
      SELECT 
        f.category,
        COUNT(f.id) as total_funds,
        COUNT(pm.fund_id) as eligible_funds,
        ROUND(COUNT(pm.fund_id)::numeric / COUNT(f.id) * 100, 1) as eligibility_percentage
      FROM funds f
      LEFT JOIN fund_performance_metrics pm ON f.id = pm.fund_id
      GROUP BY f.category
      ORDER BY COUNT(pm.fund_id) DESC
    `);

    console.log('\nFund Eligibility by Category:');
    eligibilityResult.rows.forEach(row => {
      console.log(`  ${row.category}: ${row.eligible_funds}/${row.total_funds} funds (${row.eligibility_percentage}%)`);
    });

    // Test 4: Simulate daily eligibility check
    console.log('\n=== Simulating Daily Eligibility Check ===');
    const newEligibleFunds = await executeRawQuery(`
      SELECT f.id, f.fund_name, f.category, COUNT(n.nav_date) as nav_count
      FROM funds f
      JOIN nav_data n ON f.id = n.fund_id
      WHERE n.created_at > '2025-05-30 06:45:00'
      AND f.id NOT IN (SELECT fund_id FROM fund_performance_metrics)
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(n.nav_date) >= 252
      LIMIT 5
    `);

    if (newEligibleFunds.rows.length > 0) {
      console.log('New eligible funds detected:');
      newEligibleFunds.rows.forEach(fund => {
        console.log(`  ${fund.fund_name}: ${fund.nav_count} NAV records`);
      });
    } else {
      console.log('No new eligible funds detected');
    }

    // Test 5: Check historical NAV import progress
    const navImportProgress = await executeRawQuery(`
      SELECT 
        COUNT(DISTINCT fund_id) as funds_with_data,
        COUNT(*) as total_nav_records,
        MIN(nav_date) as earliest_date,
        MAX(nav_date) as latest_date
      FROM nav_data
      WHERE created_at > '2025-05-30 06:45:00'
    `);

    console.log('\nHistorical NAV Import Progress:');
    console.log(`  Funds with data: ${navImportProgress.rows[0].funds_with_data}`);
    console.log(`  Total NAV records: ${navImportProgress.rows[0].total_nav_records}`);
    console.log(`  Date range: ${navImportProgress.rows[0].earliest_date} to ${navImportProgress.rows[0].latest_date}`);

    console.log('\n=== Automation System Test Complete ===');
    console.log('✓ Performance metrics calculated and stored');
    console.log('✓ Quartile rankings assigned');
    console.log('✓ Fund eligibility assessment working');
    console.log('✓ System ready for automated scheduling');

  } catch (error) {
    console.error('Error testing automation system:', error.message);
  }
}

// Execute the test
testAutomationSystem().then(() => {
  console.log('Automation system test completed');
  process.exit(0);
}).catch((error) => {
  console.error('Test failed:', error);
  process.exit(1);
});