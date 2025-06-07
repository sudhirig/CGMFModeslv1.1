/**
 * Deep Dive Null Value Analysis
 * Comprehensive investigation of calculation logic and data flow issues
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function deepDiveNullAnalysis() {
  try {
    console.log('=== Deep Dive Null Value Analysis ===');
    console.log('Comprehensive investigation of calculation logic and data flow');
    
    // Step 1: Examine database structure and constraints
    await examineDatabase();
    
    // Step 2: Analyze specific funds with null returns
    await analyzeNullReturnsFunds();
    
    // Step 3: Test calculation logic step-by-step
    await testCalculationStepByStep();
    
    // Step 4: Check data flow and insertion process
    await checkDataFlowProcess();
    
    // Step 5: Implement targeted fixes
    await implementTargetedFixes();
    
    console.log('\n✓ Deep dive analysis completed');
    
  } catch (error) {
    console.error('Deep dive analysis error:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function examineDatabase() {
  console.log('\n1. Examining Database Structure...');
  
  // Check fund_scores table constraints and defaults
  const tableInfo = await pool.query(`
    SELECT column_name, data_type, is_nullable, column_default
    FROM information_schema.columns 
    WHERE table_name = 'fund_scores' 
      AND column_name IN (
        'historical_returns_total', 'risk_grade_total', 'fundamentals_total',
        'return_3m_score', 'return_6m_score', 'return_1y_score'
      )
    ORDER BY column_name
  `);
  
  console.log('  Fund Scores Table Structure:');
  console.log('  Column'.padEnd(25) + 'Type'.padEnd(15) + 'Nullable'.padEnd(12) + 'Default');
  console.log('  ' + '-'.repeat(70));
  
  for (const column of tableInfo.rows) {
    console.log(
      `  ${column.column_name}`.padEnd(25) +
      column.data_type.padEnd(15) +
      column.is_nullable.padEnd(12) +
      (column.column_default || 'NULL')
    );
  }
  
  // Check for any database constraints that might affect inserts/updates
  const constraints = await pool.query(`
    SELECT constraint_name, constraint_type
    FROM information_schema.table_constraints
    WHERE table_name = 'fund_scores'
  `);
  
  console.log('\n  Table Constraints:');
  for (const constraint of constraints.rows) {
    console.log(`    ${constraint.constraint_name}: ${constraint.constraint_type}`);
  }
}

async function analyzeNullReturnsFunds() {
  console.log('\n2. Analyzing Funds with Null Returns...');
  
  // Get detailed information about funds with null returns
  const nullReturnsFunds = await pool.query(`
    SELECT 
      fs.fund_id, 
      f.fund_name, 
      f.subcategory,
      fs.risk_grade_total,
      fs.fundamentals_total,
      fs.total_score,
      COUNT(nd.*) as nav_count,
      MIN(nd.nav_date) as earliest_nav,
      MAX(nd.nav_date) as latest_nav,
      AVG(nd.nav_value) as avg_nav_value,
      COUNT(CASE WHEN nd.nav_value > 0 THEN 1 END) as valid_nav_count
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    LEFT JOIN nav_data nd ON f.id = nd.fund_id 
      AND nd.created_at > '2025-05-30 06:45:00'
    WHERE fs.score_date = CURRENT_DATE
      AND fs.historical_returns_total IS NULL
    GROUP BY fs.fund_id, f.fund_name, f.subcategory, 
             fs.risk_grade_total, fs.fundamentals_total, fs.total_score
    ORDER BY COUNT(nd.*) DESC
    LIMIT 10
  `);
  
  console.log('  Detailed Analysis of Null Returns Funds:');
  console.log('  Fund ID'.padEnd(10) + 'NAV Count'.padEnd(12) + 'Valid NAVs'.padEnd(12) + 'Risk Score'.padEnd(12) + 'Fund Score');
  console.log('  ' + '-'.repeat(70));
  
  for (const fund of nullReturnsFunds.rows) {
    console.log(
      `  ${fund.fund_id}`.padEnd(10) +
      fund.nav_count.toString().padEnd(12) +
      fund.valid_nav_count.toString().padEnd(12) +
      (fund.risk_grade_total || 'NULL').toString().padEnd(12) +
      (fund.fundamentals_total || 'NULL').toString()
    );
  }
  
  // Check if there are any patterns in null returns
  const patterns = await pool.query(`
    SELECT 
      f.subcategory,
      COUNT(*) as total_funds,
      COUNT(CASE WHEN fs.historical_returns_total IS NULL THEN 1 END) as null_returns,
      ROUND(
        COUNT(CASE WHEN fs.historical_returns_total IS NULL THEN 1 END) * 100.0 / COUNT(*), 
        1
      ) as null_percentage
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
    GROUP BY f.subcategory
    HAVING COUNT(CASE WHEN fs.historical_returns_total IS NULL THEN 1 END) > 0
    ORDER BY null_percentage DESC
  `);
  
  console.log('\n  Null Returns Pattern by Subcategory:');
  console.log('  Subcategory'.padEnd(25) + 'Total'.padEnd(8) + 'Null'.padEnd(8) + 'Null %');
  console.log('  ' + '-'.repeat(55));
  
  for (const pattern of patterns.rows) {
    console.log(
      `  ${pattern.subcategory || 'Unknown'}`.padEnd(25) +
      pattern.total_funds.toString().padEnd(8) +
      pattern.null_returns.toString().padEnd(8) +
      pattern.null_percentage.toString() + '%'
    );
  }
}

async function testCalculationStepByStep() {
  console.log('\n3. Testing Calculation Logic Step-by-Step...');
  
  // Pick a fund with null returns and test each step
  const testFund = await pool.query(`
    SELECT fs.fund_id, f.fund_name, f.subcategory
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.historical_returns_total IS NULL
    ORDER BY fs.fund_id
    LIMIT 1
  `);
  
  if (testFund.rows.length === 0) {
    console.log('  No funds with null returns found for testing');
    return;
  }
  
  const fund = testFund.rows[0];
  console.log(`  Testing Fund ${fund.fund_id}: ${fund.fund_name}`);
  
  // Step 1: Check NAV data availability
  const navCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(CASE WHEN nav_value > 0 THEN 1 END) as positive_values,
      MIN(nav_date) as earliest_date,
      MAX(nav_date) as latest_date,
      MIN(nav_value) as min_value,
      MAX(nav_value) as max_value,
      AVG(nav_value) as avg_value
    FROM nav_data 
    WHERE fund_id = $1 
      AND created_at > '2025-05-30 06:45:00'
  `, [fund.fund_id]);
  
  const navData = navCheck.rows[0];
  console.log(`    NAV Data Check: ${navData.total_records} records, ${navData.positive_values} positive values`);
  console.log(`    Date Range: ${navData.earliest_date?.toISOString().slice(0,10)} to ${navData.latest_date?.toISOString().slice(0,10)}`);
  console.log(`    Value Range: ${navData.min_value} to ${navData.max_value} (avg: ${navData.avg_value?.toFixed(2)})`);
  
  // Step 2: Test returns calculation manually
  if (navData.total_records >= 252) {
    const manualTest = await testManualReturnsCalculation(fund.fund_id);
    console.log(`    Manual Returns Test: ${JSON.stringify(manualTest)}`);
  } else {
    console.log(`    ⚠️  Insufficient NAV data for returns calculation`);
  }
  
  // Step 3: Check for data quality issues
  const qualityCheck = await pool.query(`
    SELECT 
      COUNT(CASE WHEN nav_value IS NULL THEN 1 END) as null_values,
      COUNT(CASE WHEN nav_value = 0 THEN 1 END) as zero_values,
      COUNT(CASE WHEN nav_value < 0 THEN 1 END) as negative_values,
      COUNT(CASE WHEN nav_date IS NULL THEN 1 END) as null_dates
    FROM nav_data 
    WHERE fund_id = $1 
      AND created_at > '2025-05-30 06:45:00'
  `, [fund.fund_id]);
  
  const quality = qualityCheck.rows[0];
  console.log(`    Data Quality: ${quality.null_values} nulls, ${quality.zero_values} zeros, ${quality.negative_values} negatives`);
}

async function testManualReturnsCalculation(fundId) {
  try {
    // Get sorted NAV data
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
        AND nav_value > 0
      ORDER BY nav_date ASC
    `, [fundId]);
    
    if (navData.rows.length < 252) {
      return { error: 'Insufficient data', count: navData.rows.length };
    }
    
    const navValues = navData.rows.map(row => ({
      value: parseFloat(row.nav_value),
      date: new Date(row.nav_date)
    }));
    
    const latest = navValues[navValues.length - 1];
    console.log(`    Latest NAV: ${latest.value} on ${latest.date.toISOString().slice(0,10)}`);
    
    // Test 1-year return calculation
    const oneYearAgo = new Date(latest.date);
    oneYearAgo.setDate(oneYearAgo.getDate() - 365);
    
    let closestNav = null;
    let minDiff = Infinity;
    
    for (const nav of navValues) {
      const diff = Math.abs(nav.date - oneYearAgo);
      if (diff < minDiff) {
        minDiff = diff;
        closestNav = nav;
      }
    }
    
    if (closestNav) {
      const totalReturn = (latest.value - closestNav.value) / closestNav.value;
      const annualizedReturn = totalReturn; // Already 1 year
      
      console.log(`    1Y Historical NAV: ${closestNav.value} on ${closestNav.date.toISOString().slice(0,10)}`);
      console.log(`    1Y Return: ${(totalReturn * 100).toFixed(2)}%`);
      
      return {
        latest_nav: latest.value,
        historical_nav: closestNav.value,
        return_1y: (totalReturn * 100).toFixed(2) + '%',
        days_diff: Math.round((latest.date - closestNav.date) / (1000 * 60 * 60 * 24)),
        status: 'success'
      };
    } else {
      return { error: 'No historical NAV found' };
    }
    
  } catch (error) {
    return { error: error.message };
  }
}

async function checkDataFlowProcess() {
  console.log('\n4. Checking Data Flow Process...');
  
  // Check when funds were last updated in fund_scores
  const updatePattern = await pool.query(`
    SELECT 
      DATE(created_at) as update_date,
      COUNT(*) as funds_updated,
      COUNT(CASE WHEN historical_returns_total IS NOT NULL THEN 1 END) as with_returns
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
    GROUP BY DATE(created_at)
    ORDER BY update_date DESC
  `);
  
  console.log('  Fund Scores Update Pattern:');
  console.log('  Date'.padEnd(12) + 'Updated'.padEnd(10) + 'With Returns');
  console.log('  ' + '-'.repeat(35));
  
  for (const update of updatePattern.rows) {
    console.log(
      `  ${update.update_date.toISOString().slice(0,10)}`.padEnd(12) +
      update.funds_updated.toString().padEnd(10) +
      update.with_returns.toString()
    );
  }
  
  // Check for any processing errors or incomplete calculations
  const processingCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN return_3m_score IS NOT NULL THEN 1 END) as has_3m,
      COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) as has_6m,
      COUNT(CASE WHEN return_1y_score IS NOT NULL THEN 1 END) as has_1y,
      COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) as has_3y,
      COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as has_5y
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND historical_returns_total IS NULL
  `);
  
  const processing = processingCheck.rows[0];
  console.log('\n  Individual Return Component Analysis:');
  console.log(`    Funds with null historical_returns_total: ${processing.total_funds}`);
  console.log(`    - Have 3M score: ${processing.has_3m}`);
  console.log(`    - Have 6M score: ${processing.has_6m}`);
  console.log(`    - Have 1Y score: ${processing.has_1y}`);
  console.log(`    - Have 3Y score: ${processing.has_3y}`);
  console.log(`    - Have 5Y score: ${processing.has_5y}`);
}

async function implementTargetedFixes() {
  console.log('\n5. Implementing Targeted Fixes...');
  
  // Fix 1: Recalculate historical_returns_total from existing component scores
  const fixFromComponents = await pool.query(`
    UPDATE fund_scores 
    SET historical_returns_total = 
      COALESCE(return_3m_score, 0) + 
      COALESCE(return_6m_score, 0) + 
      COALESCE(return_1y_score, 0) + 
      COALESCE(return_3y_score, 0) + 
      COALESCE(return_5y_score, 0)
    WHERE score_date = CURRENT_DATE
      AND historical_returns_total IS NULL
      AND (
        return_3m_score IS NOT NULL OR 
        return_6m_score IS NOT NULL OR 
        return_1y_score IS NOT NULL OR 
        return_3y_score IS NOT NULL OR 
        return_5y_score IS NOT NULL
      )
    RETURNING fund_id
  `);
  
  console.log(`  Fix 1: Recalculated historical_returns_total for ${fixFromComponents.rowCount} funds from components`);
  
  // Fix 2: Manually calculate returns for remaining funds
  const remainingFunds = await pool.query(`
    SELECT fs.fund_id, f.subcategory
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.historical_returns_total IS NULL
    ORDER BY fs.fund_id
    LIMIT 5
  `);
  
  console.log(`  Fix 2: Manually calculating returns for ${remainingFunds.rows.length} remaining funds...`);
  
  let manuallyFixed = 0;
  
  for (const fund of remainingFunds.rows) {
    try {
      const returns = await calculateManualReturns(fund.fund_id);
      if (returns && returns.total > 0) {
        await updateManualReturns(fund.fund_id, returns);
        manuallyFixed++;
        console.log(`    Fixed fund ${fund.fund_id}: ${returns.total} points`);
      }
    } catch (error) {
      console.error(`    Error fixing fund ${fund.fund_id}:`, error.message);
    }
  }
  
  console.log(`  Fix 2: Manually fixed ${manuallyFixed} funds`);
  
  // Fix 3: Update total scores to include all components
  const updateTotalScores = await pool.query(`
    UPDATE fund_scores 
    SET total_score = 
      COALESCE(historical_returns_total, 0) + 
      COALESCE(risk_grade_total, 0) + 
      COALESCE(fundamentals_total, 0)
    WHERE score_date = CURRENT_DATE
    RETURNING fund_id
  `);
  
  console.log(`  Fix 3: Updated total_score for ${updateTotalScores.rowCount} funds`);
  
  // Final validation
  const finalCheck = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(CASE WHEN historical_returns_total IS NULL THEN 1 END) as null_returns,
      COUNT(CASE WHEN historical_returns_total > 0 THEN 1 END) as positive_returns,
      ROUND(AVG(total_score), 2) as avg_total_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
  `);
  
  const final = finalCheck.rows[0];
  console.log('\n  Final Validation:');
  console.log(`    Total Funds: ${final.total_funds}`);
  console.log(`    Null Returns: ${final.null_returns}`);
  console.log(`    Positive Returns: ${final.positive_returns}`);
  console.log(`    Average Total Score: ${final.avg_total_score}`);
  
  const successRate = ((final.total_funds - final.null_returns) / final.total_funds * 100).toFixed(1);
  console.log(`    Success Rate: ${successRate}%`);
}

async function calculateManualReturns(fundId) {
  try {
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
        AND nav_value > 0
      ORDER BY nav_date ASC
    `, [fundId]);
    
    if (navData.rows.length < 100) return null; // Minimum data requirement
    
    const navValues = navData.rows.map(row => ({
      value: parseFloat(row.nav_value),
      date: new Date(row.nav_date)
    }));
    
    const latest = navValues[navValues.length - 1];
    let totalScore = 0;
    
    // Calculate available periods
    const periods = [
      { name: '3m', days: 90, score: 0 },
      { name: '6m', days: 180, score: 0 },
      { name: '1y', days: 365, score: 0 }
    ];
    
    for (const period of periods) {
      const targetDate = new Date(latest.date);
      targetDate.setDate(targetDate.getDate() - period.days);
      
      let closestNav = null;
      let minDiff = Infinity;
      
      for (const nav of navValues) {
        const diff = Math.abs(nav.date - targetDate);
        if (diff < minDiff) {
          minDiff = diff;
          closestNav = nav;
        }
      }
      
      if (closestNav && closestNav.value > 0) {
        const totalReturn = (latest.value - closestNav.value) / closestNav.value;
        if (isFinite(totalReturn)) {
          const annualizedReturn = Math.pow(1 + totalReturn, 365 / period.days) - 1;
          period.score = calculateReturnScore(annualizedReturn * 100);
          totalScore += period.score;
        }
      }
    }
    
    return {
      return_3m_score: periods[0].score,
      return_6m_score: periods[1].score,
      return_1y_score: periods[2].score,
      return_3y_score: 0,
      return_5y_score: 0,
      total: totalScore
    };
    
  } catch (error) {
    return null;
  }
}

function calculateReturnScore(annualizedReturn) {
  if (!isFinite(annualizedReturn)) return 0;
  if (annualizedReturn >= 15) return 8;
  if (annualizedReturn >= 12) return 6.4;
  if (annualizedReturn >= 8) return 4.8;
  if (annualizedReturn >= 5) return 3.2;
  if (annualizedReturn >= 0) return 1.6;
  return 0;
}

async function updateManualReturns(fundId, returns) {
  await pool.query(`
    UPDATE fund_scores SET
      return_3m_score = $1,
      return_6m_score = $2,
      return_1y_score = $3,
      return_3y_score = $4,
      return_5y_score = $5,
      historical_returns_total = $6
    WHERE fund_id = $7 AND score_date = CURRENT_DATE
  `, [
    returns.return_3m_score,
    returns.return_6m_score,
    returns.return_1y_score,
    returns.return_3y_score,
    returns.return_5y_score,
    returns.total,
    fundId
  ]);
}

if (require.main === module) {
  deepDiveNullAnalysis()
    .then(() => {
      console.log('\n✓ Deep dive null analysis completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Deep dive analysis failed:', error);
      process.exit(1);
    });
}

module.exports = { deepDiveNullAnalysis };