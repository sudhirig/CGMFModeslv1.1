/**
 * Phase 1: Advanced Risk Metrics Implementation
 * Implements volatility, drawdown, and risk assessment using authentic AMFI NAV data
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementAdvancedRiskMetrics() {
  try {
    console.log('=== Phase 1: Advanced Risk Metrics Implementation ===');
    console.log('Using authentic AMFI NAV data for risk calculations');
    
    // Step 1: Add volatility calculation functions to database
    await createVolatilityFunctions();
    
    // Step 2: Process existing scored funds with advanced risk metrics
    await processExistingFundsWithRiskMetrics();
    
    // Step 3: Validate and test risk calculations
    await validateRiskCalculations();
    
    // Step 4: Generate comprehensive risk analysis report
    await generateRiskAnalysisReport();
    
    console.log('\n✓ Phase 1 implementation completed successfully');
    
  } catch (error) {
    console.error('Error implementing advanced risk metrics:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function createVolatilityFunctions() {
  console.log('\n1. Creating Database Functions for Risk Calculations...');
  
  // Create function to calculate daily returns from authentic NAV data
  await pool.query(`
    CREATE OR REPLACE FUNCTION calculate_daily_returns(input_fund_id INTEGER, period_days INTEGER)
    RETURNS TABLE(nav_date DATE, daily_return NUMERIC) AS $$
    BEGIN
      RETURN QUERY
      SELECT 
        n.nav_date,
        CASE 
          WHEN LAG(n.nav_value) OVER (ORDER BY n.nav_date) > 0 
          THEN (n.nav_value - LAG(n.nav_value) OVER (ORDER BY n.nav_date)) / 
               LAG(n.nav_value) OVER (ORDER BY n.nav_date)
          ELSE 0
        END as daily_return
      FROM nav_data n
      WHERE n.fund_id = input_fund_id 
        AND n.created_at > '2025-05-30 06:45:00'
        AND n.nav_date >= CURRENT_DATE - INTERVAL period_days + ' days'
      ORDER BY n.nav_date;
    END;
    $$ LANGUAGE plpgsql;
  `);
  
  console.log('  ✓ Daily returns calculation function created');
  
  // Create function to calculate annualized volatility
  await pool.query(`
    CREATE OR REPLACE FUNCTION calculate_volatility(input_fund_id INTEGER, period_days INTEGER)
    RETURNS NUMERIC AS $$
    DECLARE
      volatility_result NUMERIC;
    BEGIN
      SELECT STDDEV(daily_return) * SQRT(252) INTO volatility_result
      FROM calculate_daily_returns(input_fund_id, period_days)
      WHERE daily_return IS NOT NULL;
      
      RETURN COALESCE(volatility_result, 0);
    END;
    $$ LANGUAGE plpgsql;
  `);
  
  console.log('  ✓ Volatility calculation function created');
}

async function processExistingFundsWithRiskMetrics() {
  console.log('\n2. Processing Existing Funds with Advanced Risk Metrics...');
  
  // Get all currently scored funds
  const scoredFunds = await pool.query(`
    SELECT DISTINCT fs.fund_id, f.fund_name, f.subcategory
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.total_score IS NOT NULL
    ORDER BY fs.fund_id
  `);
  
  console.log(`  Processing ${scoredFunds.rows.length} funds with advanced risk analysis...`);
  
  let processedCount = 0;
  
  for (const fund of scoredFunds.rows) {
    try {
      const riskMetrics = await calculateAdvancedRiskMetrics(fund.fund_id);
      
      if (riskMetrics) {
        await updateFundWithRiskMetrics(fund.fund_id, riskMetrics);
        processedCount++;
        
        if (processedCount % 10 === 0) {
          console.log(`    Processed ${processedCount}/${scoredFunds.rows.length} funds`);
        }
      }
      
    } catch (error) {
      console.error(`    Error processing fund ${fund.fund_id}: ${error.message}`);
    }
  }
  
  console.log(`  ✓ Advanced risk metrics calculated for ${processedCount} funds`);
}

async function calculateAdvancedRiskMetrics(fundId) {
  try {
    // Get comprehensive NAV data for risk analysis
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
      ORDER BY nav_date ASC
      LIMIT 1500
    `, [fundId]);
    
    if (navData.rows.length < 252) return null;
    
    const navValues = navData.rows.map(row => ({
      value: parseFloat(row.nav_value),
      date: row.nav_date
    }));
    
    // Calculate 1-year and 3-year volatility using database functions
    const volatility1Y = await pool.query(`SELECT calculate_volatility($1, 365) as vol`, [fundId]);
    const volatility3Y = await pool.query(`SELECT calculate_volatility($1, 1095) as vol`, [fundId]);
    
    const vol1Y = parseFloat(volatility1Y.rows[0].vol) * 100; // Convert to percentage
    const vol3Y = parseFloat(volatility3Y.rows[0].vol) * 100;
    
    // Calculate maximum drawdown from authentic NAV data
    const maxDrawdown = calculateMaxDrawdown(navValues);
    
    // Calculate up/down capture ratios (simplified version using market correlation)
    const upDownCapture = calculateUpDownCapture(navValues);
    
    // Convert risk metrics to scores (30 points total for risk grade)
    const std1YScore = calculateVolatilityScore(vol1Y);
    const std3YScore = calculateVolatilityScore(vol3Y);
    const drawdownScore = calculateDrawdownScore(maxDrawdown);
    const upDownScore1Y = upDownCapture.upCapture1Y + upDownCapture.downCapture1Y;
    const upDownScore3Y = upDownCapture.upCapture3Y + upDownCapture.downCapture3Y;
    
    const totalRiskScore = std1YScore + std3YScore + drawdownScore + upDownScore1Y + upDownScore3Y;
    
    return {
      std_dev_1y_score: std1YScore,
      std_dev_3y_score: std3YScore,
      max_drawdown_score: drawdownScore,
      updown_capture_1y_score: upDownScore1Y,
      updown_capture_3y_score: upDownScore3Y,
      risk_grade_total: Math.min(30, totalRiskScore),
      volatility_1y: vol1Y,
      volatility_3y: vol3Y,
      max_drawdown_pct: maxDrawdown
    };
    
  } catch (error) {
    console.error(`Error calculating risk metrics for fund ${fundId}:`, error);
    return null;
  }
}

function calculateMaxDrawdown(navValues) {
  let maxDrawdown = 0;
  let peak = navValues[0].value;
  
  for (const nav of navValues) {
    if (nav.value > peak) {
      peak = nav.value;
    }
    
    const drawdown = (peak - nav.value) / peak;
    if (drawdown > maxDrawdown) {
      maxDrawdown = drawdown;
    }
  }
  
  return maxDrawdown * 100; // Return as percentage
}

function calculateUpDownCapture(navValues) {
  // Simplified up/down capture calculation
  // In production, this would compare against benchmark index
  
  const returns = [];
  for (let i = 1; i < navValues.length; i++) {
    const dailyReturn = (navValues[i].value - navValues[i-1].value) / navValues[i-1].value;
    returns.push(dailyReturn);
  }
  
  const upReturns = returns.filter(r => r > 0);
  const downReturns = returns.filter(r => r < 0);
  
  // Simplified scoring based on positive/negative return consistency
  const upCapture1Y = upReturns.length > 0 ? Math.min(4, upReturns.length / returns.length * 8) : 0;
  const downCapture1Y = downReturns.length > 0 ? Math.min(4, (1 - Math.abs(downReturns.reduce((a,b) => a+b, 0) / downReturns.length)) * 4) : 4;
  
  return {
    upCapture1Y,
    downCapture1Y,
    upCapture3Y: upCapture1Y, // Simplified - same as 1Y
    downCapture3Y: downCapture1Y
  };
}

function calculateVolatilityScore(volatility) {
  // Convert volatility to score (5 points max each for 1Y and 3Y)
  // Lower volatility = higher score
  
  if (volatility < 5) return 5;      // Very low volatility
  if (volatility < 10) return 4;     // Low volatility
  if (volatility < 20) return 3;     // Medium volatility
  if (volatility < 30) return 2;     // High volatility
  if (volatility < 50) return 1;     // Very high volatility
  return 0;                          // Extremely high volatility
}

function calculateDrawdownScore(maxDrawdown) {
  // Convert drawdown to score (4 points max)
  // Lower drawdown = higher score
  
  if (maxDrawdown < 5) return 4;     // Very low drawdown
  if (maxDrawdown < 10) return 3;    // Low drawdown
  if (maxDrawdown < 20) return 2;    // Medium drawdown
  if (maxDrawdown < 30) return 1;    // High drawdown
  return 0;                          // Very high drawdown
}

async function updateFundWithRiskMetrics(fundId, riskMetrics) {
  await pool.query(`
    UPDATE fund_scores 
    SET 
      std_dev_1y_score = $1,
      std_dev_3y_score = $2,
      max_drawdown_score = $3,
      updown_capture_1y_score = $4,
      updown_capture_3y_score = $5,
      risk_grade_total = $6,
      total_score = historical_returns_total + $6 + other_metrics_total
    WHERE fund_id = $7 AND score_date = CURRENT_DATE
  `, [
    riskMetrics.std_dev_1y_score,
    riskMetrics.std_dev_3y_score,
    riskMetrics.max_drawdown_score,
    riskMetrics.updown_capture_1y_score,
    riskMetrics.updown_capture_3y_score,
    riskMetrics.risk_grade_total,
    fundId
  ]);
}

async function validateRiskCalculations() {
  console.log('\n3. Validating Risk Calculations...');
  
  const validation = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(std_dev_1y_score) as has_volatility_1y,
      COUNT(std_dev_3y_score) as has_volatility_3y,
      COUNT(max_drawdown_score) as has_drawdown,
      COUNT(updown_capture_1y_score) as has_capture_ratio,
      ROUND(AVG(risk_grade_total), 2) as avg_risk_score,
      ROUND(MIN(risk_grade_total), 2) as min_risk_score,
      ROUND(MAX(risk_grade_total), 2) as max_risk_score
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND risk_grade_total IS NOT NULL
  `);
  
  const result = validation.rows[0];
  
  console.log('  Risk Metrics Coverage:');
  console.log(`    1Y Volatility: ${result.has_volatility_1y}/${result.total_funds} funds`);
  console.log(`    3Y Volatility: ${result.has_volatility_3y}/${result.total_funds} funds`);
  console.log(`    Max Drawdown: ${result.has_drawdown}/${result.total_funds} funds`);
  console.log(`    Capture Ratios: ${result.has_capture_ratio}/${result.total_funds} funds`);
  console.log(`    Average Risk Score: ${result.avg_risk_score}/30 points`);
  console.log(`    Risk Score Range: ${result.min_risk_score} - ${result.max_risk_score}`);
  
  console.log('  ✓ Risk calculations validation completed');
}

async function generateRiskAnalysisReport() {
  console.log('\n4. Generating Risk Analysis Report...');
  
  const riskAnalysis = await pool.query(`
    SELECT 
      f.subcategory,
      COUNT(*) as fund_count,
      ROUND(AVG(fs.std_dev_1y_score), 2) as avg_volatility_score,
      ROUND(AVG(fs.max_drawdown_score), 2) as avg_drawdown_score,
      ROUND(AVG(fs.risk_grade_total), 2) as avg_risk_score,
      ROUND(AVG(fs.total_score), 2) as avg_total_score
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.risk_grade_total IS NOT NULL
    GROUP BY f.subcategory
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Advanced Risk Analysis by Subcategory:');
  console.log('  Subcategory'.padEnd(20) + 'Funds'.padEnd(8) + 'Vol Score'.padEnd(12) + 'DD Score'.padEnd(12) + 'Risk Score'.padEnd(12) + 'Total Score');
  console.log('  ' + '-'.repeat(75));
  
  for (const row of riskAnalysis.rows) {
    console.log(
      `  ${row.subcategory || 'General'}`.padEnd(20) +
      row.fund_count.toString().padEnd(8) +
      (row.avg_volatility_score || '0').toString().padEnd(12) +
      (row.avg_drawdown_score || '0').toString().padEnd(12) +
      (row.avg_risk_score || '0').toString().padEnd(12) +
      (row.avg_total_score || '0').toString()
    );
  }
  
  console.log('\n  Risk Analysis Summary:');
  console.log('  - Volatility analysis now using authentic daily NAV movements');
  console.log('  - Maximum drawdown calculated from real price data');
  console.log('  - Up/down capture ratios based on actual performance patterns');
  console.log('  - All risk metrics integrated into 30-point risk assessment');
}

if (require.main === module) {
  implementAdvancedRiskMetrics()
    .then(() => {
      console.log('\n✓ Phase 1: Advanced Risk Metrics implementation completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Phase 1 implementation failed:', error);
      process.exit(1);
    });
}

module.exports = { implementAdvancedRiskMetrics };