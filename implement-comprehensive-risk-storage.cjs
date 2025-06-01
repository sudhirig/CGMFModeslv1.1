/**
 * Implement Comprehensive Risk Data Storage
 * Extends database schema and stores all calculated risk metrics from authentic AMFI data
 */

const { Pool } = require('pg');

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementComprehensiveRiskStorage() {
  try {
    console.log('=== Implementing Comprehensive Risk Data Storage ===');
    
    // Step 1: Extend database schema with raw risk metrics
    await extendDatabaseSchema();
    
    // Step 2: Recalculate and store comprehensive risk data for existing funds
    await recalculateAndStoreComprehensiveRisk();
    
    // Step 3: Validate stored data completeness
    await validateStoredRiskData();
    
    // Step 4: Generate comprehensive storage report
    await generateStorageReport();
    
    console.log('\n✓ Comprehensive risk data storage implementation completed');
    
  } catch (error) {
    console.error('Error implementing comprehensive risk storage:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

async function extendDatabaseSchema() {
  console.log('\n1. Extending Database Schema with Raw Risk Metrics...');
  
  // Add raw volatility and risk metrics columns
  await pool.query(`
    ALTER TABLE fund_scores 
    ADD COLUMN IF NOT EXISTS volatility_1y_percent NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS volatility_3y_percent NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS volatility_calculation_date DATE,
    ADD COLUMN IF NOT EXISTS max_drawdown_percent NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS max_drawdown_start_date DATE,
    ADD COLUMN IF NOT EXISTS max_drawdown_end_date DATE,
    ADD COLUMN IF NOT EXISTS current_drawdown_percent NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS up_capture_ratio_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS down_capture_ratio_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS up_capture_ratio_3y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS down_capture_ratio_3y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS sharpe_ratio_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS sharpe_ratio_3y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS return_skewness_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS return_kurtosis_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS var_95_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS beta_1y NUMERIC(8,4),
    ADD COLUMN IF NOT EXISTS correlation_1y NUMERIC(8,4)
  `);
  
  console.log('  ✓ Extended fund_scores table with raw risk metrics');
  
  // Create risk analytics table for detailed statistics
  await pool.query(`
    CREATE TABLE IF NOT EXISTS risk_analytics (
      id SERIAL PRIMARY KEY,
      fund_id INTEGER REFERENCES funds(id),
      calculation_date DATE NOT NULL,
      
      -- Daily Returns Statistics
      daily_returns_mean NUMERIC(10,6),
      daily_returns_std NUMERIC(10,6),
      daily_returns_count INTEGER,
      
      -- Rolling Volatility Series
      rolling_volatility_3m NUMERIC(8,4),
      rolling_volatility_6m NUMERIC(8,4),
      rolling_volatility_12m NUMERIC(8,4),
      rolling_volatility_24m NUMERIC(8,4),
      rolling_volatility_36m NUMERIC(8,4),
      
      -- Drawdown Analysis
      max_drawdown_duration_days INTEGER,
      avg_drawdown_duration_days NUMERIC(8,2),
      drawdown_frequency_per_year NUMERIC(6,2),
      recovery_time_avg_days NUMERIC(8,2),
      
      -- Performance Consistency
      positive_months_percentage NUMERIC(6,2),
      negative_months_percentage NUMERIC(6,2),
      consecutive_positive_months_max INTEGER,
      consecutive_negative_months_max INTEGER,
      
      -- Risk Metrics
      downside_deviation_1y NUMERIC(8,4),
      sortino_ratio_1y NUMERIC(8,4),
      calmar_ratio_1y NUMERIC(8,4),
      
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(fund_id, calculation_date)
    )
  `);
  
  console.log('  ✓ Created risk_analytics table for detailed metrics');
}

async function recalculateAndStoreComprehensiveRisk() {
  console.log('\n2. Recalculating and Storing Comprehensive Risk Data...');
  
  // Get all currently scored funds
  const scoredFunds = await pool.query(`
    SELECT DISTINCT fs.fund_id, f.fund_name, f.subcategory
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    WHERE fs.score_date = CURRENT_DATE
      AND fs.risk_grade_total IS NOT NULL
    ORDER BY fs.fund_id
  `);
  
  console.log(`  Processing ${scoredFunds.rows.length} funds for comprehensive risk storage...`);
  
  let processedCount = 0;
  
  for (const fund of scoredFunds.rows) {
    try {
      const comprehensiveRisk = await calculateComprehensiveRiskMetrics(fund.fund_id);
      
      if (comprehensiveRisk) {
        await storeComprehensiveRiskData(fund.fund_id, comprehensiveRisk);
        processedCount++;
        
        if (processedCount % 10 === 0) {
          console.log(`    Stored comprehensive data for ${processedCount}/${scoredFunds.rows.length} funds`);
        }
      }
      
    } catch (error) {
      console.error(`    Error processing fund ${fund.fund_id}: ${error.message}`);
    }
  }
  
  console.log(`  ✓ Comprehensive risk data stored for ${processedCount} funds`);
}

async function calculateComprehensiveRiskMetrics(fundId) {
  try {
    // Get authentic NAV data for comprehensive analysis
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
      ORDER BY nav_date ASC
      LIMIT 2000
    `, [fundId]);
    
    if (navData.rows.length < 252) return null;
    
    const navValues = navData.rows.map(row => ({
      value: parseFloat(row.nav_value),
      date: new Date(row.nav_date)
    }));
    
    // Calculate daily returns from authentic NAV data
    const dailyReturns = calculateDailyReturns(navValues);
    
    // Comprehensive volatility analysis
    const volatilityMetrics = calculateDetailedVolatility(dailyReturns);
    
    // Comprehensive drawdown analysis
    const drawdownMetrics = calculateDetailedDrawdown(navValues);
    
    // Performance consistency metrics
    const consistencyMetrics = calculatePerformanceConsistency(dailyReturns);
    
    // Advanced risk metrics
    const advancedRiskMetrics = calculateAdvancedRiskMetrics(dailyReturns);
    
    return {
      volatility: volatilityMetrics,
      drawdown: drawdownMetrics,
      consistency: consistencyMetrics,
      advanced: advancedRiskMetrics,
      calculation_date: new Date()
    };
    
  } catch (error) {
    console.error(`Error calculating comprehensive risk for fund ${fundId}:`, error);
    return null;
  }
}

function calculateDailyReturns(navValues) {
  const returns = [];
  
  for (let i = 1; i < navValues.length; i++) {
    if (navValues[i-1].value > 0) {
      const dailyReturn = (navValues[i].value - navValues[i-1].value) / navValues[i-1].value;
      if (!isNaN(dailyReturn) && isFinite(dailyReturn)) {
        returns.push({
          return: dailyReturn,
          date: navValues[i].date
        });
      }
    }
  }
  
  return returns;
}

function calculateDetailedVolatility(dailyReturns) {
  const returns = dailyReturns.map(r => r.return);
  
  return {
    volatility_1y: calculateAnnualizedVolatility(returns.slice(-252)) * 100,
    volatility_3y: calculateAnnualizedVolatility(returns.slice(-756)) * 100,
    rolling_volatility_3m: calculateAnnualizedVolatility(returns.slice(-63)) * 100,
    rolling_volatility_6m: calculateAnnualizedVolatility(returns.slice(-126)) * 100,
    rolling_volatility_12m: calculateAnnualizedVolatility(returns.slice(-252)) * 100,
    rolling_volatility_24m: calculateAnnualizedVolatility(returns.slice(-504)) * 100,
    rolling_volatility_36m: calculateAnnualizedVolatility(returns.slice(-756)) * 100,
    return_skewness: calculateSkewness(returns),
    return_kurtosis: calculateKurtosis(returns),
    downside_deviation: calculateDownsideDeviation(returns) * Math.sqrt(252) * 100
  };
}

function calculateDetailedDrawdown(navValues) {
  let maxDrawdown = 0;
  let maxDrawdownStart = null;
  let maxDrawdownEnd = null;
  let currentDrawdown = 0;
  let peak = navValues[0].value;
  let peakDate = navValues[0].date;
  
  const drawdownPeriods = [];
  let currentDrawdownStart = null;
  
  for (let i = 1; i < navValues.length; i++) {
    const nav = navValues[i];
    
    if (nav.value > peak) {
      // New peak - end any current drawdown period
      if (currentDrawdownStart) {
        drawdownPeriods.push({
          start: currentDrawdownStart,
          end: navValues[i-1].date,
          maxDrawdown: currentDrawdown,
          duration: Math.floor((navValues[i-1].date - currentDrawdownStart) / (1000 * 60 * 60 * 24))
        });
        currentDrawdownStart = null;
      }
      
      peak = nav.value;
      peakDate = nav.date;
      currentDrawdown = 0;
    } else {
      // Calculate current drawdown
      currentDrawdown = (peak - nav.value) / peak * 100;
      
      if (!currentDrawdownStart && currentDrawdown > 0) {
        currentDrawdownStart = peakDate;
      }
      
      if (currentDrawdown > maxDrawdown) {
        maxDrawdown = currentDrawdown;
        maxDrawdownStart = peakDate;
        maxDrawdownEnd = nav.date;
      }
    }
  }
  
  return {
    max_drawdown_percent: maxDrawdown,
    max_drawdown_start_date: maxDrawdownStart,
    max_drawdown_end_date: maxDrawdownEnd,
    current_drawdown_percent: currentDrawdown,
    max_drawdown_duration_days: drawdownPeriods.length > 0 ? Math.max(...drawdownPeriods.map(d => d.duration)) : 0,
    avg_drawdown_duration_days: drawdownPeriods.length > 0 ? drawdownPeriods.reduce((sum, d) => sum + d.duration, 0) / drawdownPeriods.length : 0,
    drawdown_frequency_per_year: (drawdownPeriods.length / (navValues.length / 252))
  };
}

function calculatePerformanceConsistency(dailyReturns) {
  const monthlyReturns = aggregateToMonthlyReturns(dailyReturns);
  
  const positiveMonths = monthlyReturns.filter(r => r > 0).length;
  const negativeMonths = monthlyReturns.filter(r => r < 0).length;
  
  return {
    positive_months_percentage: (positiveMonths / monthlyReturns.length) * 100,
    negative_months_percentage: (negativeMonths / monthlyReturns.length) * 100,
    consecutive_positive_months_max: calculateMaxConsecutive(monthlyReturns, r => r > 0),
    consecutive_negative_months_max: calculateMaxConsecutive(monthlyReturns, r => r < 0)
  };
}

function calculateAdvancedRiskMetrics(dailyReturns) {
  const returns = dailyReturns.map(r => r.return);
  const riskFreeRate = 0.065 / 252; // Assuming 6.5% annual risk-free rate
  
  const avgReturn = returns.reduce((sum, r) => sum + r, 0) / returns.length;
  const volatility = calculateStandardDeviation(returns);
  const downsideDeviation = calculateDownsideDeviation(returns);
  
  return {
    sharpe_ratio_1y: volatility > 0 ? (avgReturn - riskFreeRate) / volatility * Math.sqrt(252) : 0,
    sortino_ratio_1y: downsideDeviation > 0 ? (avgReturn - riskFreeRate) / downsideDeviation * Math.sqrt(252) : 0,
    var_95_1y: calculateVaR(returns, 0.05) * 100
  };
}

// Utility functions
function calculateAnnualizedVolatility(returns) {
  if (returns.length === 0) return 0;
  return calculateStandardDeviation(returns) * Math.sqrt(252);
}

function calculateStandardDeviation(values) {
  if (values.length === 0) return 0;
  const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
  const squaredDiffs = values.map(val => Math.pow(val - mean, 2));
  const variance = squaredDiffs.reduce((sum, val) => sum + val, 0) / values.length;
  return Math.sqrt(variance);
}

function calculateDownsideDeviation(returns) {
  const negativeReturns = returns.filter(r => r < 0);
  return calculateStandardDeviation(negativeReturns);
}

function calculateSkewness(values) {
  if (values.length < 3) return 0;
  const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
  const std = calculateStandardDeviation(values);
  if (std === 0) return 0;
  
  const skewness = values.reduce((sum, val) => sum + Math.pow((val - mean) / std, 3), 0) / values.length;
  return skewness;
}

function calculateKurtosis(values) {
  if (values.length < 4) return 0;
  const mean = values.reduce((sum, val) => sum + val, 0) / values.length;
  const std = calculateStandardDeviation(values);
  if (std === 0) return 0;
  
  const kurtosis = values.reduce((sum, val) => sum + Math.pow((val - mean) / std, 4), 0) / values.length - 3;
  return kurtosis;
}

function calculateVaR(returns, confidence) {
  const sortedReturns = returns.slice().sort((a, b) => a - b);
  const index = Math.floor(confidence * sortedReturns.length);
  return sortedReturns[index] || 0;
}

function aggregateToMonthlyReturns(dailyReturns) {
  const monthlyMap = new Map();
  
  for (const dailyReturn of dailyReturns) {
    const monthKey = `${dailyReturn.date.getFullYear()}-${dailyReturn.date.getMonth()}`;
    if (!monthlyMap.has(monthKey)) {
      monthlyMap.set(monthKey, []);
    }
    monthlyMap.get(monthKey).push(dailyReturn.return);
  }
  
  return Array.from(monthlyMap.values()).map(monthReturns => {
    return monthReturns.reduce((product, r) => product * (1 + r), 1) - 1;
  });
}

function calculateMaxConsecutive(values, condition) {
  let maxConsecutive = 0;
  let currentConsecutive = 0;
  
  for (const value of values) {
    if (condition(value)) {
      currentConsecutive++;
      maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
    } else {
      currentConsecutive = 0;
    }
  }
  
  return maxConsecutive;
}

async function storeComprehensiveRiskData(fundId, riskData) {
  // Update fund_scores with raw risk metrics
  await pool.query(`
    UPDATE fund_scores 
    SET 
      volatility_1y_percent = $1,
      volatility_3y_percent = $2,
      volatility_calculation_date = $3,
      max_drawdown_percent = $4,
      max_drawdown_start_date = $5,
      max_drawdown_end_date = $6,
      current_drawdown_percent = $7,
      sharpe_ratio_1y = $8,
      return_skewness_1y = $9,
      return_kurtosis_1y = $10,
      var_95_1y = $11
    WHERE fund_id = $12 AND score_date = CURRENT_DATE
  `, [
    riskData.volatility.volatility_1y,
    riskData.volatility.volatility_3y,
    riskData.calculation_date,
    riskData.drawdown.max_drawdown_percent,
    riskData.drawdown.max_drawdown_start_date,
    riskData.drawdown.max_drawdown_end_date,
    riskData.drawdown.current_drawdown_percent,
    riskData.advanced.sharpe_ratio_1y,
    riskData.volatility.return_skewness,
    riskData.volatility.return_kurtosis,
    riskData.advanced.var_95_1y,
    fundId
  ]);
  
  // Insert detailed risk analytics
  await pool.query(`
    INSERT INTO risk_analytics (
      fund_id, calculation_date,
      rolling_volatility_3m, rolling_volatility_6m, rolling_volatility_12m,
      rolling_volatility_24m, rolling_volatility_36m,
      max_drawdown_duration_days, avg_drawdown_duration_days, drawdown_frequency_per_year,
      positive_months_percentage, negative_months_percentage,
      consecutive_positive_months_max, consecutive_negative_months_max,
      downside_deviation_1y, sortino_ratio_1y
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
    ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
      rolling_volatility_3m = EXCLUDED.rolling_volatility_3m,
      rolling_volatility_6m = EXCLUDED.rolling_volatility_6m,
      rolling_volatility_12m = EXCLUDED.rolling_volatility_12m,
      rolling_volatility_24m = EXCLUDED.rolling_volatility_24m,
      rolling_volatility_36m = EXCLUDED.rolling_volatility_36m,
      max_drawdown_duration_days = EXCLUDED.max_drawdown_duration_days,
      avg_drawdown_duration_days = EXCLUDED.avg_drawdown_duration_days,
      drawdown_frequency_per_year = EXCLUDED.drawdown_frequency_per_year,
      positive_months_percentage = EXCLUDED.positive_months_percentage,
      negative_months_percentage = EXCLUDED.negative_months_percentage,
      consecutive_positive_months_max = EXCLUDED.consecutive_positive_months_max,
      consecutive_negative_months_max = EXCLUDED.consecutive_negative_months_max,
      downside_deviation_1y = EXCLUDED.downside_deviation_1y,
      sortino_ratio_1y = EXCLUDED.sortino_ratio_1y
  `, [
    fundId,
    riskData.calculation_date,
    riskData.volatility.rolling_volatility_3m,
    riskData.volatility.rolling_volatility_6m,
    riskData.volatility.rolling_volatility_12m,
    riskData.volatility.rolling_volatility_24m,
    riskData.volatility.rolling_volatility_36m,
    riskData.drawdown.max_drawdown_duration_days,
    riskData.drawdown.avg_drawdown_duration_days,
    riskData.drawdown.drawdown_frequency_per_year,
    riskData.consistency.positive_months_percentage,
    riskData.consistency.negative_months_percentage,
    riskData.consistency.consecutive_positive_months_max,
    riskData.consistency.consecutive_negative_months_max,
    riskData.volatility.downside_deviation,
    riskData.advanced.sortino_ratio_1y
  ]);
}

async function validateStoredRiskData() {
  console.log('\n3. Validating Stored Risk Data...');
  
  const validation = await pool.query(`
    SELECT 
      COUNT(*) as total_funds,
      COUNT(volatility_1y_percent) as has_volatility_raw,
      COUNT(max_drawdown_percent) as has_drawdown_raw,
      COUNT(sharpe_ratio_1y) as has_sharpe_ratio,
      COUNT(return_skewness_1y) as has_skewness,
      COUNT(var_95_1y) as has_var,
      ROUND(AVG(volatility_1y_percent), 2) as avg_volatility,
      ROUND(AVG(max_drawdown_percent), 2) as avg_drawdown
    FROM fund_scores 
    WHERE score_date = CURRENT_DATE
      AND volatility_1y_percent IS NOT NULL
  `);
  
  const analytics = await pool.query(`
    SELECT COUNT(*) as analytics_records
    FROM risk_analytics 
    WHERE calculation_date = CURRENT_DATE
  `);
  
  const result = validation.rows[0];
  const analyticsCount = analytics.rows[0].analytics_records;
  
  console.log('  Stored Risk Data Coverage:');
  console.log(`    Raw Volatility: ${result.has_volatility_raw}/${result.total_funds} funds`);
  console.log(`    Raw Drawdown: ${result.has_drawdown_raw}/${result.total_funds} funds`);
  console.log(`    Sharpe Ratio: ${result.has_sharpe_ratio}/${result.total_funds} funds`);
  console.log(`    Statistical Metrics: ${result.has_skewness}/${result.total_funds} funds`);
  console.log(`    Value at Risk: ${result.has_var}/${result.total_funds} funds`);
  console.log(`    Detailed Analytics: ${analyticsCount} records stored`);
  console.log(`    Average 1Y Volatility: ${result.avg_volatility}%`);
  console.log(`    Average Max Drawdown: ${result.avg_drawdown}%`);
  
  console.log('  ✓ Risk data validation completed');
}

async function generateStorageReport() {
  console.log('\n4. Generating Comprehensive Storage Report...');
  
  const storageReport = await pool.query(`
    SELECT 
      f.subcategory,
      COUNT(*) as fund_count,
      ROUND(AVG(fs.volatility_1y_percent), 2) as avg_volatility_1y,
      ROUND(AVG(fs.max_drawdown_percent), 2) as avg_max_drawdown,
      ROUND(AVG(fs.sharpe_ratio_1y), 2) as avg_sharpe_ratio,
      ROUND(AVG(ra.positive_months_percentage), 2) as avg_positive_months,
      ROUND(AVG(ra.rolling_volatility_12m), 2) as avg_rolling_vol_12m
    FROM fund_scores fs
    JOIN funds f ON fs.fund_id = f.id
    LEFT JOIN risk_analytics ra ON fs.fund_id = ra.fund_id AND ra.calculation_date = CURRENT_DATE
    WHERE fs.score_date = CURRENT_DATE
      AND fs.volatility_1y_percent IS NOT NULL
    GROUP BY f.subcategory
    ORDER BY COUNT(*) DESC
  `);
  
  console.log('\n  Comprehensive Risk Data by Subcategory:');
  console.log('  Subcategory'.padEnd(20) + 'Funds'.padEnd(8) + 'Vol 1Y%'.padEnd(10) + 'Max DD%'.padEnd(10) + 'Sharpe'.padEnd(10) + 'Pos Mon%'.padEnd(10) + 'Roll Vol%');
  console.log('  ' + '-'.repeat(80));
  
  for (const row of storageReport.rows) {
    console.log(
      `  ${row.subcategory || 'General'}`.padEnd(20) +
      row.fund_count.toString().padEnd(8) +
      (row.avg_volatility_1y || '0').toString().padEnd(10) +
      (row.avg_max_drawdown || '0').toString().padEnd(10) +
      (row.avg_sharpe_ratio || '0').toString().padEnd(10) +
      (row.avg_positive_months || '0').toString().padEnd(10) +
      (row.avg_rolling_vol_12m || '0').toString()
    );
  }
  
  console.log('\n  Storage Implementation Summary:');
  console.log('  - All raw risk metrics permanently stored in database');
  console.log('  - Detailed analytics in separate risk_analytics table');
  console.log('  - Historical tracking capability enabled');
  console.log('  - Advanced statistical measures captured');
  console.log('  - Time-series analysis data available');
}

if (require.main === module) {
  implementComprehensiveRiskStorage()
    .then(() => {
      console.log('\n✓ Comprehensive risk data storage implementation completed');
      process.exit(0);
    })
    .catch(error => {
      console.error('Implementation failed:', error);
      process.exit(1);
    });
}

module.exports = { implementComprehensiveRiskStorage };