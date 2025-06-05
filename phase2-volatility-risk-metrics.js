/**
 * Phase 2: Risk & Volatility Metrics Implementation
 * Target: Fill 96%+ NULL values in volatility and risk measurements
 * - std_dev_1y_score (96.24% NULL)
 * - volatility_1y_percent (39.42% NULL) 
 * - max_drawdown_score (96.49% NULL)
 * - max_drawdown_percent (99.69% NULL)
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementRiskVolatilityMetrics() {
  try {
    console.log('Starting Phase 2: Risk & Volatility Metrics Implementation...\n');
    
    let totalProcessed = 0;
    let totalVolatilityAdded = 0;
    let totalDrawdownAdded = 0;
    let batchNumber = 0;
    
    while (batchNumber < 60) {
      batchNumber++;
      
      console.log(`Processing risk metrics batch ${batchNumber}...`);
      
      // Get funds needing volatility and drawdown calculations
      const eligibleFunds = await pool.query(`
        SELECT fs.fund_id
        FROM fund_scores fs
        WHERE (fs.std_dev_1y_score IS NULL OR fs.max_drawdown_score IS NULL)
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '15 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 100
        )
        ORDER BY fs.fund_id
        LIMIT 800
      `);
      
      const funds = eligibleFunds.rows;
      if (funds.length === 0) {
        console.log('All eligible funds processed for risk metrics');
        break;
      }
      
      console.log(`  Processing ${funds.length} funds for risk metrics...`);
      
      // Process in chunks
      let batchVolatility = 0;
      let batchDrawdown = 0;
      const chunkSize = 100;
      
      for (let i = 0; i < funds.length; i += chunkSize) {
        const chunk = funds.slice(i, i + chunkSize);
        const promises = chunk.map(fund => calculateRiskMetrics(fund.fund_id));
        const results = await Promise.allSettled(promises);
        
        results.forEach(result => {
          if (result.status === 'fulfilled' && result.value) {
            if (result.value.volatility) batchVolatility++;
            if (result.value.drawdown) batchDrawdown++;
          }
        });
      }
      
      totalProcessed += funds.length;
      totalVolatilityAdded += batchVolatility;
      totalDrawdownAdded += batchDrawdown;
      
      console.log(`  Batch ${batchNumber}: +${batchVolatility} volatility, +${batchDrawdown} drawdown metrics`);
      
      // Progress report every 10 batches
      if (batchNumber % 10 === 0) {
        const coverage = await getRiskMetricsCoverage();
        console.log(`\n--- Batch ${batchNumber} Progress ---`);
        console.log(`Volatility Coverage: ${coverage.volatility_count}/${coverage.total} (${coverage.volatility_pct}%)`);
        console.log(`Drawdown Coverage: ${coverage.drawdown_count}/${coverage.total} (${coverage.drawdown_pct}%)`);
        console.log(`Session totals: +${totalVolatilityAdded} volatility, +${totalDrawdownAdded} drawdown\n`);
      }
    }
    
    // Final results
    const finalCoverage = await getRiskMetricsCoverage();
    
    console.log(`\n=== PHASE 2 COMPLETE: RISK & VOLATILITY METRICS ===`);
    console.log(`Volatility Coverage: ${finalCoverage.volatility_count}/${finalCoverage.total} (${finalCoverage.volatility_pct}%)`);
    console.log(`Drawdown Coverage: ${finalCoverage.drawdown_count}/${finalCoverage.total} (${finalCoverage.drawdown_pct}%)`);
    console.log(`Total processed: ${totalProcessed} funds`);
    console.log(`Successfully added: ${totalVolatilityAdded} volatility, ${totalDrawdownAdded} drawdown metrics`);
    
    return {
      success: true,
      totalVolatilityAdded,
      totalDrawdownAdded,
      finalVolatilityCoverage: finalCoverage.volatility_pct,
      finalDrawdownCoverage: finalCoverage.drawdown_pct
    };
    
  } catch (error) {
    console.error('Error in risk & volatility metrics implementation:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculateRiskMetrics(fundId) {
  try {
    // Calculate both volatility and maximum drawdown metrics
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
      AND nav_date >= CURRENT_DATE - INTERVAL '15 months'
      ORDER BY nav_date ASC
    `, [fundId]);
    
    if (navData.rows.length < 100) {
      return { volatility: false, drawdown: false };
    }
    
    const navValues = navData.rows;
    const results = {
      volatility: false,
      drawdown: false
    };
    
    // Calculate daily returns for volatility
    const dailyReturns = [];
    for (let i = 1; i < navValues.length; i++) {
      const prevNav = parseFloat(navValues[i-1].nav_value);
      const currentNav = parseFloat(navValues[i].nav_value);
      
      if (prevNav > 0) {
        const dailyReturn = (currentNav - prevNav) / prevNav;
        dailyReturns.push(dailyReturn);
      }
    }
    
    // Calculate 1-year volatility (standard deviation)
    if (dailyReturns.length >= 200) {
      const mean = dailyReturns.reduce((sum, ret) => sum + ret, 0) / dailyReturns.length;
      const variance = dailyReturns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / (dailyReturns.length - 1);
      const volatility = Math.sqrt(variance) * Math.sqrt(252) * 100; // Annualized volatility percentage
      
      const volatilityScore = calculateVolatilityScore(volatility);
      
      await pool.query(`
        UPDATE fund_scores 
        SET std_dev_1y_score = $1, volatility_1y_percent = $2, volatility_calculation_date = CURRENT_DATE
        WHERE fund_id = $3
      `, [volatilityScore, volatility.toFixed(2), fundId]);
      
      results.volatility = true;
    }
    
    // Calculate maximum drawdown
    let maxDrawdown = 0;
    let maxDrawdownPercent = 0;
    let peak = parseFloat(navValues[0].nav_value);
    let drawdownStartDate = null;
    let drawdownEndDate = null;
    let maxDrawdownStartDate = null;
    let maxDrawdownEndDate = null;
    
    for (let i = 1; i < navValues.length; i++) {
      const currentNav = parseFloat(navValues[i].nav_value);
      const currentDate = navValues[i].nav_date;
      
      if (currentNav > peak) {
        peak = currentNav;
        drawdownStartDate = currentDate;
      } else {
        const drawdown = (peak - currentNav) / peak;
        if (drawdown > maxDrawdown) {
          maxDrawdown = drawdown;
          maxDrawdownPercent = drawdown * 100;
          maxDrawdownStartDate = drawdownStartDate;
          maxDrawdownEndDate = currentDate;
        }
      }
    }
    
    if (maxDrawdown > 0) {
      const drawdownScore = calculateDrawdownScore(maxDrawdownPercent);
      
      await pool.query(`
        UPDATE fund_scores 
        SET max_drawdown_score = $1, 
            max_drawdown_percent = $2,
            max_drawdown_start_date = $3,
            max_drawdown_end_date = $4
        WHERE fund_id = $5
      `, [drawdownScore, maxDrawdownPercent.toFixed(2), maxDrawdownStartDate, maxDrawdownEndDate, fundId]);
      
      results.drawdown = true;
    }
    
    return results;
    
  } catch (error) {
    return { volatility: false, drawdown: false };
  }
}

function calculateVolatilityScore(volatilityPercent) {
  // Lower volatility gets higher score (inverse relationship)
  if (volatilityPercent <= 5) return 100;      // Very low volatility - excellent
  if (volatilityPercent <= 8) return 95;       // Low volatility - very good
  if (volatilityPercent <= 12) return 88;      // Moderate low volatility - good
  if (volatilityPercent <= 15) return 80;      // Average volatility
  if (volatilityPercent <= 20) return 70;      // Moderate high volatility
  if (volatilityPercent <= 25) return 58;      // High volatility - concerning
  if (volatilityPercent <= 30) return 45;      // Very high volatility - risky
  if (volatilityPercent <= 40) return 30;      // Extremely high volatility
  if (volatilityPercent <= 50) return 20;      // Dangerous volatility
  return 10;                                   // Unacceptably high volatility
}

function calculateDrawdownScore(drawdownPercent) {
  // Lower drawdown gets higher score (inverse relationship)
  if (drawdownPercent <= 2) return 100;        // Minimal drawdown - excellent
  if (drawdownPercent <= 5) return 95;         // Low drawdown - very good
  if (drawdownPercent <= 10) return 88;        // Moderate drawdown - good
  if (drawdownPercent <= 15) return 80;        // Average drawdown
  if (drawdownPercent <= 20) return 70;        // Moderate high drawdown
  if (drawdownPercent <= 30) return 58;        // High drawdown - concerning
  if (drawdownPercent <= 40) return 45;        // Very high drawdown - risky
  if (drawdownPercent <= 50) return 30;        // Extreme drawdown
  if (drawdownPercent <= 60) return 20;        // Severe drawdown
  return 10;                                   // Catastrophic drawdown
}

async function getRiskMetricsCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN std_dev_1y_score IS NOT NULL THEN 1 END) as volatility_count,
      COUNT(CASE WHEN max_drawdown_score IS NOT NULL THEN 1 END) as drawdown_count,
      ROUND(COUNT(CASE WHEN std_dev_1y_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as volatility_pct,
      ROUND(COUNT(CASE WHEN max_drawdown_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as drawdown_pct
    FROM fund_scores
  `);
  
  return result.rows[0];
}

implementRiskVolatilityMetrics();