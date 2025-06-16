/**
 * Complete Advanced Ratios Implementation
 * Calculates authentic Sharpe Ratio, Alpha, Beta, Information Ratio for all eligible funds
 * Uses only genuine market data with zero synthetic generation
 */

import { drizzle } from 'drizzle-orm/node-postgres';
import { eq, sql, and, gte, isNull, or } from 'drizzle-orm';
import pg from 'pg';

// Database configuration
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
});

const db = drizzle(pool);

/**
 * Calculate authentic Sharpe Ratio using risk-free rate
 */
function calculateSharpeRatio(returns, riskFreeRate = 0.06) {
  if (!returns || returns.length < 30) return null;
  
  const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
  const stdDev = Math.sqrt(variance);
  
  if (stdDev === 0) return null;
  
  const annualizedReturn = meanReturn * 252; // Annualize daily returns
  const annualizedStdDev = stdDev * Math.sqrt(252);
  
  return (annualizedReturn - riskFreeRate) / annualizedStdDev;
}

/**
 * Calculate authentic Beta using market index correlation
 */
function calculateBeta(fundReturns, marketReturns) {
  if (!fundReturns || !marketReturns || fundReturns.length !== marketReturns.length || fundReturns.length < 30) {
    return null;
  }
  
  const fundMean = fundReturns.reduce((a, b) => a + b, 0) / fundReturns.length;
  const marketMean = marketReturns.reduce((a, b) => a + b, 0) / marketReturns.length;
  
  let covariance = 0;
  let marketVariance = 0;
  
  for (let i = 0; i < fundReturns.length; i++) {
    const fundDeviation = fundReturns[i] - fundMean;
    const marketDeviation = marketReturns[i] - marketMean;
    
    covariance += fundDeviation * marketDeviation;
    marketVariance += marketDeviation * marketDeviation;
  }
  
  covariance /= (fundReturns.length - 1);
  marketVariance /= (fundReturns.length - 1);
  
  return marketVariance === 0 ? null : covariance / marketVariance;
}

/**
 * Calculate authentic Alpha using CAPM
 */
function calculateAlpha(fundReturns, marketReturns, beta, riskFreeRate = 0.06) {
  if (!fundReturns || !marketReturns || beta === null) return null;
  
  const fundMeanReturn = fundReturns.reduce((a, b) => a + b, 0) / fundReturns.length;
  const marketMeanReturn = marketReturns.reduce((a, b) => a + b, 0) / marketReturns.length;
  
  const annualizedFundReturn = fundMeanReturn * 252;
  const annualizedMarketReturn = marketMeanReturn * 252;
  
  return annualizedFundReturn - (riskFreeRate + beta * (annualizedMarketReturn - riskFreeRate));
}

/**
 * Calculate Information Ratio
 */
function calculateInformationRatio(fundReturns, benchmarkReturns) {
  if (!fundReturns || !benchmarkReturns || fundReturns.length !== benchmarkReturns.length || fundReturns.length < 30) {
    return null;
  }
  
  const activeReturns = fundReturns.map((ret, i) => ret - benchmarkReturns[i]);
  const meanActiveReturn = activeReturns.reduce((a, b) => a + b, 0) / activeReturns.length;
  const trackingError = Math.sqrt(
    activeReturns.reduce((sum, ret) => sum + Math.pow(ret - meanActiveReturn, 2), 0) / (activeReturns.length - 1)
  );
  
  if (trackingError === 0) return null;
  
  return (meanActiveReturn * 252) / (trackingError * Math.sqrt(252));
}

/**
 * Get market benchmark data for calculations
 */
async function getMarketBenchmarkData(startDate, endDate) {
  try {
    const result = await db.execute(sql`
      SELECT nav_date, nav_value
      FROM market_indices 
      WHERE index_name = 'NIFTY_50'
      AND nav_date BETWEEN ${startDate} AND ${endDate}
      ORDER BY nav_date ASC
    `);
    
    if (result.rows.length < 252) return null;
    
    const returns = [];
    for (let i = 1; i < result.rows.length; i++) {
      const currentValue = parseFloat(result.rows[i].nav_value);
      const previousValue = parseFloat(result.rows[i - 1].nav_value);
      if (currentValue && previousValue) {
        returns.push((currentValue - previousValue) / previousValue);
      }
    }
    
    return returns;
  } catch (error) {
    console.log(`Market benchmark data unavailable: ${error.message}`);
    return null;
  }
}

/**
 * Calculate fund daily returns from NAV data
 */
async function calculateFundReturns(fundId, startDate, endDate) {
  try {
    const result = await db.execute(sql`
      SELECT nav_date, nav_value
      FROM nav_data 
      WHERE fund_id = ${fundId}
      AND nav_date BETWEEN ${startDate} AND ${endDate}
      AND nav_value > 0
      ORDER BY nav_date ASC
    `);
    
    if (result.rows.length < 252) return null;
    
    const returns = [];
    for (let i = 1; i < result.rows.length; i++) {
      const currentValue = parseFloat(result.rows[i].nav_value);
      const previousValue = parseFloat(result.rows[i - 1].nav_value);
      if (currentValue && previousValue) {
        returns.push((currentValue - previousValue) / previousValue);
      }
    }
    
    return returns.length >= 251 ? returns : null;
  } catch (error) {
    console.log(`Error calculating returns for fund ${fundId}: ${error.message}`);
    return null;
  }
}

/**
 * Calculate volatility from returns
 */
function calculateVolatility(returns) {
  if (!returns || returns.length < 30) return null;
  
  const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (returns.length - 1);
  
  return Math.sqrt(variance) * Math.sqrt(252); // Annualized volatility
}

/**
 * Calculate maximum drawdown from NAV data
 */
function calculateMaxDrawdown(navValues) {
  if (!navValues || navValues.length < 30) return null;
  
  let maxDrawdown = 0;
  let peak = navValues[0];
  
  for (let i = 1; i < navValues.length; i++) {
    if (navValues[i] > peak) {
      peak = navValues[i];
    } else {
      const drawdown = (peak - navValues[i]) / peak;
      maxDrawdown = Math.max(maxDrawdown, drawdown);
    }
  }
  
  return maxDrawdown;
}

/**
 * Process advanced ratios for a single fund
 */
async function processAdvancedRatiosForFund(fund) {
  try {
    const endDate = new Date();
    const startDate = new Date();
    startDate.setFullYear(endDate.getFullYear() - 1); // 1 year of data
    
    console.log(`Processing advanced ratios for fund ${fund.fund_id}...`);
    
    // Get fund returns and NAV data
    const fundReturns = await calculateFundReturns(fund.fund_id, startDate, endDate);
    if (!fundReturns) {
      console.log(`Insufficient authentic data for fund ${fund.fund_id}, skipping`);
      return null;
    }
    
    // Get market benchmark returns
    const marketReturns = await getMarketBenchmarkData(startDate, endDate);
    if (!marketReturns) {
      console.log(`Market benchmark data unavailable for period, skipping fund ${fund.fund_id}`);
      return null;
    }
    
    // Align returns arrays to same length
    const minLength = Math.min(fundReturns.length, marketReturns.length);
    const alignedFundReturns = fundReturns.slice(0, minLength);
    const alignedMarketReturns = marketReturns.slice(0, minLength);
    
    // Calculate authentic advanced ratios
    const volatility = calculateVolatility(alignedFundReturns);
    const sharpeRatio = calculateSharpeRatio(alignedFundReturns);
    const beta = calculateBeta(alignedFundReturns, alignedMarketReturns);
    const alpha = calculateAlpha(alignedFundReturns, alignedMarketReturns, beta);
    const informationRatio = calculateInformationRatio(alignedFundReturns, alignedMarketReturns);
    
    // Get NAV values for max drawdown calculation
    const navResult = await db.execute(sql`
      SELECT nav_value
      FROM nav_data 
      WHERE fund_id = ${fund.fund_id}
      AND nav_date BETWEEN ${startDate} AND ${endDate}
      AND nav_value > 0
      ORDER BY nav_date ASC
    `);
    
    const navValues = navResult.rows.map(row => parseFloat(row.nav_value));
    const maxDrawdown = calculateMaxDrawdown(navValues);
    
    // Only proceed if we have valid calculations
    if (volatility === null && sharpeRatio === null && beta === null) {
      console.log(`No valid calculations possible for fund ${fund.fund_id}`);
      return null;
    }
    
    return {
      fund_id: fund.fund_id,
      volatility,
      sharpe_ratio: sharpeRatio,
      max_drawdown: maxDrawdown,
      alpha,
      beta,
      information_ratio: informationRatio,
      data_source: 'authentic_calculation',
      calculation_period: `${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`
    };
    
  } catch (error) {
    console.log(`Error processing fund ${fund.fund_id}: ${error.message}`);
    return null;
  }
}

/**
 * Update fund_performance_metrics with calculated ratios
 */
async function updateFundPerformanceMetrics(fundId, ratios) {
  try {
    await db.execute(sql`
      UPDATE fund_performance_metrics 
      SET 
        volatility = ${ratios.volatility},
        sharpe_ratio = ${ratios.sharpe_ratio},
        max_drawdown = ${ratios.max_drawdown},
        alpha = ${ratios.alpha},
        beta = ${ratios.beta},
        information_ratio = ${ratios.information_ratio},
        calculation_date = NOW()
      WHERE fund_id = ${fundId}
    `);
    
    console.log(`✓ Updated advanced ratios for fund ${fundId}`);
    return true;
  } catch (error) {
    console.log(`Error updating fund ${fundId}: ${error.message}`);
    return false;
  }
}

/**
 * Main execution function - Complete Advanced Ratios Implementation
 */
async function completeAdvancedRatiosImplementation() {
  try {
    console.log('🚀 Starting Complete Advanced Ratios Implementation');
    console.log('📊 Using only authentic market data with zero synthetic generation');
    
    // Get funds that need advanced ratios calculations
    const fundsToProcess = await db.execute(sql`
      SELECT DISTINCT fund_id, total_nav_records
      FROM fund_performance_metrics 
      WHERE total_nav_records >= 252
      AND (volatility IS NULL OR sharpe_ratio IS NULL OR alpha IS NULL OR beta IS NULL)
      ORDER BY total_nav_records DESC
      LIMIT 1000
    `);
    
    console.log(`📈 Found ${fundsToProcess.rows.length} funds requiring advanced ratios calculation`);
    
    let processedCount = 0;
    let successCount = 0;
    let batchSize = 10;
    
    // Process in batches for stability
    for (let i = 0; i < fundsToProcess.rows.length; i += batchSize) {
      const batch = fundsToProcess.rows.slice(i, i + batchSize);
      
      console.log(`\n🔄 Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(fundsToProcess.rows.length/batchSize)}`);
      
      for (const fund of batch) {
        const ratios = await processAdvancedRatiosForFund(fund);
        
        if (ratios) {
          const updated = await updateFundPerformanceMetrics(fund.fund_id, ratios);
          if (updated) successCount++;
        }
        
        processedCount++;
        
        // Progress update every 50 funds
        if (processedCount % 50 === 0) {
          console.log(`📊 Progress: ${processedCount}/${fundsToProcess.rows.length} funds processed (${successCount} successful)`);
        }
      }
      
      // Brief pause between batches to prevent database overload
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    // Final summary
    console.log('\n✅ Advanced Ratios Implementation Complete');
    console.log(`📈 Total funds processed: ${processedCount}`);
    console.log(`✓ Successful calculations: ${successCount}`);
    console.log(`⚠️ Funds with insufficient data: ${processedCount - successCount}`);
    console.log('🎯 All calculations use authentic market data only');
    
    // Verify final coverage
    const finalCoverage = await db.execute(sql`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN volatility IS NOT NULL THEN 1 END) as volatility_coverage,
        COUNT(CASE WHEN sharpe_ratio IS NOT NULL THEN 1 END) as sharpe_coverage,
        COUNT(CASE WHEN alpha IS NOT NULL THEN 1 END) as alpha_coverage,
        COUNT(CASE WHEN beta IS NOT NULL THEN 1 END) as beta_coverage,
        COUNT(CASE WHEN information_ratio IS NOT NULL THEN 1 END) as info_ratio_coverage
      FROM fund_performance_metrics
      WHERE total_nav_records >= 252
    `);
    
    const coverage = finalCoverage.rows[0];
    console.log('\n📊 Final Advanced Ratios Coverage:');
    console.log(`Volatility: ${coverage.volatility_coverage}/${coverage.total_funds} funds`);
    console.log(`Sharpe Ratio: ${coverage.sharpe_coverage}/${coverage.total_funds} funds`);
    console.log(`Alpha: ${coverage.alpha_coverage}/${coverage.total_funds} funds`);
    console.log(`Beta: ${coverage.beta_coverage}/${coverage.total_funds} funds`);
    console.log(`Information Ratio: ${coverage.info_ratio_coverage}/${coverage.total_funds} funds`);
    
  } catch (error) {
    console.error('❌ Error in advanced ratios implementation:', error);
  } finally {
    await pool.end();
  }
}

// Execute the implementation
completeAdvancedRatiosImplementation();