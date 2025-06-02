/**
 * Phase 3: Simplified Advanced Financial Ratios (Authentic NAV-based)
 * Implementation without external market dependencies
 * Focuses on fund-specific metrics using only authentic NAV data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementSimplifiedAdvancedRatios() {
  try {
    console.log('Starting Phase 3: Simplified Advanced Financial Ratios...\n');
    
    let totalProcessed = 0;
    let totalSharpeAdded = 0;
    let totalBetaAdded = 0;
    let batchNumber = 0;
    
    while (batchNumber < 30) {
      batchNumber++;
      
      console.log(`Processing simplified ratios batch ${batchNumber}...`);
      
      // Get funds needing ratio calculations with sufficient data
      const eligibleFunds = await pool.query(`
        SELECT fs.fund_id, f.fund_name, f.category
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE (fs.sharpe_ratio_score IS NULL OR fs.beta_score IS NULL)
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '18 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 150
        )
        ORDER BY fs.fund_id
        LIMIT 500
      `);
      
      const funds = eligibleFunds.rows;
      if (funds.length === 0) {
        console.log('All eligible funds processed for simplified ratios');
        break;
      }
      
      console.log(`  Processing ${funds.length} funds for simplified ratios...`);
      
      // Process in chunks
      let batchSharpe = 0;
      let batchBeta = 0;
      const chunkSize = 50;
      
      for (let i = 0; i < funds.length; i += chunkSize) {
        const chunk = funds.slice(i, i + chunkSize);
        const promises = chunk.map(fund => calculateSimplifiedRatios(fund));
        const results = await Promise.allSettled(promises);
        
        results.forEach(result => {
          if (result.status === 'fulfilled' && result.value) {
            if (result.value.sharpe) batchSharpe++;
            if (result.value.beta) batchBeta++;
          }
        });
      }
      
      totalProcessed += funds.length;
      totalSharpeAdded += batchSharpe;
      totalBetaAdded += batchBeta;
      
      console.log(`  Batch ${batchNumber}: +${batchSharpe} Sharpe, +${batchBeta} Beta ratios`);
      
      // Progress report every 10 batches
      if (batchNumber % 10 === 0) {
        const coverage = await getSimplifiedRatiosCoverage();
        console.log(`\n--- Batch ${batchNumber} Progress ---`);
        console.log(`Sharpe Ratio Coverage: ${coverage.sharpe_count}/${coverage.total} (${coverage.sharpe_pct}%)`);
        console.log(`Beta Coverage: ${coverage.beta_count}/${coverage.total} (${coverage.beta_pct}%)`);
        console.log(`Session totals: +${totalSharpeAdded} Sharpe, +${totalBetaAdded} Beta\n`);
      }
    }
    
    // Final results
    const finalCoverage = await getSimplifiedRatiosCoverage();
    
    console.log(`\n=== PHASE 3 COMPLETE: SIMPLIFIED ADVANCED RATIOS ===`);
    console.log(`Sharpe Ratio Coverage: ${finalCoverage.sharpe_count}/${finalCoverage.total} (${finalCoverage.sharpe_pct}%)`);
    console.log(`Beta Coverage: ${finalCoverage.beta_count}/${finalCoverage.total} (${finalCoverage.beta_pct}%)`);
    console.log(`Total processed: ${totalProcessed} funds`);
    console.log(`Successfully added: ${totalSharpeAdded} Sharpe, ${totalBetaAdded} Beta ratios`);
    
    return {
      success: true,
      totalSharpeAdded,
      totalBetaAdded,
      finalSharpeCoverage: finalCoverage.sharpe_pct,
      finalBetaCoverage: finalCoverage.beta_pct
    };
    
  } catch (error) {
    console.error('Error in simplified advanced ratios implementation:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculateSimplifiedRatios(fund) {
  try {
    const fundId = fund.fund_id;
    const category = fund.category;
    const results = {
      sharpe: false,
      beta: false
    };
    
    // Get fund's NAV data for calculation
    const fundNavData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
      AND nav_date >= CURRENT_DATE - INTERVAL '18 months'
      ORDER BY nav_date ASC
    `, [fundId]);
    
    if (fundNavData.rows.length < 150) {
      return results;
    }
    
    const navValues = fundNavData.rows;
    
    // Calculate daily returns
    const dailyReturns = [];
    for (let i = 1; i < navValues.length; i++) {
      const prevNav = parseFloat(navValues[i-1].nav_value);
      const currentNav = parseFloat(navValues[i].nav_value);
      
      if (prevNav > 0) {
        const dailyReturn = (currentNav - prevNav) / prevNav;
        dailyReturns.push(dailyReturn);
      }
    }
    
    if (dailyReturns.length < 100) {
      return results;
    }
    
    // Calculate Sharpe Ratio (using fund's own return profile)
    const meanReturn = dailyReturns.reduce((sum, ret) => sum + ret, 0) / dailyReturns.length;
    const variance = dailyReturns.reduce((sum, ret) => sum + Math.pow(ret - meanReturn, 2), 0) / (dailyReturns.length - 1);
    const volatility = Math.sqrt(variance * 252); // Annualized
    const annualizedReturn = meanReturn * 252;
    
    if (volatility > 0) {
      const riskFreeRate = 0.06; // 6% risk-free rate assumption
      const sharpeRatio = (annualizedReturn - riskFreeRate) / volatility;
      const sharpeScore = calculateSharpeScore(sharpeRatio);
      
      await pool.query(`
        UPDATE fund_scores 
        SET sharpe_ratio_1y = $1, sharpe_ratio_score = $2, sharpe_calculation_date = CURRENT_DATE
        WHERE fund_id = $3
      `, [sharpeRatio.toFixed(3), sharpeScore, fundId]);
      
      results.sharpe = true;
    }
    
    // Calculate Beta using category peer comparison
    const categoryBeta = await calculateCategoryBeta(fundId, category, dailyReturns);
    if (categoryBeta !== null) {
      const betaScore = calculateBetaScore(categoryBeta);
      
      await pool.query(`
        UPDATE fund_scores 
        SET beta_1y = $1, beta_score = $2, beta_calculation_date = CURRENT_DATE
        WHERE fund_id = $3
      `, [categoryBeta.toFixed(3), betaScore, fundId]);
      
      results.beta = true;
    }
    
    return results;
    
  } catch (error) {
    return { sharpe: false, beta: false };
  }
}

async function calculateCategoryBeta(fundId, category, fundReturns) {
  try {
    // Get category average returns for beta calculation
    const categoryData = await pool.query(`
      WITH category_funds AS (
        SELECT DISTINCT fs.fund_id
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE f.category = $1 
        AND fs.fund_id != $2
        AND fs.return_1y_score IS NOT NULL
        LIMIT 50
      ),
      category_nav_data AS (
        SELECT nd.nav_date, AVG(nd.nav_value) as avg_nav
        FROM nav_data nd
        JOIN category_funds cf ON nd.fund_id = cf.fund_id
        WHERE nd.nav_date >= CURRENT_DATE - INTERVAL '18 months'
        GROUP BY nd.nav_date
        HAVING COUNT(nd.fund_id) >= 10
        ORDER BY nd.nav_date ASC
      )
      SELECT avg_nav, nav_date FROM category_nav_data
    `, [category, fundId]);
    
    if (categoryData.rows.length < 100) {
      // Use simplified beta calculation based on volatility comparison
      const fundVolatility = Math.sqrt(
        fundReturns.reduce((sum, ret) => {
          const mean = fundReturns.reduce((s, r) => s + r, 0) / fundReturns.length;
          return sum + Math.pow(ret - mean, 2);
        }, 0) / (fundReturns.length - 1)
      );
      
      // Estimate beta based on volatility relative to typical market volatility
      const marketVolatilityEstimate = 0.18; // 18% annual volatility estimate
      const annualizedFundVolatility = fundVolatility * Math.sqrt(252);
      return Math.min(3.0, Math.max(0.1, annualizedFundVolatility / marketVolatilityEstimate));
    }
    
    // Calculate category returns
    const categoryReturns = [];
    const categoryNavs = categoryData.rows;
    
    for (let i = 1; i < categoryNavs.length; i++) {
      const prevNav = parseFloat(categoryNavs[i-1].avg_nav);
      const currentNav = parseFloat(categoryNavs[i].avg_nav);
      
      if (prevNav > 0) {
        const dailyReturn = (currentNav - prevNav) / prevNav;
        categoryReturns.push(dailyReturn);
      }
    }
    
    if (categoryReturns.length < 50) {
      return null;
    }
    
    // Calculate beta using aligned returns
    const minLength = Math.min(fundReturns.length, categoryReturns.length);
    const alignedFundReturns = fundReturns.slice(-minLength);
    const alignedCategoryReturns = categoryReturns.slice(-minLength);
    
    const fundMean = alignedFundReturns.reduce((sum, ret) => sum + ret, 0) / alignedFundReturns.length;
    const categoryMean = alignedCategoryReturns.reduce((sum, ret) => sum + ret, 0) / alignedCategoryReturns.length;
    
    let covariance = 0;
    let categoryVariance = 0;
    
    for (let i = 0; i < alignedFundReturns.length; i++) {
      const fundDeviation = alignedFundReturns[i] - fundMean;
      const categoryDeviation = alignedCategoryReturns[i] - categoryMean;
      
      covariance += fundDeviation * categoryDeviation;
      categoryVariance += categoryDeviation * categoryDeviation;
    }
    
    covariance /= (alignedFundReturns.length - 1);
    categoryVariance /= (alignedCategoryReturns.length - 1);
    
    if (categoryVariance === 0) {
      return 1.0; // Default beta
    }
    
    const beta = covariance / categoryVariance;
    return Math.min(5.0, Math.max(0.1, beta)); // Cap beta between 0.1 and 5.0
    
  } catch (error) {
    return null;
  }
}

function calculateSharpeScore(sharpeRatio) {
  if (sharpeRatio >= 3.0) return 100;
  if (sharpeRatio >= 2.0) return 95;
  if (sharpeRatio >= 1.5) return 88;
  if (sharpeRatio >= 1.0) return 80;
  if (sharpeRatio >= 0.5) return 70;
  if (sharpeRatio >= 0.0) return 55;
  if (sharpeRatio >= -0.5) return 35;
  return 20;
}

function calculateBetaScore(beta) {
  // Beta around 1.0 is generally good for equity funds
  if (beta >= 0.8 && beta <= 1.2) return 95;
  if (beta >= 0.6 && beta <= 1.5) return 85;
  if (beta >= 0.4 && beta <= 1.8) return 75;
  if (beta >= 0.2 && beta <= 2.0) return 65;
  if (beta >= 0.1 && beta <= 2.5) return 50;
  return 30;
}

async function getSimplifiedRatiosCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as sharpe_count,
      COUNT(CASE WHEN beta_score IS NOT NULL THEN 1 END) as beta_count,
      ROUND(COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as sharpe_pct,
      ROUND(COUNT(CASE WHEN beta_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as beta_pct
    FROM fund_scores
  `);
  
  return result.rows[0];
}

implementSimplifiedAdvancedRatios();