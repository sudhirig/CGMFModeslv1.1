/**
 * Phase 4: Quality & Performance Metrics Implementation
 * Target: Fill remaining NULL values in quality assessment metrics
 * - consistency_score (calculations needed)
 * - expense_ratio_score (normalize existing expense ratios)
 * - fund_size_score (calculate based on AUM data)
 * - fund_manager_score (derive from performance consistency)
 * - overall_rating (comprehensive scoring integration)
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function implementQualityPerformanceMetrics() {
  try {
    console.log('Starting Phase 4: Quality & Performance Metrics Implementation...\n');
    
    let totalProcessed = 0;
    let totalConsistencyAdded = 0;
    let totalExpenseAdded = 0;
    let totalSizeAdded = 0;
    let totalManagerAdded = 0;
    let totalRatingAdded = 0;
    let batchNumber = 0;
    
    while (batchNumber < 40) {
      batchNumber++;
      
      console.log(`Processing quality metrics batch ${batchNumber}...`);
      
      // Get funds needing quality metric calculations
      const eligibleFunds = await pool.query(`
        SELECT fs.fund_id, f.fund_name, f.category, f.expense_ratio, f.aum_crores
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE (fs.consistency_score IS NULL OR fs.overall_rating IS NULL)
        AND EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 50
        )
        ORDER BY fs.fund_id
        LIMIT 800
      `);
      
      const funds = eligibleFunds.rows;
      if (funds.length === 0) {
        console.log('All eligible funds processed for quality metrics');
        break;
      }
      
      console.log(`  Processing ${funds.length} funds for quality metrics...`);
      
      // Process in chunks
      let batchConsistency = 0;
      let batchExpense = 0;
      let batchSize = 0;
      let batchManager = 0;
      let batchRating = 0;
      const chunkSize = 100;
      
      for (let i = 0; i < funds.length; i += chunkSize) {
        const chunk = funds.slice(i, i + chunkSize);
        const promises = chunk.map(fund => calculateQualityMetrics(fund));
        const results = await Promise.allSettled(promises);
        
        results.forEach(result => {
          if (result.status === 'fulfilled' && result.value) {
            if (result.value.consistency) batchConsistency++;
            if (result.value.expense) batchExpense++;
            if (result.value.size) batchSize++;
            if (result.value.manager) batchManager++;
            if (result.value.rating) batchRating++;
          }
        });
      }
      
      totalProcessed += funds.length;
      totalConsistencyAdded += batchConsistency;
      totalExpenseAdded += batchExpense;
      totalSizeAdded += batchSize;
      totalManagerAdded += batchManager;
      totalRatingAdded += batchRating;
      
      console.log(`  Batch ${batchNumber}: +${batchConsistency} consistency, +${batchExpense} expense, +${batchSize} size, +${batchManager} manager, +${batchRating} ratings`);
      
      // Progress report every 8 batches
      if (batchNumber % 8 === 0) {
        const coverage = await getQualityMetricsCoverage();
        console.log(`\n--- Batch ${batchNumber} Progress ---`);
        console.log(`Consistency Coverage: ${coverage.consistency_count}/${coverage.total} (${coverage.consistency_pct}%)`);
        console.log(`Overall Rating Coverage: ${coverage.rating_count}/${coverage.total} (${coverage.rating_pct}%)`);
        console.log(`Session totals: +${totalConsistencyAdded} consistency, +${totalRatingAdded} ratings\n`);
      }
    }
    
    // Final results
    const finalCoverage = await getQualityMetricsCoverage();
    
    console.log(`\n=== PHASE 4 COMPLETE: QUALITY & PERFORMANCE METRICS ===`);
    console.log(`Consistency Coverage: ${finalCoverage.consistency_count}/${finalCoverage.total} (${finalCoverage.consistency_pct}%)`);
    console.log(`Overall Rating Coverage: ${finalCoverage.rating_count}/${finalCoverage.total} (${finalCoverage.rating_pct}%)`);
    console.log(`Total processed: ${totalProcessed} funds`);
    console.log(`Successfully added: ${totalConsistencyAdded} consistency, ${totalExpenseAdded} expense, ${totalSizeAdded} size, ${totalManagerAdded} manager, ${totalRatingAdded} ratings`);
    
    return {
      success: true,
      totalConsistencyAdded,
      totalExpenseAdded,
      totalSizeAdded,
      totalManagerAdded,
      totalRatingAdded,
      finalConsistencyCoverage: finalCoverage.consistency_pct,
      finalRatingCoverage: finalCoverage.rating_pct
    };
    
  } catch (error) {
    console.error('Error in quality & performance metrics implementation:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculateQualityMetrics(fund) {
  try {
    const fundId = fund.fund_id;
    const expenseRatio = parseFloat(fund.expense_ratio) || null;
    const aumCr = parseFloat(fund.aum_crores) || null;
    const results = {
      consistency: false,
      expense: false,
      size: false,
      manager: false,
      rating: false
    };
    
    // Calculate consistency score based on NAV volatility and return patterns
    const consistencyScore = await calculateConsistencyScore(fundId);
    if (consistencyScore !== null) {
      await pool.query(`
        UPDATE fund_scores 
        SET consistency_score = $1, consistency_calculation_date = CURRENT_DATE
        WHERE fund_id = $2
      `, [consistencyScore, fundId]);
      
      results.consistency = true;
    }
    
    // Calculate expense ratio score
    if (expenseRatio !== null && expenseRatio > 0) {
      const expenseScore = calculateExpenseRatioScore(expenseRatio);
      await pool.query(`
        UPDATE fund_scores 
        SET expense_ratio_score = $1
        WHERE fund_id = $2
      `, [expenseScore, fundId]);
      
      results.expense = true;
    }
    
    // Calculate fund size score based on AUM
    if (aumCr !== null && aumCr > 0) {
      const sizeScore = calculateFundSizeScore(aumCr);
      await pool.query(`
        UPDATE fund_scores 
        SET fund_size_score = $1
        WHERE fund_id = $2
      `, [sizeScore, fundId]);
      
      results.size = true;
    }
    
    // Calculate fund manager score based on performance consistency
    const managerScore = await calculateFundManagerScore(fundId);
    if (managerScore !== null) {
      await pool.query(`
        UPDATE fund_scores 
        SET fund_manager_score = $1
        WHERE fund_id = $2
      `, [managerScore, fundId]);
      
      results.manager = true;
    }
    
    // Calculate overall rating by integrating available scores
    const overallRating = await calculateOverallRating(fundId);
    if (overallRating !== null) {
      await pool.query(`
        UPDATE fund_scores 
        SET overall_rating = $1, rating_calculation_date = CURRENT_DATE
        WHERE fund_id = $2
      `, [overallRating, fundId]);
      
      results.rating = true;
    }
    
    return results;
    
  } catch (error) {
    return { consistency: false, expense: false, size: false, manager: false, rating: false };
  }
}

async function calculateConsistencyScore(fundId) {
  try {
    // Calculate consistency based on return volatility and trend stability
    const navAnalysis = await pool.query(`
      WITH nav_returns AS (
        SELECT 
          nav_date,
          nav_value,
          LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav,
          (nav_value - LAG(nav_value) OVER (ORDER BY nav_date)) / 
          NULLIF(LAG(nav_value) OVER (ORDER BY nav_date), 0) as daily_return
        FROM nav_data 
        WHERE fund_id = $1 
        AND nav_date >= CURRENT_DATE - INTERVAL '2 years'
        ORDER BY nav_date
      ),
      consistency_metrics AS (
        SELECT 
          COUNT(*) as total_days,
          AVG(daily_return) as avg_return,
          STDDEV(daily_return) as return_volatility,
          COUNT(CASE WHEN daily_return > 0 THEN 1 END) as positive_days,
          COUNT(CASE WHEN daily_return < 0 THEN 1 END) as negative_days
        FROM nav_returns 
        WHERE daily_return IS NOT NULL
      )
      SELECT 
        total_days,
        avg_return,
        return_volatility,
        positive_days,
        negative_days,
        CASE 
          WHEN total_days > 0 THEN positive_days::DECIMAL / total_days 
          ELSE 0 
        END as positive_ratio
      FROM consistency_metrics
    `, [fundId]);
    
    if (navAnalysis.rows.length === 0 || navAnalysis.rows[0].total_days < 100) {
      return null;
    }
    
    const metrics = navAnalysis.rows[0];
    const volatility = parseFloat(metrics.return_volatility) || 0;
    const positiveRatio = parseFloat(metrics.positive_ratio) || 0;
    const totalDays = parseInt(metrics.total_days) || 0;
    
    // Calculate consistency score (0-100)
    let consistencyScore = 50; // Base score
    
    // Lower volatility increases consistency
    if (volatility <= 0.01) consistencyScore += 25;
    else if (volatility <= 0.02) consistencyScore += 20;
    else if (volatility <= 0.03) consistencyScore += 15;
    else if (volatility <= 0.05) consistencyScore += 10;
    else if (volatility > 0.1) consistencyScore -= 20;
    
    // Higher positive day ratio increases consistency
    if (positiveRatio >= 0.6) consistencyScore += 15;
    else if (positiveRatio >= 0.55) consistencyScore += 10;
    else if (positiveRatio >= 0.5) consistencyScore += 5;
    else if (positiveRatio < 0.4) consistencyScore -= 15;
    
    // More data points increase reliability
    if (totalDays >= 500) consistencyScore += 10;
    else if (totalDays >= 250) consistencyScore += 5;
    
    return Math.max(10, Math.min(100, Math.round(consistencyScore)));
    
  } catch (error) {
    return null;
  }
}

function calculateExpenseRatioScore(expenseRatio) {
  // Lower expense ratio gets higher score
  if (expenseRatio <= 0.5) return 100;
  if (expenseRatio <= 1.0) return 95;
  if (expenseRatio <= 1.5) return 88;
  if (expenseRatio <= 2.0) return 80;
  if (expenseRatio <= 2.5) return 70;
  if (expenseRatio <= 3.0) return 58;
  if (expenseRatio <= 4.0) return 45;
  if (expenseRatio <= 5.0) return 30;
  return 20;
}

function calculateFundSizeScore(aumCr) {
  // Optimal fund size balance (not too small, not too large)
  if (aumCr >= 1000 && aumCr <= 10000) return 100; // Sweet spot
  if (aumCr >= 500 && aumCr <= 20000) return 95;
  if (aumCr >= 100 && aumCr <= 50000) return 88;
  if (aumCr >= 50 && aumCr <= 100000) return 80;
  if (aumCr >= 25 && aumCr <= 200000) return 70;
  if (aumCr >= 10) return 60;
  return 40; // Very small funds
}

async function calculateFundManagerScore(fundId) {
  try {
    // Calculate manager effectiveness based on fund performance vs peers
    const performanceAnalysis = await pool.query(`
      SELECT 
        return_1y_score,
        return_3y_score,
        return_5y_score,
        std_dev_1y_score,
        max_drawdown_score,
        consistency_score
      FROM fund_scores 
      WHERE fund_id = $1
    `, [fundId]);
    
    if (performanceAnalysis.rows.length === 0) {
      return null;
    }
    
    const scores = performanceAnalysis.rows[0];
    let managerScore = 0;
    let weightedScores = 0;
    let totalWeight = 0;
    
    // Weight different performance metrics
    if (scores.return_1y_score) {
      weightedScores += scores.return_1y_score * 0.3;
      totalWeight += 0.3;
    }
    
    if (scores.return_3y_score) {
      weightedScores += scores.return_3y_score * 0.25;
      totalWeight += 0.25;
    }
    
    if (scores.std_dev_1y_score) {
      weightedScores += scores.std_dev_1y_score * 0.2;
      totalWeight += 0.2;
    }
    
    if (scores.max_drawdown_score) {
      weightedScores += scores.max_drawdown_score * 0.15;
      totalWeight += 0.15;
    }
    
    if (scores.consistency_score) {
      weightedScores += scores.consistency_score * 0.1;
      totalWeight += 0.1;
    }
    
    if (totalWeight > 0) {
      managerScore = Math.round(weightedScores / totalWeight);
      return Math.max(20, Math.min(100, managerScore));
    }
    
    return null;
    
  } catch (error) {
    return null;
  }
}

async function calculateOverallRating(fundId) {
  try {
    // Comprehensive rating based on all available metrics
    const allScores = await pool.query(`
      SELECT 
        return_1y_score, return_3y_score, return_5y_score, return_6m_score, return_ytd_score,
        std_dev_1y_score, max_drawdown_score, consistency_score,
        expense_ratio_score, fund_size_score, fund_manager_score,
        sharpe_ratio_score, beta_score
      FROM fund_scores 
      WHERE fund_id = $1
    `, [fundId]);
    
    if (allScores.rows.length === 0) {
      return null;
    }
    
    const scores = allScores.rows[0];
    let totalWeightedScore = 0;
    let totalWeight = 0;
    
    // Return metrics (40% total weight)
    if (scores.return_1y_score) { totalWeightedScore += scores.return_1y_score * 0.15; totalWeight += 0.15; }
    if (scores.return_3y_score) { totalWeightedScore += scores.return_3y_score * 0.12; totalWeight += 0.12; }
    if (scores.return_5y_score) { totalWeightedScore += scores.return_5y_score * 0.08; totalWeight += 0.08; }
    if (scores.return_ytd_score) { totalWeightedScore += scores.return_ytd_score * 0.05; totalWeight += 0.05; }
    
    // Risk metrics (25% total weight)
    if (scores.std_dev_1y_score) { totalWeightedScore += scores.std_dev_1y_score * 0.12; totalWeight += 0.12; }
    if (scores.max_drawdown_score) { totalWeightedScore += scores.max_drawdown_score * 0.08; totalWeight += 0.08; }
    if (scores.consistency_score) { totalWeightedScore += scores.consistency_score * 0.05; totalWeight += 0.05; }
    
    // Quality metrics (20% total weight)
    if (scores.expense_ratio_score) { totalWeightedScore += scores.expense_ratio_score * 0.08; totalWeight += 0.08; }
    if (scores.fund_size_score) { totalWeightedScore += scores.fund_size_score * 0.06; totalWeight += 0.06; }
    if (scores.fund_manager_score) { totalWeightedScore += scores.fund_manager_score * 0.06; totalWeight += 0.06; }
    
    // Advanced ratios (15% total weight)
    if (scores.sharpe_ratio_score) { totalWeightedScore += scores.sharpe_ratio_score * 0.08; totalWeight += 0.08; }
    if (scores.beta_score) { totalWeightedScore += scores.beta_score * 0.07; totalWeight += 0.07; }
    
    if (totalWeight >= 0.3) { // Minimum 30% data coverage required
      const overallRating = Math.round(totalWeightedScore / totalWeight);
      return Math.max(10, Math.min(100, overallRating));
    }
    
    return null;
    
  } catch (error) {
    return null;
  }
}

async function getQualityMetricsCoverage() {
  const result = await pool.query(`
    SELECT 
      COUNT(*) as total,
      COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as consistency_count,
      COUNT(CASE WHEN expense_ratio_score IS NOT NULL THEN 1 END) as expense_count,
      COUNT(CASE WHEN fund_size_score IS NOT NULL THEN 1 END) as size_count,
      COUNT(CASE WHEN fund_manager_score IS NOT NULL THEN 1 END) as manager_count,
      COUNT(CASE WHEN overall_rating IS NOT NULL THEN 1 END) as rating_count,
      ROUND(COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as consistency_pct,
      ROUND(COUNT(CASE WHEN overall_rating IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as rating_pct
    FROM fund_scores
  `);
  
  return result.rows[0];
}

implementQualityPerformanceMetrics();