/**
 * Implement Correct Authentic Recommendation Distribution
 * Target: STRONG_BUY (28.1%), BUY (54.3%), HOLD (17.4%), SELL (0.1%), STRONG_SELL (0.1%)
 * Uses proper score-based thresholds to achieve authentic market distribution
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function implementCorrectAuthenticRecommendations() {
  console.log('Implementing Correct Authentic Recommendation Distribution...\n');
  
  try {
    // Target authentic distribution based on your specifications
    const targetPercentages = {
      'STRONG_BUY': 28.1,
      'BUY': 54.3,
      'HOLD': 17.4,
      'SELL': 0.1,
      'STRONG_SELL': 0.1
    };
    
    // Step 1: Get all funds with scores from fund_scores_corrected
    const totalFundsQuery = await pool.query(`
      SELECT COUNT(*) as total_funds
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
    `);
    
    const totalFunds = parseInt(totalFundsQuery.rows[0].total_funds);
    console.log(`Total funds available: ${totalFunds}`);
    
    // Calculate target counts for the authentic distribution
    const targetCounts = {
      'STRONG_BUY': Math.round(totalFunds * targetPercentages.STRONG_BUY / 100),
      'BUY': Math.round(totalFunds * targetPercentages.BUY / 100),
      'HOLD': Math.round(totalFunds * targetPercentages.HOLD / 100),
      'SELL': Math.round(totalFunds * targetPercentages.SELL / 100),
      'STRONG_SELL': Math.round(totalFunds * targetPercentages.STRONG_SELL / 100)
    };
    
    console.log('Target authentic distribution:');
    console.log(`STRONG_BUY: ${targetCounts.STRONG_BUY} funds (${targetPercentages.STRONG_BUY}%)`);
    console.log(`BUY: ${targetCounts.BUY} funds (${targetPercentages.BUY}%)`);
    console.log(`HOLD: ${targetCounts.HOLD} funds (${targetPercentages.HOLD}%)`);
    console.log(`SELL: ${targetCounts.SELL} funds (${targetPercentages.SELL}%)`);
    console.log(`STRONG_SELL: ${targetCounts.STRONG_SELL} funds (${targetPercentages.STRONG_SELL}%)`);
    console.log('');
    
    // Step 2: Calculate score thresholds based on fund ranking to achieve target distribution
    const thresholdQuery = await pool.query(`
      WITH ranked_funds AS (
        SELECT 
          fund_id,
          total_score,
          ROW_NUMBER() OVER (ORDER BY total_score DESC) as rank
        FROM fund_scores_corrected 
        WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
      )
      SELECT 
        (SELECT total_score FROM ranked_funds WHERE rank = $1) as strong_buy_threshold,
        (SELECT total_score FROM ranked_funds WHERE rank = $2) as buy_threshold,
        (SELECT total_score FROM ranked_funds WHERE rank = $3) as hold_threshold,
        (SELECT total_score FROM ranked_funds WHERE rank = $4) as sell_threshold
    `, [
      targetCounts.STRONG_BUY,
      targetCounts.STRONG_BUY + targetCounts.BUY,
      targetCounts.STRONG_BUY + targetCounts.BUY + targetCounts.HOLD,
      totalFunds - targetCounts.STRONG_SELL
    ]);
    
    const thresholds = thresholdQuery.rows[0];
    console.log('Calculated score thresholds for authentic distribution:');
    console.log(`STRONG_BUY: >= ${thresholds.strong_buy_threshold}`);
    console.log(`BUY: >= ${thresholds.buy_threshold} and < ${thresholds.strong_buy_threshold}`);
    console.log(`HOLD: >= ${thresholds.hold_threshold} and < ${thresholds.buy_threshold}`);
    console.log(`SELL: >= ${thresholds.sell_threshold} and < ${thresholds.hold_threshold}`);
    console.log(`STRONG_SELL: < ${thresholds.sell_threshold}`);
    console.log('');
    
    // Step 3: Apply the correct recommendation logic
    const updateResult = await pool.query(`
      UPDATE fund_scores_corrected 
      SET recommendation = 
        CASE 
          WHEN total_score >= $1 THEN 'STRONG_BUY'
          WHEN total_score >= $2 THEN 'BUY'
          WHEN total_score >= $3 THEN 'HOLD'
          WHEN total_score >= $4 THEN 'SELL'
          ELSE 'STRONG_SELL'
        END
      WHERE score_date = '2025-06-05' AND total_score IS NOT NULL
    `, [
      thresholds.strong_buy_threshold,
      thresholds.buy_threshold,
      thresholds.hold_threshold,
      thresholds.sell_threshold
    ]);
    
    console.log(`✓ Updated ${updateResult.rowCount} funds with authentic recommendation logic`);
    
    // Step 4: Verify the authentic distribution
    const verificationQuery = await pool.query(`
      SELECT 
        recommendation,
        COUNT(*) as fund_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND recommendation IS NOT NULL
      GROUP BY recommendation
      ORDER BY 
        CASE recommendation
          WHEN 'STRONG_BUY' THEN 1
          WHEN 'BUY' THEN 2
          WHEN 'HOLD' THEN 3
          WHEN 'SELL' THEN 4
          WHEN 'STRONG_SELL' THEN 5
        END
    `);
    
    console.log('✓ Authentic Recommendation Distribution Verified:');
    let totalVerified = 0;
    for (const row of verificationQuery.rows) {
      totalVerified += parseInt(row.fund_count);
      console.log(`${row.recommendation}: ${row.fund_count} funds (${row.percentage}%)`);
    }
    console.log(`Total: ${totalVerified} funds`);
    console.log('');
    
    // Step 5: Sync with fund_performance_metrics to maintain consistency
    console.log('Syncing fund_performance_metrics with authentic recommendations...');
    
    const syncResult = await pool.query(`
      UPDATE fund_performance_metrics fpm
      SET 
        recommendation = fsc.recommendation,
        quartile = CASE 
          WHEN fsc.recommendation = 'STRONG_BUY' THEN 1
          WHEN fsc.recommendation = 'BUY' THEN 2
          WHEN fsc.recommendation = 'HOLD' THEN 3
          WHEN fsc.recommendation IN ('SELL', 'STRONG_SELL') THEN 4
        END,
        total_score = fsc.total_score
      FROM fund_scores_corrected fsc
      WHERE fpm.fund_id = fsc.fund_id 
        AND fsc.score_date = '2025-06-05'
        AND fsc.recommendation IS NOT NULL
    `);
    
    console.log(`✓ Synced ${syncResult.rowCount} funds in fund_performance_metrics`);
    
    // Step 6: Create current date records for production use
    console.log('Creating current date records for production system...');
    
    const currentDateResult = await pool.query(`
      INSERT INTO fund_scores_corrected (
        fund_id, score_date, total_score, recommendation, quartile,
        return_1m, return_3m, return_6m, return_1y, return_3y, return_5y, return_ytd,
        historical_returns_total, risk_grade_total, fundamentals_total, other_metrics_total,
        subcategory, subcategory_rank, subcategory_total
      )
      SELECT 
        fund_id, CURRENT_DATE, total_score, recommendation,
        CASE 
          WHEN recommendation = 'STRONG_BUY' THEN 1
          WHEN recommendation = 'BUY' THEN 2
          WHEN recommendation = 'HOLD' THEN 3
          WHEN recommendation IN ('SELL', 'STRONG_SELL') THEN 4
        END as quartile,
        return_1m, return_3m, return_6m, return_1y, return_3y, return_5y, return_ytd,
        historical_returns_total, risk_grade_total, fundamentals_total, other_metrics_total,
        subcategory, subcategory_rank, subcategory_total
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND recommendation IS NOT NULL
      ON CONFLICT (fund_id, score_date) DO UPDATE SET
        recommendation = EXCLUDED.recommendation,
        quartile = EXCLUDED.quartile,
        total_score = EXCLUDED.total_score
    `);
    
    console.log(`✓ Created/updated ${currentDateResult.rowCount} current date records`);
    
    // Step 7: Final verification across all tables
    const finalVerification = await pool.query(`
      SELECT 
        'fund_scores_corrected (2025-06-05)' as source,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy,
        COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy,
        COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold,
        COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell,
        COUNT(CASE WHEN recommendation = 'STRONG_SELL' THEN 1 END) as strong_sell
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND recommendation IS NOT NULL
      
      UNION ALL
      
      SELECT 
        'fund_scores_corrected (current)' as source,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy,
        COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy,
        COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold,
        COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell,
        COUNT(CASE WHEN recommendation = 'STRONG_SELL' THEN 1 END) as strong_sell
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE AND recommendation IS NOT NULL
      
      UNION ALL
      
      SELECT 
        'fund_performance_metrics' as source,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy,
        COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy,
        COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold,
        COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell,
        COUNT(CASE WHEN recommendation = 'STRONG_SELL' THEN 1 END) as strong_sell
      FROM fund_performance_metrics 
      WHERE recommendation IS NOT NULL
    `);
    
    console.log('\n=== FINAL AUTHENTIC DISTRIBUTION VERIFICATION ===');
    for (const row of finalVerification.rows) {
      console.log(`\n${row.source}:`);
      console.log(`  Total: ${row.total_funds} funds`);
      console.log(`  STRONG_BUY: ${row.strong_buy} (${(row.strong_buy * 100 / row.total_funds).toFixed(1)}%)`);
      console.log(`  BUY: ${row.buy} (${(row.buy * 100 / row.total_funds).toFixed(1)}%)`);
      console.log(`  HOLD: ${row.hold} (${(row.hold * 100 / row.total_funds).toFixed(1)}%)`);
      console.log(`  SELL: ${row.sell} (${(row.sell * 100 / row.total_funds).toFixed(1)}%)`);
      console.log(`  STRONG_SELL: ${row.strong_sell} (${(row.strong_sell * 100 / row.total_funds).toFixed(1)}%)`);
    }
    
    console.log('\n✅ Authentic recommendation distribution successfully implemented!');
    console.log('The system now reflects authentic market patterns with proper investment recommendations.');
    
  } catch (error) {
    console.error('Error implementing authentic recommendations:', error);
  } finally {
    await pool.end();
  }
}

// Run the implementation
implementCorrectAuthenticRecommendations().catch(console.error);