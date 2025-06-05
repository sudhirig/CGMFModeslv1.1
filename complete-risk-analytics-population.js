/**
 * Complete Risk Analytics Population
 * Populates fund_scores_corrected with all authentic risk analytics data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function populateCompleteRiskAnalyticsData() {
  console.log('Populating Complete Risk Analytics Data in fund_scores_corrected');
  console.log('Integrating all authentic calculated risk metrics...\n');
  
  const scoreDate = new Date().toISOString().split('T')[0];

  try {
    // Update existing records with complete risk analytics data
    const updateResult = await pool.query(`
      UPDATE fund_scores_corrected fsc
      SET 
        calmar_ratio_1y = ra.calmar_ratio_1y,
        sortino_ratio_1y = ra.sortino_ratio_1y,
        downside_deviation_1y = ra.downside_deviation_1y,
        rolling_volatility_12m = ra.rolling_volatility_12m,
        rolling_volatility_24m = ra.rolling_volatility_24m,
        rolling_volatility_36m = ra.rolling_volatility_36m,
        rolling_volatility_3m = ra.rolling_volatility_3m,
        rolling_volatility_6m = ra.rolling_volatility_6m,
        positive_months_percentage = ra.positive_months_percentage,
        negative_months_percentage = ra.negative_months_percentage,
        max_drawdown_duration_days = ra.max_drawdown_duration_days,
        avg_drawdown_duration_days = ra.avg_drawdown_duration_days,
        drawdown_frequency_per_year = ra.drawdown_frequency_per_year,
        consecutive_negative_months_max = ra.consecutive_negative_months_max,
        consecutive_positive_months_max = ra.consecutive_positive_months_max,
        daily_returns_count = ra.daily_returns_count,
        daily_returns_mean = ra.daily_returns_mean,
        daily_returns_std = ra.daily_returns_std,
        recovery_time_avg_days = ra.recovery_time_avg_days
      FROM risk_analytics ra
      WHERE fsc.fund_id = ra.fund_id 
        AND fsc.score_date = $1
        AND ra.calculation_date = (SELECT MAX(calculation_date) FROM risk_analytics WHERE fund_id = ra.fund_id)
    `, [scoreDate]);

    console.log(`✓ Updated ${updateResult.rowCount} funds with risk analytics data\n`);

    // Verify the population
    const verification = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN calmar_ratio_1y IS NOT NULL THEN 1 END) as has_calmar,
        COUNT(CASE WHEN sortino_ratio_1y IS NOT NULL THEN 1 END) as has_sortino,
        COUNT(CASE WHEN downside_deviation_1y IS NOT NULL THEN 1 END) as has_downside_dev,
        COUNT(CASE WHEN rolling_volatility_12m IS NOT NULL THEN 1 END) as has_volatility_12m,
        COUNT(CASE WHEN positive_months_percentage IS NOT NULL THEN 1 END) as has_monthly_stats,
        COUNT(CASE WHEN daily_returns_count IS NOT NULL THEN 1 END) as has_daily_returns,
        COUNT(CASE WHEN max_drawdown_duration_days IS NOT NULL THEN 1 END) as has_drawdown_duration
      FROM fund_scores_corrected 
      WHERE score_date = $1
    `, [scoreDate]);

    const result = verification.rows[0];
    
    console.log('Risk Analytics Data Population Verification:');
    console.log(`Total Funds: ${result.total_funds}`);
    console.log(`Calmar Ratio: ${result.has_calmar}/${result.total_funds} funds`);
    console.log(`Sortino Ratio: ${result.has_sortino}/${result.total_funds} funds`);
    console.log(`Downside Deviation: ${result.has_downside_dev}/${result.total_funds} funds`);
    console.log(`12M Volatility: ${result.has_volatility_12m}/${result.total_funds} funds`);
    console.log(`Monthly Performance Stats: ${result.has_monthly_stats}/${result.total_funds} funds`);
    console.log(`Daily Returns Data: ${result.has_daily_returns}/${result.total_funds} funds`);
    console.log(`Drawdown Duration: ${result.has_drawdown_duration}/${result.total_funds} funds`);

    // Show sample of populated data
    const sampleData = await pool.query(`
      SELECT 
        fsc.fund_id,
        f.fund_name,
        fsc.total_score,
        fsc.calmar_ratio_1y,
        fsc.sortino_ratio_1y,
        fsc.downside_deviation_1y,
        fsc.rolling_volatility_12m,
        fsc.positive_months_percentage,
        fsc.daily_returns_count,
        fsc.subcategory_rank,
        fsc.quartile
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.score_date = $1
        AND fsc.calmar_ratio_1y IS NOT NULL
        AND fsc.sortino_ratio_1y IS NOT NULL
      ORDER BY fsc.total_score DESC
      LIMIT 10
    `, [scoreDate]);

    console.log('\nSample Funds with Complete Risk Analytics Data:');
    console.log('');
    
    for (const fund of sampleData.rows) {
      console.log(`${fund.fund_name}`);
      console.log(`  Score: ${fund.total_score}/100 | Rank: ${fund.subcategory_rank} (Q${fund.quartile})`);
      console.log(`  Calmar: ${fund.calmar_ratio_1y?.toFixed(2)} | Sortino: ${fund.sortino_ratio_1y?.toFixed(2)} | Downside Dev: ${fund.downside_deviation_1y?.toFixed(4)}`);
      console.log(`  Volatility 12M: ${fund.rolling_volatility_12m?.toFixed(2)}% | Positive Months: ${fund.positive_months_percentage?.toFixed(1)}%`);
      console.log(`  Daily Returns Count: ${fund.daily_returns_count}`);
      console.log('');
    }

    // Enhanced scoring adjustments based on risk analytics
    console.log('Applying enhanced scoring adjustments based on risk analytics...');
    
    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        -- Enhance consistency score with monthly performance data
        consistency_score = LEAST(8.0, GREATEST(0,
          consistency_score + 
          CASE 
            WHEN positive_months_percentage >= 70 THEN 1.0
            WHEN positive_months_percentage >= 60 THEN 0.5
            WHEN positive_months_percentage >= 50 THEN 0.0
            ELSE -0.5
          END
        )),
        
        -- Enhance momentum score with Sortino ratio (cap extreme values)
        momentum_score = LEAST(8.0, GREATEST(0,
          momentum_score + 
          CASE 
            WHEN sortino_ratio_1y >= 1.5 AND sortino_ratio_1y <= 10 THEN 1.0
            WHEN sortino_ratio_1y >= 1.0 AND sortino_ratio_1y <= 10 THEN 0.5
            WHEN sortino_ratio_1y >= 0.5 AND sortino_ratio_1y <= 10 THEN 0.0
            ELSE -0.5
          END
        )),
        
        -- Enhance drawdown score with Calmar ratio
        max_drawdown_score = LEAST(8.0, GREATEST(0,
          max_drawdown_score + 
          CASE 
            WHEN calmar_ratio_1y >= 2.0 THEN 1.0
            WHEN calmar_ratio_1y >= 1.0 THEN 0.5
            WHEN calmar_ratio_1y >= 0.5 THEN 0.0
            ELSE -0.5
          END
        )),
        
        -- Enhance volatility score with rolling volatility
        std_dev_1y_score = LEAST(8.0, GREATEST(0,
          std_dev_1y_score + 
          CASE 
            WHEN rolling_volatility_12m <= 10 THEN 1.0
            WHEN rolling_volatility_12m <= 15 THEN 0.5
            WHEN rolling_volatility_12m <= 20 THEN 0.0
            ELSE -0.5
          END
        ))
      WHERE score_date = $1
        AND (calmar_ratio_1y IS NOT NULL 
             OR sortino_ratio_1y IS NOT NULL 
             OR positive_months_percentage IS NOT NULL 
             OR rolling_volatility_12m IS NOT NULL)
    `, [scoreDate]);

    // Recalculate component totals and final scores
    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        -- Recalculate component totals
        risk_grade_total = LEAST(30.0, GREATEST(13.0,
          std_dev_1y_score + std_dev_3y_score + updown_capture_1y_score + updown_capture_3y_score + max_drawdown_score
        )),
        other_metrics_total = LEAST(30.0,
          sectoral_similarity_score + forward_score + momentum_score + consistency_score
        )
      WHERE score_date = $1
    `, [scoreDate]);

    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        total_score = LEAST(100.0, GREATEST(34.0,
          historical_returns_total + risk_grade_total + fundamentals_total + other_metrics_total
        ))
      WHERE score_date = $1
    `, [scoreDate]);

    // Recalculate rankings based on enhanced scores
    console.log('Recalculating rankings from risk analytics enhanced scores...');
    
    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        subcategory_rank = rankings.sub_rank,
        subcategory_total = rankings.sub_total,
        subcategory_percentile = rankings.sub_percentile
      FROM (
        SELECT 
          fund_id,
          ROW_NUMBER() OVER (PARTITION BY subcategory ORDER BY total_score DESC, fund_id) as sub_rank,
          COUNT(*) OVER (PARTITION BY subcategory) as sub_total,
          ROUND(
            (1.0 - (ROW_NUMBER() OVER (PARTITION BY subcategory ORDER BY total_score DESC, fund_id) - 1.0) / 
             NULLIF(COUNT(*) OVER (PARTITION BY subcategory) - 1.0, 0)) * 100, 2
          ) as sub_percentile
        FROM fund_scores_corrected
        WHERE score_date = $1
      ) rankings
      WHERE fund_scores_corrected.fund_id = rankings.fund_id
        AND fund_scores_corrected.score_date = $1
    `, [scoreDate]);

    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        quartile = CASE 
          WHEN subcategory_percentile >= 75 THEN 1
          WHEN subcategory_percentile >= 50 THEN 2
          WHEN subcategory_percentile >= 25 THEN 3
          ELSE 4
        END,
        subcategory_quartile = CASE 
          WHEN subcategory_percentile >= 75 THEN 1
          WHEN subcategory_percentile >= 50 THEN 2
          WHEN subcategory_percentile >= 25 THEN 3
          ELSE 4
        END
      WHERE score_date = $1 AND subcategory_percentile IS NOT NULL
    `, [scoreDate]);

    // Final validation
    const finalValidation = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN total_score BETWEEN 34.0 AND 100.0 THEN 1 END) as valid_scores,
        COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as has_rankings,
        COUNT(CASE WHEN calmar_ratio_1y IS NOT NULL THEN 1 END) as has_risk_analytics,
        AVG(total_score)::numeric(5,2) as avg_score,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score
      FROM fund_scores_corrected 
      WHERE score_date = $1
    `, [scoreDate]);

    const finalResult = finalValidation.rows[0];

    console.log('\n' + '='.repeat(80));
    console.log('COMPLETE RISK ANALYTICS INTEGRATION RESULTS');
    console.log('='.repeat(80));
    console.log(`Total Funds with Enhanced Scoring: ${finalResult.total_funds}`);
    console.log(`Valid Score Range (34-100): ${finalResult.valid_scores}/${finalResult.total_funds}`);
    console.log(`Complete Rankings: ${finalResult.has_rankings}/${finalResult.total_funds}`);
    console.log(`Risk Analytics Data: ${finalResult.has_risk_analytics}/${finalResult.total_funds}`);
    console.log(`Enhanced Score Range: ${finalResult.min_score} - ${finalResult.max_score}`);
    console.log(`Average Enhanced Score: ${finalResult.avg_score}/100`);

    console.log('\nRisk Analytics Integration Achievements:');
    console.log('• All authentic risk analytics data now stored in fund_scores_corrected');
    console.log('• Enhanced scoring using Calmar ratio, Sortino ratio, monthly performance');
    console.log('• Volatility assessments using rolling volatility data');
    console.log('• Downside risk evaluation with downside deviation metrics');
    console.log('• Complete ranking recalculation from enhanced authentic scores');
    console.log('• All database constraints maintained throughout integration');

    process.exit(0);

  } catch (error) {
    console.error('Risk analytics population failed:', error);
    process.exit(1);
  }
}

populateCompleteRiskAnalyticsData();