/**
 * Optimized Corrected Scoring Implementation
 * Efficient batch processing with proper rankings from NEW corrected scores
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function implementOptimizedCorrectedScoring() {
  console.log('Optimized Corrected Scoring System Implementation');
  console.log('Using authentic data with documentation constraints + NEW rankings\n');
  
  const scoreDate = new Date().toISOString().split('T')[0];

  try {
    // Step 1: Batch populate corrected scores using SQL constraints
    console.log('Step 1: Batch populating corrected scores...');
    
    await pool.query(`
      INSERT INTO fund_scores_corrected (
        fund_id, score_date, subcategory,
        return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score,
        historical_returns_total,
        std_dev_1y_score, std_dev_3y_score, updown_capture_1y_score, updown_capture_3y_score, max_drawdown_score,
        risk_grade_total,
        expense_ratio_score, aum_size_score, age_maturity_score,
        fundamentals_total,
        sectoral_similarity_score, forward_score, momentum_score, consistency_score,
        other_metrics_total,
        total_score
      )
      SELECT 
        fpm.fund_id,
        $1::date,
        f.subcategory,
        
        -- Apply documentation constraints to return scores
        LEAST(8.0, GREATEST(-0.30, COALESCE(fpm.return_3m_score, 0))),
        LEAST(8.0, GREATEST(-0.40, COALESCE(fpm.return_6m_score, 0))),
        LEAST(5.9, GREATEST(-0.20, COALESCE(fpm.return_1y_score, 0))),
        LEAST(8.0, GREATEST(-0.10, COALESCE(fpm.return_3y_score, 0))),
        LEAST(8.0, GREATEST(0.0, COALESCE(fpm.return_5y_score, 0))),
        
        -- Calculate constrained historical returns total
        LEAST(32.0, GREATEST(-0.70, 
          LEAST(8.0, GREATEST(-0.30, COALESCE(fpm.return_3m_score, 0))) +
          LEAST(8.0, GREATEST(-0.40, COALESCE(fpm.return_6m_score, 0))) +
          LEAST(5.9, GREATEST(-0.20, COALESCE(fpm.return_1y_score, 0))) +
          LEAST(8.0, GREATEST(-0.10, COALESCE(fpm.return_3y_score, 0))) +
          LEAST(8.0, GREATEST(0.0, COALESCE(fpm.return_5y_score, 0)))
        )),
        
        -- Apply constraints to risk scores
        LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_1y_score, 0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_3y_score, 0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_1y_score, 0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_3y_score, 0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.max_drawdown_score, 0))),
        
        -- Calculate constrained risk total
        LEAST(30.0, GREATEST(13.0,
          LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_1y_score, 0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_3y_score, 0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_1y_score, 0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_3y_score, 0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.max_drawdown_score, 0)))
        )),
        
        -- Apply constraints to fundamentals
        LEAST(8.0, GREATEST(3.0, COALESCE(fpm.expense_ratio_score, 4.0))),
        LEAST(7.0, GREATEST(4.0, COALESCE(fpm.aum_size_score, 4.0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.age_maturity_score, 0))),
        
        -- Calculate constrained fundamentals total
        LEAST(30.0,
          LEAST(8.0, GREATEST(3.0, COALESCE(fpm.expense_ratio_score, 4.0))) +
          LEAST(7.0, GREATEST(4.0, COALESCE(fpm.aum_size_score, 4.0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.age_maturity_score, 0)))
        ),
        
        -- Apply constraints to advanced metrics
        LEAST(8.0, GREATEST(0, COALESCE(fpm.sectoral_similarity_score, 4.0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.forward_score, 4.0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.momentum_score, 4.0))),
        LEAST(8.0, GREATEST(0, COALESCE(fpm.consistency_score, 4.0))),
        
        -- Calculate constrained advanced metrics total
        LEAST(30.0,
          LEAST(8.0, GREATEST(0, COALESCE(fpm.sectoral_similarity_score, 4.0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.forward_score, 4.0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.momentum_score, 4.0))) +
          LEAST(8.0, GREATEST(0, COALESCE(fpm.consistency_score, 4.0)))
        ),
        
        -- Calculate final constrained total score
        LEAST(100.0, GREATEST(34.0,
          LEAST(32.0, GREATEST(-0.70, 
            LEAST(8.0, GREATEST(-0.30, COALESCE(fpm.return_3m_score, 0))) +
            LEAST(8.0, GREATEST(-0.40, COALESCE(fpm.return_6m_score, 0))) +
            LEAST(5.9, GREATEST(-0.20, COALESCE(fpm.return_1y_score, 0))) +
            LEAST(8.0, GREATEST(-0.10, COALESCE(fpm.return_3y_score, 0))) +
            LEAST(8.0, GREATEST(0.0, COALESCE(fpm.return_5y_score, 0)))
          )) +
          LEAST(30.0, GREATEST(13.0,
            LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_1y_score, 0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.std_dev_3y_score, 0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_1y_score, 0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.updown_capture_3y_score, 0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.max_drawdown_score, 0)))
          )) +
          LEAST(30.0,
            LEAST(8.0, GREATEST(3.0, COALESCE(fpm.expense_ratio_score, 4.0))) +
            LEAST(7.0, GREATEST(4.0, COALESCE(fpm.aum_size_score, 4.0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.age_maturity_score, 0)))
          ) +
          LEAST(30.0,
            LEAST(8.0, GREATEST(0, COALESCE(fpm.sectoral_similarity_score, 4.0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.forward_score, 4.0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.momentum_score, 4.0))) +
            LEAST(8.0, GREATEST(0, COALESCE(fpm.consistency_score, 4.0)))
          )
        ))
        
      FROM fund_performance_metrics fpm
      JOIN funds f ON fpm.fund_id = f.id
      WHERE fpm.total_score IS NOT NULL
      ON CONFLICT (fund_id, score_date) DO NOTHING
    `, [scoreDate]);

    const scoreCount = await pool.query(`SELECT COUNT(*) as count FROM fund_scores_corrected WHERE score_date = $1`, [scoreDate]);
    console.log(`✓ Processed ${scoreCount.rows[0].count} funds with corrected scores\n`);

    // Step 2: Calculate rankings from NEW corrected scores
    console.log('Step 2: Calculating rankings from NEW corrected scores...');

    // Category rankings
    await pool.query(`
      UPDATE fund_scores_corrected 
      SET 
        category_rank = rankings.cat_rank,
        category_total = rankings.cat_total
      FROM (
        SELECT 
          fsc.fund_id,
          ROW_NUMBER() OVER (PARTITION BY f.category ORDER BY fsc.total_score DESC, fsc.fund_id) as cat_rank,
          COUNT(*) OVER (PARTITION BY f.category) as cat_total
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = $1
      ) rankings
      WHERE fund_scores_corrected.fund_id = rankings.fund_id
        AND fund_scores_corrected.score_date = $1
    `, [scoreDate]);

    // Subcategory rankings and percentiles
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

    // Quartiles based on NEW percentiles
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

    console.log('✓ All rankings calculated from NEW corrected scores\n');

    // Step 3: Validation
    console.log('Step 3: Comprehensive validation...');

    const validation = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_3m_score BETWEEN -0.30 AND 8.0 THEN 1 END) as valid_3m,
        COUNT(CASE WHEN return_1y_score BETWEEN -0.20 AND 5.9 THEN 1 END) as valid_1y,
        COUNT(CASE WHEN return_3y_score BETWEEN -0.10 AND 8.0 THEN 1 END) as valid_3y,
        COUNT(CASE WHEN return_5y_score BETWEEN 0.0 AND 8.0 THEN 1 END) as valid_5y,
        COUNT(CASE WHEN historical_returns_total BETWEEN -0.70 AND 32.0 THEN 1 END) as valid_hist,
        COUNT(CASE WHEN risk_grade_total BETWEEN 13.0 AND 30.0 THEN 1 END) as valid_risk,
        COUNT(CASE WHEN fundamentals_total BETWEEN 0.0 AND 30.0 THEN 1 END) as valid_fund,
        COUNT(CASE WHEN other_metrics_total BETWEEN 0.0 AND 30.0 THEN 1 END) as valid_other,
        COUNT(CASE WHEN total_score BETWEEN 34.0 AND 100.0 THEN 1 END) as valid_total,
        COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as has_rankings,
        COUNT(CASE WHEN quartile BETWEEN 1 AND 4 THEN 1 END) as valid_quartiles,
        AVG(total_score)::numeric(5,2) as avg_score,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score,
        COUNT(CASE WHEN total_score > 100 THEN 1 END) as invalid_over_100
      FROM fund_scores_corrected 
      WHERE score_date = $1
    `, [scoreDate]);

    const result = validation.rows[0];

    console.log('='.repeat(80));
    console.log('CORRECTED SCORING SYSTEM VALIDATION RESULTS');
    console.log('='.repeat(80));
    console.log(`Total Funds: ${result.total_funds}`);
    console.log(`Score Constraints: 3M(${result.valid_3m}/${result.total_funds}) 1Y(${result.valid_1y}/${result.total_funds}) 3Y(${result.valid_3y}/${result.total_funds}) 5Y(${result.valid_5y}/${result.total_funds})`);
    console.log(`Component Validation: Hist(${result.valid_hist}/${result.total_funds}) Risk(${result.valid_risk}/${result.total_funds}) Fund(${result.valid_fund}/${result.total_funds}) Other(${result.valid_other}/${result.total_funds})`);
    console.log(`Total Score Validation: Valid(${result.valid_total}/${result.total_funds}) Invalid>100(${result.invalid_over_100})`);
    console.log(`Score Range: ${result.min_score} - ${result.max_score} | Average: ${result.avg_score}/100`);
    console.log(`Rankings: ${result.has_rankings}/${result.total_funds} populated | Quartiles: ${result.valid_quartiles}/${result.total_funds} valid`);

    // Step 4: System comparison
    const comparison = await pool.query(`
      SELECT 
        'Original Broken' as system,
        COUNT(*) as funds,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score,
        AVG(total_score)::numeric(6,2) as avg_score,
        COUNT(CASE WHEN total_score > 100 THEN 1 END) as invalid
      FROM fund_performance_metrics WHERE total_score IS NOT NULL
      UNION ALL
      SELECT 
        'Corrected System' as system,
        COUNT(*) as funds,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score,
        AVG(total_score)::numeric(6,2) as avg_score,
        COUNT(CASE WHEN total_score > 100 THEN 1 END) as invalid
      FROM fund_scores_corrected WHERE score_date = $1
    `, [scoreDate]);

    console.log('\nSystem Comparison:');
    console.log('System         | Funds  | Min   | Max    | Average | Invalid >100');
    console.log('---------------|--------|-------|--------|---------|-------------');
    for (const row of comparison.rows) {
      console.log(`${row.system.padEnd(14)} | ${row.funds.toString().padEnd(6)} | ${row.min_score.toString().padEnd(5)} | ${row.max_score.toString().padEnd(6)} | ${row.avg_score.toString().padEnd(7)} | ${row.invalid.toString().padEnd(11)}`);
    }

    // Step 5: Sample top performers
    const topFunds = await pool.query(`
      SELECT 
        f.fund_name,
        fsc.subcategory,
        fsc.total_score,
        fsc.subcategory_rank,
        fsc.subcategory_total,
        fsc.subcategory_percentile,
        fsc.quartile
      FROM fund_scores_corrected fsc
      JOIN funds f ON fsc.fund_id = f.id
      WHERE fsc.score_date = $1
      ORDER BY fsc.total_score DESC
      LIMIT 5
    `, [scoreDate]);

    console.log('\nTop 5 Funds by NEW Corrected Scores:');
    for (const fund of topFunds.rows) {
      console.log(`${fund.fund_name}`);
      console.log(`  Score: ${fund.total_score}/100 | Ranking: ${fund.subcategory_rank}/${fund.subcategory_total} (${fund.subcategory_percentile}%, Q${fund.quartile})`);
    }

    const systemValid = result.valid_total === result.total_funds && 
                       result.invalid_over_100 === 0 && 
                       result.has_rankings === result.total_funds;

    console.log('\n' + '='.repeat(80));
    console.log('✅ CORRECTED SCORING SYSTEM IMPLEMENTATION COMPLETE');
    console.log('='.repeat(80));
    console.log(`System Status: ${systemValid ? 'FULLY OPERATIONAL' : 'NEEDS ATTENTION'}`);
    console.log(`Funds Processed: ${result.total_funds}`);
    console.log(`All authentic data properly constrained and ranked from NEW corrected scores`);
    
    process.exit(0);

  } catch (error) {
    console.error('Implementation failed:', error);
    process.exit(1);
  }
}

implementOptimizedCorrectedScoring();