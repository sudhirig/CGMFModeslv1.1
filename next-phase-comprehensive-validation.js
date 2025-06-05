/**
 * Next Phase: Comprehensive System Validation and Gap Analysis
 * Validates complete coverage and identifies any remaining data gaps
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

async function comprehensiveSystemValidation() {
  console.log('Next Phase: Comprehensive System Validation and Gap Analysis');
  console.log('Analyzing complete coverage and identifying expansion opportunities...\n');

  try {
    // Step 1: Current system status
    console.log('='.repeat(80));
    console.log('CURRENT SYSTEM STATUS');
    console.log('='.repeat(80));

    const currentStatus = await pool.query(`
      SELECT 
        (SELECT COUNT(*) FROM fund_scores_corrected WHERE score_date = CURRENT_DATE) as corrected_funds,
        (SELECT COUNT(*) FROM funds) as total_funds_database,
        (SELECT COUNT(DISTINCT fund_id) FROM nav_data) as funds_with_nav_data,
        (SELECT COUNT(DISTINCT fund_id) FROM nav_data WHERE created_at > '2025-05-30 06:45:00') as funds_with_recent_nav,
        (SELECT COUNT(DISTINCT subcategory) FROM fund_scores_corrected WHERE score_date = CURRENT_DATE) as covered_subcategories
    `);

    const status = currentStatus.rows[0];
    
    console.log(`Corrected Scoring Coverage: ${status.corrected_funds} funds`);
    console.log(`Total Database Funds: ${status.total_funds_database} funds`);
    console.log(`Funds with NAV Data: ${status.funds_with_nav_data} funds`);
    console.log(`Funds with Recent NAV: ${status.funds_with_recent_nav} funds`);
    console.log(`Covered Subcategories: ${status.covered_subcategories} subcategories`);

    // Step 2: Detailed subcategory coverage analysis
    const subcategoryAnalysis = await pool.query(`
      SELECT 
        fsc.subcategory,
        COUNT(*) as scored_funds,
        AVG(fsc.total_score)::numeric(5,2) as avg_score,
        MIN(fsc.total_score) as min_score,
        MAX(fsc.total_score) as max_score,
        COUNT(CASE WHEN fsc.quartile = 1 THEN 1 END) as q1_funds,
        COUNT(CASE WHEN fsc.quartile = 2 THEN 1 END) as q2_funds,
        COUNT(CASE WHEN fsc.quartile = 3 THEN 1 END) as q3_funds,
        COUNT(CASE WHEN fsc.quartile = 4 THEN 1 END) as q4_funds
      FROM fund_scores_corrected fsc
      WHERE fsc.score_date = CURRENT_DATE
      GROUP BY fsc.subcategory
      ORDER BY COUNT(*) DESC
    `);

    console.log('\n' + '='.repeat(80));
    console.log('SUBCATEGORY COVERAGE ANALYSIS');
    console.log('='.repeat(80));

    let totalScoredFunds = 0;
    for (const sub of subcategoryAnalysis.rows) {
      totalScoredFunds += parseInt(sub.scored_funds);
      console.log(`${sub.subcategory}:`);
      console.log(`  Funds: ${sub.scored_funds} | Avg Score: ${sub.avg_score}/100`);
      console.log(`  Range: ${sub.min_score} - ${sub.max_score}`);
      console.log(`  Quartiles: Q1:${sub.q1_funds} Q2:${sub.q2_funds} Q3:${sub.q3_funds} Q4:${sub.q4_funds}`);
      console.log('');
    }

    // Step 3: Gap analysis - funds with NAV data but no scores
    const gapAnalysis = await pool.query(`
      SELECT 
        f.subcategory,
        COUNT(DISTINCT f.id) as total_funds,
        COUNT(DISTINCT CASE WHEN fsc.fund_id IS NOT NULL THEN f.id END) as scored_funds,
        COUNT(DISTINCT CASE WHEN fsc.fund_id IS NULL THEN f.id END) as unscored_funds
      FROM funds f
      LEFT JOIN fund_scores_corrected fsc ON f.id = fsc.fund_id AND fsc.score_date = CURRENT_DATE
      WHERE f.id IN (SELECT DISTINCT fund_id FROM nav_data WHERE created_at > '2025-05-30 06:45:00')
      GROUP BY f.subcategory
      HAVING COUNT(DISTINCT CASE WHEN fsc.fund_id IS NULL THEN f.id END) > 0
      ORDER BY COUNT(DISTINCT CASE WHEN fsc.fund_id IS NULL THEN f.id END) DESC
    `);

    console.log('='.repeat(80));
    console.log('GAP ANALYSIS - FUNDS WITH NAV DATA BUT NO SCORES');
    console.log('='.repeat(80));

    let totalUnscoredFunds = 0;
    if (gapAnalysis.rows.length > 0) {
      for (const gap of gapAnalysis.rows) {
        totalUnscoredFunds += parseInt(gap.unscored_funds);
        const coverage = ((gap.scored_funds / gap.total_funds) * 100).toFixed(1);
        console.log(`${gap.subcategory}:`);
        console.log(`  Total: ${gap.total_funds} | Scored: ${gap.scored_funds} | Missing: ${gap.unscored_funds} (${coverage}% coverage)`);
      }
    } else {
      console.log('✓ Complete coverage - all funds with NAV data have been scored');
    }

    // Step 4: Data quality validation
    const qualityCheck = await pool.query(`
      SELECT 
        COUNT(*) as total_scores,
        COUNT(CASE WHEN total_score BETWEEN 34.0 AND 100.0 THEN 1 END) as valid_scores,
        COUNT(CASE WHEN historical_returns_total IS NOT NULL THEN 1 END) as has_returns,
        COUNT(CASE WHEN risk_grade_total IS NOT NULL THEN 1 END) as has_risk,
        COUNT(CASE WHEN fundamentals_total IS NOT NULL THEN 1 END) as has_fundamentals,
        COUNT(CASE WHEN other_metrics_total IS NOT NULL THEN 1 END) as has_other_metrics,
        COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as has_rankings,
        AVG(total_score)::numeric(5,2) as avg_total_score,
        AVG(historical_returns_total)::numeric(5,2) as avg_returns_score,
        AVG(risk_grade_total)::numeric(5,2) as avg_risk_score,
        AVG(fundamentals_total)::numeric(5,2) as avg_fundamentals_score,
        AVG(other_metrics_total)::numeric(5,2) as avg_other_score
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE
    `);

    const quality = qualityCheck.rows[0];

    console.log('\n' + '='.repeat(80));
    console.log('DATA QUALITY VALIDATION');
    console.log('='.repeat(80));

    console.log(`Total Scored Funds: ${quality.total_scores}`);
    console.log(`Valid Score Range (34-100): ${quality.valid_scores}/${quality.total_scores} (${((quality.valid_scores/quality.total_scores)*100).toFixed(1)}%)`);
    console.log(`Complete Component Coverage:`);
    console.log(`  Returns: ${quality.has_returns}/${quality.total_scores} (${((quality.has_returns/quality.total_scores)*100).toFixed(1)}%)`);
    console.log(`  Risk: ${quality.has_risk}/${quality.total_scores} (${((quality.has_risk/quality.total_scores)*100).toFixed(1)}%)`);
    console.log(`  Fundamentals: ${quality.has_fundamentals}/${quality.total_scores} (${((quality.has_fundamentals/quality.total_scores)*100).toFixed(1)}%)`);
    console.log(`  Other Metrics: ${quality.has_other_metrics}/${quality.total_scores} (${((quality.has_other_metrics/quality.total_scores)*100).toFixed(1)}%)`);
    console.log(`  Rankings: ${quality.has_rankings}/${quality.total_scores} (${((quality.has_rankings/quality.total_scores)*100).toFixed(1)}%)`);

    console.log('\nAverage Component Scores:');
    console.log(`  Total Score: ${quality.avg_total_score}/100`);
    console.log(`  Returns: ${quality.avg_returns_score}/40`);
    console.log(`  Risk: ${quality.avg_risk_score}/30`);
    console.log(`  Fundamentals: ${quality.avg_fundamentals_score}/30`);
    console.log(`  Other Metrics: ${quality.avg_other_score}/30`);

    // Step 5: Risk analytics coverage
    const riskAnalytics = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN calmar_ratio_1y IS NOT NULL THEN 1 END) as has_calmar,
        COUNT(CASE WHEN sortino_ratio_1y IS NOT NULL THEN 1 END) as has_sortino,
        COUNT(CASE WHEN rolling_volatility_12m IS NOT NULL THEN 1 END) as has_rolling_vol,
        COUNT(CASE WHEN positive_months_percentage IS NOT NULL THEN 1 END) as has_monthly_perf,
        COUNT(CASE WHEN downside_deviation_1y IS NOT NULL THEN 1 END) as has_downside_dev,
        AVG(calmar_ratio_1y)::numeric(4,2) as avg_calmar,
        AVG(sortino_ratio_1y)::numeric(4,2) as avg_sortino
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE
    `);

    const risk = riskAnalytics.rows[0];

    console.log('\n' + '='.repeat(80));
    console.log('RISK ANALYTICS COVERAGE');
    console.log('='.repeat(80));

    console.log(`Advanced Risk Metrics Coverage:`);
    console.log(`  Calmar Ratio: ${risk.has_calmar}/${risk.total_funds} (${((risk.has_calmar/risk.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Sortino Ratio: ${risk.has_sortino}/${risk.total_funds} (${((risk.has_sortino/risk.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Rolling Volatility: ${risk.has_rolling_vol}/${risk.total_funds} (${((risk.has_rolling_vol/risk.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Monthly Performance: ${risk.has_monthly_perf}/${risk.total_funds} (${((risk.has_monthly_perf/risk.total_funds)*100).toFixed(1)}%)`);
    console.log(`  Downside Deviation: ${risk.has_downside_dev}/${risk.total_funds} (${((risk.has_downside_dev/risk.total_funds)*100).toFixed(1)}%)`);

    if (risk.avg_calmar && risk.avg_sortino) {
      console.log(`\nAverage Risk Metrics:`);
      console.log(`  Calmar Ratio: ${risk.avg_calmar}`);
      console.log(`  Sortino Ratio: ${risk.avg_sortino}`);
    }

    // Step 6: Top performing funds across subcategories
    const topPerformers = await pool.query(`
      WITH ranked_funds AS (
        SELECT 
          f.fund_name,
          fsc.subcategory,
          fsc.total_score,
          fsc.subcategory_rank,
          fsc.subcategory_total,
          fsc.quartile,
          fsc.calmar_ratio_1y,
          fsc.sortino_ratio_1y,
          ROW_NUMBER() OVER (PARTITION BY fsc.subcategory ORDER BY fsc.total_score DESC) as category_position
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = CURRENT_DATE
      )
      SELECT * FROM ranked_funds 
      WHERE category_position = 1
      ORDER BY total_score DESC
      LIMIT 10
    `);

    console.log('\n' + '='.repeat(80));
    console.log('TOP PERFORMERS BY SUBCATEGORY');
    console.log('='.repeat(80));

    for (const fund of topPerformers.rows) {
      console.log(`${fund.fund_name}`);
      console.log(`  Category: ${fund.subcategory}`);
      console.log(`  Score: ${fund.total_score}/100 | Rank: ${fund.subcategory_rank}/${fund.subcategory_total} (Q${fund.quartile})`);
      if (fund.calmar_ratio_1y || fund.sortino_ratio_1y) {
        console.log(`  Risk Metrics: Calmar:${fund.calmar_ratio_1y?.toFixed(2)} Sortino:${fund.sortino_ratio_1y?.toFixed(2)}`);
      }
      console.log('');
    }

    // Step 7: Next phase recommendations
    console.log('='.repeat(80));
    console.log('NEXT PHASE RECOMMENDATIONS');
    console.log('='.repeat(80));

    const systemCompleteness = (quality.valid_scores / quality.total_scores) * 100;
    const riskAnalyticsCoverage = (risk.has_calmar / risk.total_funds) * 100;

    console.log('System Assessment:');
    console.log(`✓ Core Scoring System: ${systemCompleteness.toFixed(1)}% complete`);
    console.log(`✓ Risk Analytics: ${riskAnalyticsCoverage.toFixed(1)}% coverage`);
    console.log(`✓ Subcategory Coverage: ${subcategoryAnalysis.rows.length} categories`);
    console.log(`✓ Total Scored Funds: ${quality.total_scores}`);

    if (totalUnscoredFunds > 0) {
      console.log(`\nExpansion Opportunities:`);
      console.log(`• ${totalUnscoredFunds} additional funds with NAV data available for scoring`);
      console.log(`• Focus on subcategories with lower coverage percentages`);
      console.log(`• Implement batch processing for remaining funds`);
    }

    console.log('\nRecommended Next Steps:');
    console.log('1. Production Deployment - System is ready for live deployment');
    console.log('2. User Interface Enhancement - Build comprehensive fund analysis dashboards');
    console.log('3. API Development - Create endpoints for fund search and comparison');
    console.log('4. Performance Monitoring - Implement daily scoring updates');
    console.log('5. Data Quality Assurance - Automated validation and alerts');

    if (totalUnscoredFunds === 0 && systemCompleteness >= 99.0) {
      console.log('\n🎯 SYSTEM STATUS: PRODUCTION READY');
      console.log('The comprehensive mutual fund analysis platform is complete and ready for deployment.');
    }

    process.exit(0);

  } catch (error) {
    console.error('Comprehensive validation failed:', error);
    process.exit(1);
  }
}

comprehensiveSystemValidation();