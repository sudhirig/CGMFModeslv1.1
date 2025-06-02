/**
 * Complete Phase Status Report and Systematic Completion
 * Analyzes current progress and identifies authentic data gaps
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function generateCompletePhaseStatusReport() {
  try {
    console.log('=== COMPLETE 5-PHASE IMPLEMENTATION STATUS ===\n');
    
    // Overall summary
    const overallStatus = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        -- Phase 1: Return Metrics
        COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) as phase1_6m,
        COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) as phase1_3y,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as phase1_5y,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as phase1_ytd,
        -- Phase 2: Risk Metrics
        COUNT(CASE WHEN std_dev_1y_score IS NOT NULL THEN 1 END) as phase2_volatility,
        COUNT(CASE WHEN max_drawdown_score IS NOT NULL THEN 1 END) as phase2_drawdown,
        -- Phase 3: Advanced Ratios
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as phase3_sharpe,
        COUNT(CASE WHEN beta_score IS NOT NULL THEN 1 END) as phase3_beta,
        -- Phase 4: Quality Metrics
        COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as phase4_consistency,
        COUNT(CASE WHEN overall_rating IS NOT NULL THEN 1 END) as phase4_rating,
        COUNT(CASE WHEN expense_ratio_score IS NOT NULL THEN 1 END) as phase4_expense,
        -- Phase 5: Rankings
        COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as phase5_subcategory,
        COUNT(CASE WHEN category_rank IS NOT NULL THEN 1 END) as phase5_category
      FROM fund_scores
    `);
    
    const stats = overallStatus.rows[0];
    
    console.log('PHASE 1: RETURN METRICS EXPANSION');
    console.log(`✓ 6-Month Returns: ${stats.phase1_6m} funds (${(stats.phase1_6m/stats.total_funds*100).toFixed(2)}%)`);
    console.log(`✓ 3-Year Returns: ${stats.phase1_3y} funds (${(stats.phase1_3y/stats.total_funds*100).toFixed(2)}%)`);
    console.log(`✓ 5-Year Returns: ${stats.phase1_5y} funds (${(stats.phase1_5y/stats.total_funds*100).toFixed(2)}%)`);
    console.log(`✓ YTD Returns: ${stats.phase1_ytd} funds (${(stats.phase1_ytd/stats.total_funds*100).toFixed(2)}%)`);
    
    console.log('\nPHASE 2: RISK & VOLATILITY METRICS');
    console.log(`✓ Volatility Analysis: ${stats.phase2_volatility} funds (${(stats.phase2_volatility/stats.total_funds*100).toFixed(2)}%)`);
    console.log(`✓ Drawdown Analysis: ${stats.phase2_drawdown} funds (${(stats.phase2_drawdown/stats.total_funds*100).toFixed(2)}%)`);
    
    console.log('\nPHASE 3: ADVANCED FINANCIAL RATIOS');
    console.log(`⚡ Sharpe Ratios: ${stats.phase3_sharpe} funds (${(stats.phase3_sharpe/stats.total_funds*100).toFixed(2)}%) - IN PROGRESS`);
    console.log(`⚡ Beta Calculations: ${stats.phase3_beta} funds (${(stats.phase3_beta/stats.total_funds*100).toFixed(2)}%) - IN PROGRESS`);
    
    console.log('\nPHASE 4: QUALITY & PERFORMANCE METRICS');
    console.log(`⚡ Consistency Scores: ${stats.phase4_consistency} funds (${(stats.phase4_consistency/stats.total_funds*100).toFixed(2)}%) - IN PROGRESS`);
    console.log(`⚡ Overall Ratings: ${stats.phase4_rating} funds (${(stats.phase4_rating/stats.total_funds*100).toFixed(2)}%) - IN PROGRESS`);
    console.log(`✓ Expense Ratio Scores: ${stats.phase4_expense} funds (${(stats.phase4_expense/stats.total_funds*100).toFixed(2)}%)`);
    
    console.log('\nPHASE 5: CATEGORY-BASED QUARTILE RANKING');
    console.log(`✓ Subcategory Rankings: ${stats.phase5_subcategory} funds (${(stats.phase5_subcategory/stats.total_funds*100).toFixed(2)}%)`);
    console.log(`✓ Category Rankings: ${stats.phase5_category} funds (${(stats.phase5_category/stats.total_funds*100).toFixed(2)}%)`);
    
    // Data availability analysis
    console.log('\n=== AUTHENTIC DATA AVAILABILITY ANALYSIS ===');
    
    const dataAvailability = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN nav_18m >= 250 THEN 1 END) as high_quality_data,
        COUNT(CASE WHEN nav_18m >= 150 THEN 1 END) as sufficient_data,
        COUNT(CASE WHEN nav_18m >= 50 THEN 1 END) as minimal_data,
        COUNT(CASE WHEN nav_18m < 50 OR nav_18m IS NULL THEN 1 END) as insufficient_data,
        ROUND(AVG(nav_18m), 0) as avg_nav_records
      FROM (
        SELECT fs.fund_id,
               COUNT(nd.nav_value) as nav_18m
        FROM fund_scores fs
        LEFT JOIN nav_data nd ON fs.fund_id = nd.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '18 months'
          AND nd.nav_value IS NOT NULL
          AND nd.nav_value > 0
        GROUP BY fs.fund_id
      ) nav_analysis
    `);
    
    const dataStats = dataAvailability.rows[0];
    
    console.log(`Total Funds: ${dataStats.total_funds}`);
    console.log(`High Quality Data (250+ records): ${dataStats.high_quality_data} funds (${(dataStats.high_quality_data/dataStats.total_funds*100).toFixed(1)}%)`);
    console.log(`Sufficient Data (150+ records): ${dataStats.sufficient_data} funds (${(dataStats.sufficient_data/dataStats.total_funds*100).toFixed(1)}%)`);
    console.log(`Minimal Data (50+ records): ${dataStats.minimal_data} funds (${(dataStats.minimal_data/dataStats.total_funds*100).toFixed(1)}%)`);
    console.log(`Insufficient Data (<50 records): ${dataStats.insufficient_data} funds (${(dataStats.insufficient_data/dataStats.total_funds*100).toFixed(1)}%)`);
    console.log(`Average NAV Records per Fund: ${dataStats.avg_nav_records}`);
    
    // Identify specific data gaps by category
    console.log('\n=== DATA GAPS BY CATEGORY ===');
    
    const categoryGaps = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_funds,
        COUNT(CASE WHEN nav_count >= 150 THEN 1 END) as funds_with_sufficient_data,
        COUNT(CASE WHEN nav_count < 150 THEN 1 END) as data_gap_funds,
        ROUND(COUNT(CASE WHEN nav_count < 150 THEN 1 END) * 100.0 / COUNT(*), 1) as gap_percentage
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '18 months'
        GROUP BY fund_id
      ) nav_counts ON f.id = nav_counts.fund_id
      GROUP BY f.category
      ORDER BY gap_percentage DESC
    `);
    
    categoryGaps.rows.forEach(cat => {
      console.log(`${cat.category}: ${cat.data_gap_funds}/${cat.total_funds} funds with insufficient data (${cat.gap_percentage}%)`);
    });
    
    // Next steps recommendation
    console.log('\n=== RECOMMENDED NEXT STEPS ===');
    
    if (stats.phase3_sharpe < 1000) {
      console.log('1. Continue Phase 3: Advanced Ratios implementation (currently processing)');
    }
    
    if (stats.phase4_consistency < 5000) {
      console.log('2. Expand Phase 4: Quality Metrics for funds with sufficient data');
    }
    
    if (dataStats.insufficient_data > 1000) {
      console.log('3. Data Gap Mitigation: Request additional NAV data sources for funds with <50 records');
    }
    
    console.log('4. Continuous monitoring of ongoing phase implementations');
    console.log('5. Validate scoring accuracy for funds with complete metrics');
    
    return {
      overallStats: stats,
      dataAvailability: dataStats,
      categoryGaps: categoryGaps.rows
    };
    
  } catch (error) {
    console.error('Error generating phase status report:', error);
    return null;
  } finally {
    await pool.end();
  }
}

generateCompletePhaseStatusReport();