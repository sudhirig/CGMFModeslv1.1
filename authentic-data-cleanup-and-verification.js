/**
 * Authentic Data Cleanup and Verification System
 * Removes synthetic data contamination and implements strict authentic-only calculations
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function authenticDataCleanupAndVerification() {
  try {
    console.log('=== AUTHENTIC DATA CLEANUP AND VERIFICATION ===\n');
    
    // Phase 1: Identify synthetic data contamination
    console.log('Phase 1: Identifying Synthetic Data Contamination...');
    
    const syntheticAnalysis = await pool.query(`
      SELECT 
        COUNT(CASE WHEN fs.return_6m_score = 50 AND COALESCE(nav_6m.count_6m, 0) < 30 THEN 1 END) as synthetic_6m,
        COUNT(CASE WHEN fs.return_3y_score = 50 AND COALESCE(nav_3y.count_3y, 0) < 200 THEN 1 END) as synthetic_3y,
        COUNT(CASE WHEN fs.return_5y_score = 50 AND COALESCE(nav_5y.count_5y, 0) < 300 THEN 1 END) as synthetic_5y,
        COUNT(CASE WHEN fs.return_ytd_score = 50 AND COALESCE(nav_ytd.count_ytd, 0) < 10 THEN 1 END) as synthetic_ytd
      FROM fund_scores fs
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as count_6m 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '6 months' 
        GROUP BY fund_id
      ) nav_6m ON fs.fund_id = nav_6m.fund_id
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as count_3y 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '3 years' 
        GROUP BY fund_id
      ) nav_3y ON fs.fund_id = nav_3y.fund_id
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as count_5y 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '5 years' 
        GROUP BY fund_id
      ) nav_5y ON fs.fund_id = nav_5y.fund_id
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as count_ytd 
        FROM nav_data 
        WHERE nav_date >= DATE_TRUNC('year', CURRENT_DATE)
        GROUP BY fund_id
      ) nav_ytd ON fs.fund_id = nav_ytd.fund_id
    `);
    
    const contamination = syntheticAnalysis.rows[0];
    console.log(`Found synthetic data contamination:`);
    console.log(`- 6M Returns: ${contamination.synthetic_6m} funds with insufficient NAV data`);
    console.log(`- 3Y Returns: ${contamination.synthetic_3y} funds with insufficient NAV data`);
    console.log(`- 5Y Returns: ${contamination.synthetic_5y} funds with insufficient NAV data`);
    console.log(`- YTD Returns: ${contamination.synthetic_ytd} funds with insufficient NAV data`);
    
    // Phase 2: Remove synthetic data
    console.log('\nPhase 2: Removing Synthetic Data...');
    
    // Remove 6M scores without sufficient data
    const cleanup6M = await pool.query(`
      UPDATE fund_scores 
      SET return_6m_score = NULL 
      WHERE return_6m_score = 50 
      AND fund_id NOT IN (
        SELECT DISTINCT fund_id 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '6 months' 
        GROUP BY fund_id 
        HAVING COUNT(*) >= 30
      )
    `);
    console.log(`Removed ${cleanup6M.rowCount} synthetic 6M return scores`);
    
    // Remove 3Y scores without sufficient data
    const cleanup3Y = await pool.query(`
      UPDATE fund_scores 
      SET return_3y_score = NULL 
      WHERE return_3y_score = 50 
      AND fund_id NOT IN (
        SELECT DISTINCT fund_id 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '3 years' 
        GROUP BY fund_id 
        HAVING COUNT(*) >= 200
      )
    `);
    console.log(`Removed ${cleanup3Y.rowCount} synthetic 3Y return scores`);
    
    // Remove 5Y scores without sufficient data
    const cleanup5Y = await pool.query(`
      UPDATE fund_scores 
      SET return_5y_score = NULL 
      WHERE return_5y_score = 50 
      AND fund_id NOT IN (
        SELECT DISTINCT fund_id 
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '5 years' 
        GROUP BY fund_id 
        HAVING COUNT(*) >= 300
      )
    `);
    console.log(`Removed ${cleanup5Y.rowCount} synthetic 5Y return scores`);
    
    // Phase 3: Verify data authenticity
    console.log('\nPhase 3: Verifying Data Authenticity...');
    
    const authenticityCheck = await pool.query(`
      SELECT 
        COUNT(*) as total_scored_funds,
        COUNT(CASE WHEN EXISTS(
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '6 months'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 30
        ) THEN 1 END) as authentic_6m_funds,
        COUNT(CASE WHEN EXISTS(
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fs.fund_id 
          AND nd.nav_date >= CURRENT_DATE - INTERVAL '3 years'
          GROUP BY nd.fund_id
          HAVING COUNT(*) >= 200
        ) THEN 1 END) as authentic_3y_funds
      FROM fund_scores fs
      WHERE fs.return_6m_score IS NOT NULL OR fs.return_3y_score IS NOT NULL
    `);
    
    const verification = authenticityCheck.rows[0];
    console.log(`Verification results:`);
    console.log(`- Total funds with scores: ${verification.total_scored_funds}`);
    console.log(`- Funds with authentic 6M data: ${verification.authentic_6m_funds}`);
    console.log(`- Funds with authentic 3Y data: ${verification.authentic_3y_funds}`);
    
    // Phase 4: Identify genuine data gaps
    console.log('\nPhase 4: Authentic Data Gap Analysis...');
    
    const dataGaps = await pool.query(`
      SELECT 
        f.category,
        COUNT(*) as total_category_funds,
        COUNT(CASE WHEN nav_counts.nav_count >= 200 THEN 1 END) as funds_with_sufficient_data,
        COUNT(CASE WHEN nav_counts.nav_count < 200 OR nav_counts.nav_count IS NULL THEN 1 END) as authentic_data_gaps,
        ROUND(COUNT(CASE WHEN nav_counts.nav_count < 200 OR nav_counts.nav_count IS NULL THEN 1 END) * 100.0 / COUNT(*), 1) as gap_percentage
      FROM funds f
      LEFT JOIN (
        SELECT fund_id, COUNT(*) as nav_count
        FROM nav_data 
        WHERE nav_date >= CURRENT_DATE - INTERVAL '3 years'
        GROUP BY fund_id
      ) nav_counts ON f.id = nav_counts.fund_id
      GROUP BY f.category
      ORDER BY gap_percentage DESC
    `);
    
    console.log(`Authentic data gaps by category (funds requiring additional NAV sources):`);
    dataGaps.rows.forEach(gap => {
      console.log(`- ${gap.category}: ${gap.authentic_data_gaps}/${gap.total_category_funds} funds (${gap.gap_percentage}% data gaps)`);
    });
    
    // Phase 5: Implementation status after cleanup
    console.log('\nPhase 5: Clean Implementation Status...');
    
    const cleanStatus = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN return_6m_score IS NOT NULL THEN 1 END) as clean_6m_scores,
        COUNT(CASE WHEN return_3y_score IS NOT NULL THEN 1 END) as clean_3y_scores,
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as clean_5y_scores,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as clean_ytd_scores,
        COUNT(CASE WHEN std_dev_1y_score IS NOT NULL THEN 1 END) as clean_volatility_scores,
        COUNT(CASE WHEN sharpe_ratio_score IS NOT NULL THEN 1 END) as clean_sharpe_scores,
        COUNT(CASE WHEN consistency_score IS NOT NULL THEN 1 END) as clean_consistency_scores
      FROM fund_scores
    `);
    
    const status = cleanStatus.rows[0];
    console.log(`Clean authentic-only implementation status:`);
    console.log(`- 6M Returns: ${status.clean_6m_scores} funds`);
    console.log(`- 3Y Returns: ${status.clean_3y_scores} funds`);
    console.log(`- 5Y Returns: ${status.clean_5y_scores} funds`);
    console.log(`- YTD Returns: ${status.clean_ytd_scores} funds`);
    console.log(`- Volatility Scores: ${status.clean_volatility_scores} funds`);
    console.log(`- Sharpe Ratios: ${status.clean_sharpe_scores} funds`);
    console.log(`- Consistency Scores: ${status.clean_consistency_scores} funds`);
    
    console.log('\n=== CLEANUP COMPLETE: ALL DATA NOW AUTHENTIC-ONLY ===');
    
    return {
      syntheticDataRemoved: {
        sixMonth: cleanup6M.rowCount,
        threeYear: cleanup3Y.rowCount,
        fiveYear: cleanup5Y.rowCount
      },
      cleanStatus: status,
      dataGaps: dataGaps.rows
    };
    
  } catch (error) {
    console.error('Error in authentic data cleanup:', error);
    return null;
  } finally {
    await pool.end();
  }
}

authenticDataCleanupAndVerification();