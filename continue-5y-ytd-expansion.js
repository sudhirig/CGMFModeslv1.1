/**
 * Continue 5Y and YTD Authentic Expansion
 * Resumes background processing for eligible funds with sufficient historical data
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function continue5YAndYTDExpansion() {
  try {
    console.log('=== CONTINUING 5Y AND YTD AUTHENTIC EXPANSION ===\n');
    
    // Process missing 5Y calculations
    console.log('Processing missing 5Y calculations...');
    const missing5Y = await pool.query(`
      SELECT DISTINCT fs.fund_id, f.category
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.return_5y_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = fs.fund_id 
        AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
        GROUP BY nd.fund_id 
        HAVING COUNT(*) >= 300
      )
      ORDER BY fs.fund_id
      LIMIT 18
    `);
    
    let processed5Y = 0;
    for (const fund of missing5Y.rows) {
      if (await calculate5YReturn(fund)) {
        processed5Y++;
        console.log(`5Y calculation completed for fund ${fund.fund_id}`);
      }
    }
    
    // Process missing YTD calculations
    console.log('\nProcessing missing YTD calculations...');
    const missingYTD = await pool.query(`
      SELECT DISTINCT fs.fund_id, f.category
      FROM fund_scores fs
      JOIN funds f ON fs.fund_id = f.id
      WHERE fs.return_ytd_score IS NULL
      AND EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = fs.fund_id 
        AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
        GROUP BY nd.fund_id 
        HAVING COUNT(*) >= 30
      )
      ORDER BY fs.fund_id
      LIMIT 40
    `);
    
    let processedYTD = 0;
    for (const fund of missingYTD.rows) {
      if (await calculateYTDReturn(fund)) {
        processedYTD++;
        console.log(`YTD calculation completed for fund ${fund.fund_id}`);
      }
    }
    
    // Final status check
    const finalStatus = await pool.query(`
      SELECT 
        COUNT(CASE WHEN return_5y_score IS NOT NULL THEN 1 END) as final_5y_coverage,
        COUNT(CASE WHEN return_ytd_score IS NOT NULL THEN 1 END) as final_ytd_coverage,
        COUNT(CASE WHEN EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fund_scores.fund_id 
          AND nd.nav_date <= CURRENT_DATE - INTERVAL '5 years'
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 300
        ) AND return_5y_score IS NULL THEN 1 END) as remaining_5y_eligible,
        COUNT(CASE WHEN EXISTS (
          SELECT 1 FROM nav_data nd 
          WHERE nd.fund_id = fund_scores.fund_id 
          AND nd.nav_date >= DATE_TRUNC('year', CURRENT_DATE)
          GROUP BY nd.fund_id 
          HAVING COUNT(*) >= 30
        ) AND return_ytd_score IS NULL THEN 1 END) as remaining_ytd_eligible
      FROM fund_scores
    `);
    
    const final = finalStatus.rows[0];
    
    console.log('\n=== EXPANSION RESULTS ===');
    console.log(`5Y calculations added: ${processed5Y}`);
    console.log(`YTD calculations added: ${processedYTD}`);
    console.log(`Total new authentic calculations: ${processed5Y + processedYTD}`);
    
    console.log('\nUpdated Coverage:');
    console.log(`- 5Y Returns: ${final.final_5y_coverage} funds`);
    console.log(`- YTD Returns: ${final.final_ytd_coverage} funds`);
    
    console.log('\nRemaining Opportunities:');
    console.log(`- 5Y eligible for processing: ${final.remaining_5y_eligible} funds`);
    console.log(`- YTD eligible for processing: ${final.remaining_ytd_eligible} funds`);
    
    return {
      success: true,
      added5Y: processed5Y,
      addedYTD: processedYTD,
      final5Y: final.final_5y_coverage,
      finalYTD: final.final_ytd_coverage,
      remaining5Y: final.remaining_5y_eligible,
      remainingYTD: final.remaining_ytd_eligible
    };
    
  } catch (error) {
    console.error('Error in 5Y/YTD expansion:', error);
    return { success: false, error: error.message };
  } finally {
    await pool.end();
  }
}

async function calculate5YReturn(fund) {
  try {
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
      AND nav_date >= CURRENT_DATE - INTERVAL '5 years 6 months'
      AND nav_value > 0
      ORDER BY nav_date
    `, [fund.fund_id]);
    
    if (navData.rows.length < 250) return false;
    
    const fiveYearsAgo = new Date();
    fiveYearsAgo.setFullYear(fiveYearsAgo.getFullYear() - 5);
    
    const startNav = navData.rows.find(row => new Date(row.nav_date) <= fiveYearsAgo);
    const endNav = navData.rows[navData.rows.length - 1];
    
    if (!startNav || !endNav) return false;
    
    const return5Y = ((parseFloat(endNav.nav_value) - parseFloat(startNav.nav_value)) / parseFloat(startNav.nav_value)) * 100;
    
    if (!isFinite(return5Y)) return false;
    
    // Authentic scoring based on performance ranges
    const score = return5Y >= 25 ? 100 : return5Y >= 20 ? 95 : return5Y >= 15 ? 85 : 
                 return5Y >= 10 ? 75 : return5Y >= 5 ? 65 : return5Y >= 0 ? 55 : 
                 return5Y >= -5 ? 45 : return5Y >= -10 ? 35 : 25;
    
    await pool.query(`
      UPDATE fund_scores 
      SET return5y = $2, return_5y_score = $3, score_date = CURRENT_DATE
      WHERE fund_id = $1
    `, [fund.fund_id, return5Y.toFixed(2), score]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

async function calculateYTDReturn(fund) {
  try {
    const navData = await pool.query(`
      SELECT nav_value, nav_date
      FROM nav_data 
      WHERE fund_id = $1 
      AND nav_date >= DATE_TRUNC('year', CURRENT_DATE) - INTERVAL '2 weeks'
      AND nav_value > 0
      ORDER BY nav_date
    `, [fund.fund_id]);
    
    if (navData.rows.length < 20) return false;
    
    const yearStart = new Date(new Date().getFullYear(), 0, 1);
    const startNav = navData.rows.find(row => new Date(row.nav_date) >= yearStart);
    const endNav = navData.rows[navData.rows.length - 1];
    
    if (!startNav || !endNav) return false;
    
    const returnYTD = ((parseFloat(endNav.nav_value) - parseFloat(startNav.nav_value)) / parseFloat(startNav.nav_value)) * 100;
    
    if (!isFinite(returnYTD)) return false;
    
    // Authentic YTD scoring
    const score = returnYTD >= 20 ? 100 : returnYTD >= 15 ? 90 : returnYTD >= 10 ? 80 : 
                 returnYTD >= 5 ? 70 : returnYTD >= 0 ? 60 : returnYTD >= -5 ? 50 : 
                 returnYTD >= -10 ? 40 : 30;
    
    await pool.query(`
      UPDATE fund_scores 
      SET return_ytd_score = $2, score_date = CURRENT_DATE
      WHERE fund_id = $1
    `, [fund.fund_id, score]);
    
    return true;
    
  } catch (error) {
    return false;
  }
}

continue5YAndYTDExpansion();