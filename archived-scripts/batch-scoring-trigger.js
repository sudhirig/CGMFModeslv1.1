/**
 * Batch Quartile Scoring for All Eligible Funds
 * Processes funds with 252+ NAV records (approximately 1 year of data)
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

async function batchQuartileScoring() {
  try {
    console.log('Starting batch quartile scoring for eligible funds...');
    
    // Get all funds with sufficient historical data
    const eligibleFunds = await pool.query(`
      SELECT f.id, f.fund_name, f.category, COUNT(*) as nav_records
      FROM funds f 
      JOIN nav_data n ON f.id = n.fund_id 
      WHERE f.status = 'ACTIVE'
      GROUP BY f.id, f.fund_name, f.category 
      HAVING COUNT(*) >= 252
      ORDER BY COUNT(*) DESC
      LIMIT 100
    `);
    
    console.log(`Found ${eligibleFunds.rows.length} eligible funds for initial batch`);
    
    let processedCount = 0;
    let errorCount = 0;
    
    for (const fund of eligibleFunds.rows) {
      try {
        console.log(`Processing fund ${fund.id}: ${fund.fund_name}`);
        
        // Get NAV data for calculations
        const navData = await pool.query(`
          SELECT nav_value, nav_date 
          FROM nav_data 
          WHERE fund_id = $1 
          ORDER BY nav_date DESC 
          LIMIT 1000
        `, [fund.id]);
        
        if (navData.rows.length >= 252) {
          // Calculate basic performance metrics
          const navValues = navData.rows.map(n => parseFloat(n.nav_value));
          const dates = navData.rows.map(n => new Date(n.nav_date));
          
          // 1-year return calculation
          const currentNav = navValues[0];
          const yearAgoNav = navValues[251] || navValues[navValues.length - 1];
          const return1y = ((currentNav - yearAgoNav) / yearAgoNav) * 100;
          
          // Simple volatility calculation (standard deviation)
          const returns = [];
          for (let i = 1; i < navValues.length && i < 252; i++) {
            const dailyReturn = (navValues[i-1] - navValues[i]) / navValues[i];
            returns.push(dailyReturn);
          }
          
          const avgReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
          const variance = returns.reduce((sum, ret) => sum + Math.pow(ret - avgReturn, 2), 0) / returns.length;
          const volatility = Math.sqrt(variance) * Math.sqrt(252) * 100; // Annualized
          
          // Simple scoring logic
          let totalScore = 50; // Base score
          
          // Return score (0-25 points)
          if (return1y > 15) totalScore += 25;
          else if (return1y > 10) totalScore += 20;
          else if (return1y > 5) totalScore += 15;
          else if (return1y > 0) totalScore += 10;
          else totalScore += 5;
          
          // Volatility score (0-25 points) - lower is better
          if (volatility < 5) totalScore += 25;
          else if (volatility < 10) totalScore += 20;
          else if (volatility < 15) totalScore += 15;
          else if (volatility < 20) totalScore += 10;
          else totalScore += 5;
          
          // Determine quartile based on total score
          let quartile;
          if (totalScore >= 85) quartile = 1;
          else if (totalScore >= 70) quartile = 2;
          else if (totalScore >= 55) quartile = 3;
          else quartile = 4;
          
          // Determine recommendation
          let recommendation;
          if (quartile === 1) recommendation = 'STRONG_BUY';
          else if (quartile === 2) recommendation = 'BUY';
          else if (quartile === 3) recommendation = 'HOLD';
          else recommendation = 'SELL';
          
          // Insert score into database
          await pool.query(`
            INSERT INTO fund_scores (
              fund_id, score_date, return_1y_score, total_score, quartile, recommendation
            ) VALUES ($1, CURRENT_DATE, $2, $3, $4, $5)
            ON CONFLICT (fund_id, score_date) 
            DO UPDATE SET 
              return_1y_score = EXCLUDED.return_1y_score,
              total_score = EXCLUDED.total_score,
              quartile = EXCLUDED.quartile,
              recommendation = EXCLUDED.recommendation
          `, [fund.id, return1y, totalScore, quartile, recommendation]);
          
          processedCount++;
          
          if (processedCount % 10 === 0) {
            console.log(`Processed ${processedCount} funds so far...`);
          }
        }
        
      } catch (error) {
        console.error(`Error processing fund ${fund.id}: ${error.message}`);
        errorCount++;
      }
    }
    
    console.log(`Batch scoring completed:`);
    console.log(`- Successfully processed: ${processedCount} funds`);
    console.log(`- Errors encountered: ${errorCount} funds`);
    
    // Get summary statistics
    const summary = await pool.query(`
      SELECT 
        COUNT(*) as total_scores,
        COUNT(DISTINCT fund_id) as unique_funds,
        AVG(total_score) as avg_score,
        COUNT(CASE WHEN quartile = 1 THEN 1 END) as quartile_1_count,
        COUNT(CASE WHEN quartile = 2 THEN 1 END) as quartile_2_count,
        COUNT(CASE WHEN quartile = 3 THEN 1 END) as quartile_3_count,
        COUNT(CASE WHEN quartile = 4 THEN 1 END) as quartile_4_count
      FROM fund_scores
    `);
    
    console.log('Quartile distribution:', summary.rows[0]);
    
  } catch (error) {
    console.error('Batch scoring error:', error);
  } finally {
    await pool.end();
    process.exit(0);
  }
}

batchQuartileScoring();