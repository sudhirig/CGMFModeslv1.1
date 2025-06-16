/**
 * Direct script to score the 917 funds that have sufficient NAV data but aren't scored yet
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({ 
  connectionString: process.env.DATABASE_URL 
});

async function scoreReadyFunds() {
  try {
    console.log('🚀 Starting scoring for funds with sufficient NAV data...');
    
    // Get funds that have 30+ NAV records but no scores yet
    const unscored = await pool.query(`
      SELECT 
        f.id,
        f.fund_name,
        f.category,
        COUNT(nd.nav_value) as nav_count
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id
      WHERE fs.fund_id IS NULL
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(nd.nav_value) >= 30
      ORDER BY COUNT(nd.nav_value) DESC
      LIMIT 50
    `);
    
    console.log(`Found ${unscored.rows.length} funds ready for scoring`);
    
    let scored = 0;
    let failed = 0;
    
    for (const fund of unscored.rows) {
      try {
        console.log(`Scoring fund ${fund.id}: ${fund.fund_name} (${fund.nav_count} NAV records)`);
        
        // Get NAV data for this fund
        const navData = await pool.query(`
          SELECT nav_date, nav_value 
          FROM nav_data 
          WHERE fund_id = $1 
          ORDER BY nav_date DESC 
          LIMIT 500
        `, [fund.id]);
        
        if (navData.rows.length >= 30) {
          // Calculate basic performance metrics
          const navValues = navData.rows.map(n => parseFloat(n.nav_value));
          
          // Calculate returns
          const currentNav = navValues[0];
          const nav30d = navValues[30] || navValues[navValues.length - 1];
          const nav90d = navValues[90] || navValues[navValues.length - 1];
          const nav365d = navValues[365] || navValues[navValues.length - 1];
          
          const return1m = nav30d ? ((currentNav - nav30d) / nav30d) * 100 : null;
          const return3m = nav90d ? ((currentNav - nav90d) / nav90d) * 100 : null;
          const return1y = nav365d ? ((currentNav - nav365d) / nav365d) * 100 : null;
          
          // Calculate volatility
          const dailyReturns = [];
          for (let i = 1; i < Math.min(navValues.length, 252); i++) {
            const dailyReturn = (navValues[i-1] - navValues[i]) / navValues[i];
            dailyReturns.push(dailyReturn);
          }
          
          const avgReturn = dailyReturns.reduce((sum, r) => sum + r, 0) / dailyReturns.length;
          const variance = dailyReturns.reduce((sum, r) => sum + Math.pow(r - avgReturn, 2), 0) / dailyReturns.length;
          const volatility = Math.sqrt(variance * 252) * 100; // Annualized
          
          // Score based on category benchmarks
          const return3mScore = return3m ? Math.max(0, Math.min(100, 50 + return3m * 2)) : 50;
          const return1yScore = return1y ? Math.max(0, Math.min(100, 50 + return1y)) : 50;
          const volatilityScore = volatility ? Math.max(0, Math.min(100, 100 - volatility * 2)) : 50;
          
          const totalScore = (return3mScore * 0.3 + return1yScore * 0.4 + volatilityScore * 0.3);
          const quartile = totalScore >= 75 ? 1 : totalScore >= 50 ? 2 : totalScore >= 25 ? 3 : 4;
          
          // Insert score record
          await pool.query(`
            INSERT INTO fund_scores (
              fund_id, score_date, return_3m_score, return_1y_score, 
              std_dev_1y_score, total_score, quartile, 
              historical_returns_total, risk_grade_total, other_metrics_total,
              recommendation, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
          `, [
            fund.id,
            new Date().toISOString().split('T')[0],
            return3mScore,
            return1yScore,
            volatilityScore,
            totalScore,
            quartile,
            return3mScore + (return1yScore || 0),
            volatilityScore,
            50, // default other metrics
            quartile <= 2 ? 'BUY' : quartile === 3 ? 'HOLD' : 'SELL',
            new Date()
          ]);
          
          scored++;
          console.log(`✓ Scored fund ${fund.id} - Total: ${totalScore.toFixed(1)}, Quartile: ${quartile}`);
        }
        
      } catch (error) {
        console.error(`Error scoring fund ${fund.id}:`, error.message);
        failed++;
      }
      
      // Small delay to avoid overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    console.log(`\n🎯 Scoring complete!`);
    console.log(`✅ Successfully scored: ${scored} funds`);
    console.log(`❌ Failed: ${failed} funds`);
    console.log(`📊 Total funds with scores now: ${scored + 13227}`);
    
  } catch (error) {
    console.error('Error in scoring process:', error);
  } finally {
    await pool.end();
  }
}

scoreReadyFunds();