/**
 * Test quartile scoring system with schema fix
 */
import { db, pool } from './server/db.js';
import { funds, navData } from './shared/schema.js';
import { eq } from 'drizzle-orm';

async function testQuartileScoring() {
  try {
    console.log('Testing quartile scoring for SBI OVERNIGHT FUND (ID: 6895)...');
    
    // Get fund details
    const fund = await db.select().from(funds).where(eq(funds.id, 6895)).limit(1);
    if (!fund.length) {
      console.log('Fund not found');
      return;
    }
    
    console.log(`Fund: ${fund[0].fundName}`);
    console.log(`Category: ${fund[0].category}`);
    
    // Get NAV data count
    const navCount = await db.select().from(navData).where(eq(navData.fundId, 6895));
    console.log(`NAV records available: ${navCount.length}`);
    
    // Calculate basic metrics for scoring test
    if (navCount.length >= 365) {
      // Get recent NAV data (last 1 year)
      const recentNavs = navCount
        .sort((a, b) => new Date(b.navDate) - new Date(a.navDate))
        .slice(0, 252); // Approximately 1 year of trading days
      
      if (recentNavs.length >= 252) {
        const startNav = parseFloat(recentNavs[recentNavs.length - 1].navValue);
        const endNav = parseFloat(recentNavs[0].navValue);
        const return1y = ((endNav - startNav) / startNav) * 100;
        
        console.log(`1-year return: ${return1y.toFixed(2)}%`);
        
        // Test inserting a score record
        const scoreDate = new Date().toISOString().split('T')[0];
        
        console.log('Testing score insertion with fixed schema...');
        
        const testScore = {
          fund_id: 6895,
          score_date: scoreDate,
          return_1y_score: return1y > 0 ? 8 : 5,
          total_score: 75,
          quartile: 2,
          recommendation: 'HOLD'
        };
        
        await pool.query(`
          INSERT INTO fund_scores (
            fund_id, score_date, return_1y_score, total_score, quartile, recommendation
          ) VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (fund_id, score_date) 
          DO UPDATE SET 
            return_1y_score = EXCLUDED.return_1y_score,
            total_score = EXCLUDED.total_score,
            quartile = EXCLUDED.quartile,
            recommendation = EXCLUDED.recommendation
        `, [
          testScore.fund_id,
          testScore.score_date,
          testScore.return_1y_score,
          testScore.total_score,
          testScore.quartile,
          testScore.recommendation
        ]);
        
        console.log('✅ Score inserted successfully!');
        
        // Verify the score was saved
        const savedScore = await pool.query(`
          SELECT * FROM fund_scores WHERE fund_id = $1 AND score_date = $2
        `, [6895, scoreDate]);
        
        if (savedScore.rows.length > 0) {
          console.log('✅ Score verification successful!');
          console.log('Saved score:', savedScore.rows[0]);
        } else {
          console.log('❌ Score verification failed');
        }
        
      } else {
        console.log('Insufficient recent NAV data for scoring');
      }
    } else {
      console.log('Insufficient total NAV data for scoring');
    }
    
  } catch (error) {
    console.error('Error in quartile scoring test:', error);
  } finally {
    process.exit(0);
  }
}

testQuartileScoring();