import express from 'express';
import { FundScoringEngine } from '../services/batch-quartile-scoring';
import { executeRawQuery } from '../db';

const router = express.Router();

/**
 * This endpoint manually triggers the quartile scoring process
 * after historical NAV data has been generated
 */
router.post('/start', async (req, res) => {
  try {
    // First, check if we have funds with sufficient historical NAV data
    const fundsWithNavQuery = `
      SELECT 
        f.id as fund_id, 
        f.fund_name, 
        f.category,
        COUNT(n.nav_value) as nav_count
      FROM 
        funds f
      JOIN 
        nav_data n ON f.id = n.fund_id
      GROUP BY 
        f.id, f.fund_name, f.category
      HAVING 
        COUNT(n.nav_value) > 30
    `;
    
    const fundsWithNavResult = await executeRawQuery(fundsWithNavQuery);
    const fundsWithNav = fundsWithNavResult.rows;
    
    if (fundsWithNav.length === 0) {
      return res.status(200).json({
        success: false,
        message: 'No funds with sufficient historical NAV data found yet',
        fundsWithNav: 0
      });
    }
    
    console.log(`Found ${fundsWithNav.length} funds with sufficient historical NAV data for scoring`);
    
    // Create a new instance of the scoring engine and start the scoring process
    const scoringEngine = new FundScoringEngine();
    
    // Extract fund IDs and pass them to the processing function
    const fundIds = fundsWithNav.map(f => f.fund_id);
    console.log(`Starting batch scoring for ${fundIds.length} funds with historical NAV data`);
    
    // Start the quartile scoring process in the background - will process in batches
    // This will score funds that have sufficient NAV data
    const scoringPromise = scoringEngine.batchScoreFunds(500);
    
    // Return immediately so the client doesn't have to wait
    res.status(200).json({
      success: true,
      message: `Started quartile scoring for ${fundsWithNav.length} funds with real NAV data`,
      fundsWithNav: fundsWithNav.length
    });
    
    // Continue processing in the background
    scoringPromise.then(result => {
      console.log(`Quartile scoring completed successfully: ${result.scoredFunds} funds scored`);
    }).catch(error => {
      console.error('Error in background quartile scoring:', error);
    });
  } catch (error: any) {
    console.error('Error triggering quartile scoring:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to trigger quartile scoring: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

/**
 * This endpoint checks the status of funds with quartile scores
 * based on real historical NAV data
 */
router.get('/status', async (req, res) => {
  try {
    // Get the count of funds with real quartile scores (non-zero component scores)
    const realScoredFundsQuery = `
      SELECT 
        COUNT(*) as total_funds,
        SUM(CASE WHEN fs.quartile IS NOT NULL THEN 1 ELSE 0 END) as scored_funds,
        SUM(CASE WHEN fs.quartile = 1 THEN 1 ELSE 0 END) as q1_funds,
        SUM(CASE WHEN fs.quartile = 2 THEN 1 ELSE 0 END) as q2_funds,
        SUM(CASE WHEN fs.quartile = 3 THEN 1 ELSE 0 END) as q3_funds,
        SUM(CASE WHEN fs.quartile = 4 THEN 1 ELSE 0 END) as q4_funds,
        SUM(CASE WHEN fs.return_3m_score > 0 OR fs.return_6m_score > 0 OR fs.return_1y_score > 0 THEN 1 ELSE 0 END) as real_scored_funds
      FROM funds f
      LEFT JOIN fund_scores fs ON f.id = fs.fund_id
    `;
    
    const scoredFundsResult = await executeRawQuery(realScoredFundsQuery);
    const scoringStats = scoredFundsResult.rows[0];
    
    // Get a sample of recently scored funds with real data
    const recentlyScoredQuery = `
      SELECT 
        f.id, 
        f.fund_name, 
        f.category,
        fs.quartile,
        fs.total_score,
        fs.return_1y_score,
        fs.return_3y_score,
        fs.risk_score,
        fs.sharpe_ratio_score,
        fs.consistency_score
      FROM 
        funds f
      JOIN 
        fund_scores fs ON f.id = fs.fund_id
      WHERE 
        fs.return_3m_score > 0 OR fs.return_6m_score > 0 OR fs.return_1y_score > 0
      ORDER BY 
        fs.last_updated DESC
      LIMIT 10
    `;
    
    const recentlyScoredResult = await executeRawQuery(recentlyScoredQuery);
    const recentlyScored = recentlyScoredResult.rows;
    
    res.status(200).json({
      success: true,
      scoringStats,
      recentlyScored,
      hasRealScores: scoringStats.real_scored_funds > 0
    });
  } catch (error: any) {
    console.error('Error getting quartile scoring status:', error);
    
    res.status(500).json({
      success: false,
      message: 'Failed to get quartile scoring status: ' + (error.message || 'Unknown error'),
      error: String(error)
    });
  }
});

export default router;