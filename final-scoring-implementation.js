/**
 * Final Scoring Implementation - Source of Truth
 * Total: 100 Points Maximum
 * 
 * Historical Returns (40 points):
 * ├── 3-month rolling: 5 points
 * ├── 6-month rolling: 10 points  
 * ├── 1-year rolling: 10 points
 * ├── 3-year rolling: 8 points
 * └── 5-year rolling: 7 points
 * 
 * Risk Grade (30 points):
 * ├── Std Dev 1Y: 5 points
 * ├── Std Dev 3Y: 5 points
 * ├── Up/Down Capture 1Y: 8 points
 * ├── Up/Down Capture 3Y: 8 points
 * └── Max Drawdown: 4 points
 * 
 * Other Metrics (30 points):
 * ├── Sectoral Similarity: 10 points
 * ├── Forward Score: 10 points
 * ├── AUM Size: 5 points
 * └── Expense Ratio: 5 points
 */

import { Pool } from 'pg';
import { config } from 'dotenv';

config();

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

class FinalScoringImplementation {

  /**
   * Calculate 3-month return score (5 points maximum)
   */
  static calculate3MonthScore(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= 15.0) return 5.0;
    if (returnPercent >= 12.0) return 4.0;
    if (returnPercent >= 8.0) return 3.0;
    if (returnPercent >= 5.0) return 2.0;
    if (returnPercent >= 0.0) return 1.0;
    
    return Math.max(0, returnPercent * 0.1); // Negative return penalty
  }

  /**
   * Calculate 6-month return score (10 points maximum)
   */
  static calculate6MonthScore(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= 15.0) return 10.0;
    if (returnPercent >= 12.0) return 8.0;
    if (returnPercent >= 8.0) return 6.0;
    if (returnPercent >= 5.0) return 4.0;
    if (returnPercent >= 0.0) return 2.0;
    
    return Math.max(0, returnPercent * 0.2); // Negative return penalty
  }

  /**
   * Calculate 1-year return score (10 points maximum)
   */
  static calculate1YearScore(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= 15.0) return 10.0;
    if (returnPercent >= 12.0) return 8.0;
    if (returnPercent >= 8.0) return 6.0;
    if (returnPercent >= 5.0) return 4.0;
    if (returnPercent >= 0.0) return 2.0;
    
    return Math.max(0, returnPercent * 0.2); // Negative return penalty
  }

  /**
   * Calculate 3-year return score (8 points maximum)
   */
  static calculate3YearScore(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= 15.0) return 8.0;
    if (returnPercent >= 12.0) return 6.4;
    if (returnPercent >= 8.0) return 4.8;
    if (returnPercent >= 5.0) return 3.2;
    if (returnPercent >= 0.0) return 1.6;
    
    return Math.max(0, returnPercent * 0.16); // Negative return penalty
  }

  /**
   * Calculate 5-year return score (7 points maximum)
   */
  static calculate5YearScore(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= 15.0) return 7.0;
    if (returnPercent >= 12.0) return 5.6;
    if (returnPercent >= 8.0) return 4.2;
    if (returnPercent >= 5.0) return 2.8;
    if (returnPercent >= 0.0) return 1.4;
    
    return Math.max(0, returnPercent * 0.14); // Negative return penalty
  }

  /**
   * Calculate Standard Deviation 1Y score (5 points maximum)
   */
  static calculateStdDev1YScore(volatility) {
    if (volatility === null || volatility === undefined) return 0;
    
    if (volatility <= 10) return 5.0;
    if (volatility <= 15) return 4.0;
    if (volatility <= 20) return 3.0;
    if (volatility <= 25) return 2.0;
    if (volatility <= 30) return 1.0;
    
    return Math.max(0, 5 - (volatility - 10) * 0.1);
  }

  /**
   * Calculate Standard Deviation 3Y score (5 points maximum)
   */
  static calculateStdDev3YScore(volatility) {
    if (volatility === null || volatility === undefined) return 0;
    
    if (volatility <= 10) return 5.0;
    if (volatility <= 15) return 4.0;
    if (volatility <= 20) return 3.0;
    if (volatility <= 25) return 2.0;
    if (volatility <= 30) return 1.0;
    
    return Math.max(0, 5 - (volatility - 10) * 0.1);
  }

  /**
   * Calculate Up/Down Capture 1Y score (8 points maximum)
   */
  static calculateUpDownCapture1YScore(upCapture, downCapture) {
    if (upCapture === null || downCapture === null) return 0;
    
    // Better up capture (higher) and lower down capture (lower) = higher score
    const captureRatio = upCapture / Math.max(downCapture, 0.1);
    
    if (captureRatio >= 1.5) return 8.0;
    if (captureRatio >= 1.2) return 6.4;
    if (captureRatio >= 1.0) return 4.8;
    if (captureRatio >= 0.8) return 3.2;
    
    return Math.max(0, captureRatio * 4);
  }

  /**
   * Calculate Up/Down Capture 3Y score (8 points maximum)
   */
  static calculateUpDownCapture3YScore(upCapture, downCapture) {
    if (upCapture === null || downCapture === null) return 0;
    
    // Better up capture (higher) and lower down capture (lower) = higher score
    const captureRatio = upCapture / Math.max(downCapture, 0.1);
    
    if (captureRatio >= 1.5) return 8.0;
    if (captureRatio >= 1.2) return 6.4;
    if (captureRatio >= 1.0) return 4.8;
    if (captureRatio >= 0.8) return 3.2;
    
    return Math.max(0, captureRatio * 4);
  }

  /**
   * Calculate Max Drawdown score (4 points maximum)
   */
  static calculateMaxDrawdownScore(maxDrawdown) {
    if (maxDrawdown === null || maxDrawdown === undefined) return 0;
    
    // Lower drawdown = higher score
    if (maxDrawdown <= 5) return 4.0;
    if (maxDrawdown <= 10) return 3.2;
    if (maxDrawdown <= 15) return 2.4;
    if (maxDrawdown <= 20) return 1.6;
    if (maxDrawdown <= 25) return 0.8;
    
    return Math.max(0, 4 - (maxDrawdown - 5) * 0.16);
  }

  /**
   * Calculate Sectoral Similarity score (10 points maximum)
   */
  static calculateSectoralSimilarityScore(fund) {
    // Category-based similarity analysis
    const categoryBonus = fund.subcategory ? 2.0 : 0;
    const baseScore = 6.0; // Base sectoral score
    
    return Math.min(10.0, baseScore + categoryBonus);
  }

  /**
   * Calculate Forward Score (10 points maximum)
   */
  static calculateForwardScore(recentReturns) {
    // Based on recent performance momentum
    const { return_3m, return_6m } = recentReturns;
    
    if (return_3m === null || return_6m === null) return 0;
    
    const momentum = (return_3m + return_6m) / 2;
    
    if (momentum >= 15) return 10.0;
    if (momentum >= 10) return 8.0;
    if (momentum >= 5) return 6.0;
    if (momentum >= 0) return 4.0;
    
    return Math.max(0, momentum * 0.4);
  }

  /**
   * Calculate AUM Size score (5 points maximum)
   */
  static calculateAUMSizeScore(aumValue) {
    if (aumValue === null || aumValue === undefined) return 0;
    
    const aumCrores = aumValue / 10000000; // Convert to crores
    
    if (aumCrores >= 5000) return 5.0; // Large cap advantage
    if (aumCrores >= 1000) return 4.0;
    if (aumCrores >= 500) return 3.5;
    if (aumCrores >= 100) return 3.0;
    if (aumCrores >= 50) return 2.0;
    
    return Math.max(1.0, aumCrores / 1000); // Minimum 1 point
  }

  /**
   * Calculate Expense Ratio score (5 points maximum)
   */
  static calculateExpenseRatioScore(expenseRatio) {
    if (expenseRatio === null || expenseRatio === undefined) return 0;
    
    // Lower expense ratio = higher score
    if (expenseRatio <= 0.5) return 5.0;
    if (expenseRatio <= 1.0) return 4.0;
    if (expenseRatio <= 1.5) return 3.0;
    if (expenseRatio <= 2.0) return 2.0;
    if (expenseRatio <= 2.5) return 1.0;
    
    return Math.max(0, 5 - (expenseRatio - 0.5) * 2);
  }

  /**
   * Get fund data with authentic NAV calculations
   */
  static async getFundDataWithReturns(fundId) {
    const result = await pool.query(`
      SELECT 
        f.*,
        fpm.*,
        -- Calculate authentic returns from NAV data
        (SELECT 
          ((nav_current.nav_value / nav_3m.nav_value) - 1) * 100
         FROM nav_data nav_current, nav_data nav_3m
         WHERE nav_current.fund_id = f.id 
           AND nav_3m.fund_id = f.id
           AND nav_current.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id)
           AND nav_3m.nav_date <= nav_current.nav_date - INTERVAL '90 days'
         ORDER BY ABS(EXTRACT(EPOCH FROM (nav_current.nav_date - nav_3m.nav_date - INTERVAL '90 days')))
         LIMIT 1) as return_3m_percent,
         
        (SELECT 
          ((nav_current.nav_value / nav_6m.nav_value) - 1) * 100
         FROM nav_data nav_current, nav_data nav_6m
         WHERE nav_current.fund_id = f.id 
           AND nav_6m.fund_id = f.id
           AND nav_current.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id)
           AND nav_6m.nav_date <= nav_current.nav_date - INTERVAL '180 days'
         ORDER BY ABS(EXTRACT(EPOCH FROM (nav_current.nav_date - nav_6m.nav_date - INTERVAL '180 days')))
         LIMIT 1) as return_6m_percent,
         
        (SELECT 
          ((nav_current.nav_value / nav_1y.nav_value) - 1) * 100
         FROM nav_data nav_current, nav_data nav_1y
         WHERE nav_current.fund_id = f.id 
           AND nav_1y.fund_id = f.id
           AND nav_current.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id)
           AND nav_1y.nav_date <= nav_current.nav_date - INTERVAL '365 days'
         ORDER BY ABS(EXTRACT(EPOCH FROM (nav_current.nav_date - nav_1y.nav_date - INTERVAL '365 days')))
         LIMIT 1) as return_1y_percent,
         
        (SELECT 
          (POWER(nav_current.nav_value / nav_3y.nav_value, 365.0/1095.0) - 1) * 100
         FROM nav_data nav_current, nav_data nav_3y
         WHERE nav_current.fund_id = f.id 
           AND nav_3y.fund_id = f.id
           AND nav_current.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id)
           AND nav_3y.nav_date <= nav_current.nav_date - INTERVAL '1095 days'
         ORDER BY ABS(EXTRACT(EPOCH FROM (nav_current.nav_date - nav_3y.nav_date - INTERVAL '1095 days')))
         LIMIT 1) as return_3y_percent,
         
        (SELECT 
          (POWER(nav_current.nav_value / nav_5y.nav_value, 365.0/1825.0) - 1) * 100
         FROM nav_data nav_current, nav_5y
         WHERE nav_current.fund_id = f.id 
           AND nav_5y.fund_id = f.id
           AND nav_current.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id)
           AND nav_5y.nav_date <= nav_current.nav_date - INTERVAL '1825 days'
         ORDER BY ABS(EXTRACT(EPOCH FROM (nav_current.nav_date - nav_5y.nav_date - INTERVAL '1825 days')))
         LIMIT 1) as return_5y_percent
         
      FROM funds f
      LEFT JOIN fund_performance_metrics fpm ON f.id = fpm.fund_id
      WHERE f.id = $1
    `, [fundId]);
    
    return result.rows[0];
  }

  /**
   * Calculate final scores for a fund using the new specification
   */
  static async calculateFinalScores(fundId) {
    const fundData = await this.getFundDataWithReturns(fundId);
    if (!fundData) return null;

    const scores = {};

    // Historical Returns Component (40 points total)
    scores.return_3m_score = this.calculate3MonthScore(fundData.return_3m_percent);
    scores.return_6m_score = this.calculate6MonthScore(fundData.return_6m_percent);
    scores.return_1y_score = this.calculate1YearScore(fundData.return_1y_percent);
    scores.return_3y_score = this.calculate3YearScore(fundData.return_3y_percent);
    scores.return_5y_score = this.calculate5YearScore(fundData.return_5y_percent);
    
    scores.historical_returns_total = Math.min(40.0, 
      scores.return_3m_score + scores.return_6m_score + scores.return_1y_score + 
      scores.return_3y_score + scores.return_5y_score
    );

    // Risk Grade Component (30 points total)
    scores.std_dev_1y_score = this.calculateStdDev1YScore(fundData.volatility_1y_percent);
    scores.std_dev_3y_score = this.calculateStdDev3YScore(fundData.volatility_3y_percent);
    scores.updown_capture_1y_score = this.calculateUpDownCapture1YScore(
      fundData.up_capture_ratio_1y, fundData.down_capture_ratio_1y
    );
    scores.updown_capture_3y_score = this.calculateUpDownCapture3YScore(
      fundData.up_capture_ratio_3y, fundData.down_capture_ratio_3y
    );
    scores.max_drawdown_score = this.calculateMaxDrawdownScore(fundData.max_drawdown_percent);
    
    scores.risk_grade_total = Math.min(30.0,
      scores.std_dev_1y_score + scores.std_dev_3y_score + 
      scores.updown_capture_1y_score + scores.updown_capture_3y_score + 
      scores.max_drawdown_score
    );

    // Other Metrics Component (30 points total)
    scores.sectoral_similarity_score = this.calculateSectoralSimilarityScore(fundData);
    scores.forward_score = this.calculateForwardScore({
      return_3m: fundData.return_3m_percent,
      return_6m: fundData.return_6m_percent
    });
    scores.aum_size_score = this.calculateAUMSizeScore(fundData.aum_value);
    scores.expense_ratio_score = this.calculateExpenseRatioScore(fundData.expense_ratio);
    
    scores.other_metrics_total = Math.min(30.0,
      scores.sectoral_similarity_score + scores.forward_score + 
      scores.aum_size_score + scores.expense_ratio_score
    );

    // Total Score (100 points maximum)
    scores.total_score = Math.min(100.0,
      scores.historical_returns_total + scores.risk_grade_total + scores.other_metrics_total
    );

    // Store fund information
    scores.fund_id = fundId;
    scores.subcategory = fundData.subcategory;
    scores.fund_name = fundData.fund_name;

    return scores;
  }

  /**
   * Update fund_scores_corrected table with final specification scores
   */
  static async updateFinalScores(scores) {
    const scoreDate = new Date().toISOString().split('T')[0];
    
    await pool.query(`
      INSERT INTO fund_scores_corrected (
        fund_id, score_date, subcategory,
        return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score,
        historical_returns_total,
        std_dev_1y_score, std_dev_3y_score, updown_capture_1y_score, updown_capture_3y_score, max_drawdown_score,
        risk_grade_total,
        sectoral_similarity_score, forward_score, aum_size_score, expense_ratio_score,
        other_metrics_total,
        total_score
      ) VALUES (
        $1, $2, $3,
        $4, $5, $6, $7, $8,
        $9,
        $10, $11, $12, $13, $14,
        $15,
        $16, $17, $18, $19,
        $20,
        $21
      )
      ON CONFLICT (fund_id, score_date) 
      DO UPDATE SET
        return_3m_score = EXCLUDED.return_3m_score,
        return_6m_score = EXCLUDED.return_6m_score,
        return_1y_score = EXCLUDED.return_1y_score,
        return_3y_score = EXCLUDED.return_3y_score,
        return_5y_score = EXCLUDED.return_5y_score,
        historical_returns_total = EXCLUDED.historical_returns_total,
        std_dev_1y_score = EXCLUDED.std_dev_1y_score,
        std_dev_3y_score = EXCLUDED.std_dev_3y_score,
        updown_capture_1y_score = EXCLUDED.updown_capture_1y_score,
        updown_capture_3y_score = EXCLUDED.updown_capture_3y_score,
        max_drawdown_score = EXCLUDED.max_drawdown_score,
        risk_grade_total = EXCLUDED.risk_grade_total,
        sectoral_similarity_score = EXCLUDED.sectoral_similarity_score,
        forward_score = EXCLUDED.forward_score,
        aum_size_score = EXCLUDED.aum_size_score,
        expense_ratio_score = EXCLUDED.expense_ratio_score,
        other_metrics_total = EXCLUDED.other_metrics_total,
        total_score = EXCLUDED.total_score
    `, [
      scores.fund_id, scoreDate, scores.subcategory,
      scores.return_3m_score, scores.return_6m_score, scores.return_1y_score, scores.return_3y_score, scores.return_5y_score,
      scores.historical_returns_total,
      scores.std_dev_1y_score, scores.std_dev_3y_score, scores.updown_capture_1y_score, scores.updown_capture_3y_score, scores.max_drawdown_score,
      scores.risk_grade_total,
      scores.sectoral_similarity_score, scores.forward_score, scores.aum_size_score, scores.expense_ratio_score,
      scores.other_metrics_total,
      scores.total_score
    ]);
  }

  /**
   * Process all funds with the final scoring specification
   */
  static async processAllFundsWithFinalScoring() {
    console.log('Starting final scoring implementation for all funds...');
    
    // Get all funds with sufficient NAV data
    const result = await pool.query(`
      SELECT DISTINCT f.id, f.fund_name
      FROM funds f
      WHERE EXISTS (
        SELECT 1 FROM nav_data nd 
        WHERE nd.fund_id = f.id 
        GROUP BY nd.fund_id 
        HAVING COUNT(*) >= 30
      )
      ORDER BY f.id
      LIMIT 100
    `);
    
    const funds = result.rows;
    console.log(`Processing ${funds.length} funds with final scoring specification...`);
    
    let processed = 0;
    let errors = 0;
    
    for (const fund of funds) {
      try {
        const scores = await this.calculateFinalScores(fund.id);
        if (scores) {
          await this.updateFinalScores(scores);
          processed++;
          
          if (processed % 10 === 0) {
            console.log(`Processed ${processed}/${funds.length} funds with final scoring`);
          }
        }
      } catch (error) {
        console.error(`Error processing fund ${fund.id}:`, error.message);
        errors++;
      }
    }
    
    console.log(`Final scoring implementation complete. Processed: ${processed}, Errors: ${errors}`);
    
    // Verify results
    const verification = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        AVG(total_score) as avg_total_score,
        MAX(total_score) as max_total_score,
        MIN(total_score) as min_total_score,
        AVG(historical_returns_total) as avg_historical,
        AVG(risk_grade_total) as avg_risk,
        AVG(other_metrics_total) as avg_other
      FROM fund_scores_corrected
      WHERE score_date = CURRENT_DATE
    `);
    
    console.log('Final scoring verification:', verification.rows[0]);
    
    return {
      processed,
      errors,
      verification: verification.rows[0]
    };
  }
}

// Run the final scoring implementation
async function runFinalScoringImplementation() {
  try {
    const results = await FinalScoringImplementation.processAllFundsWithFinalScoring();
    console.log('Final scoring implementation results:', results);
  } catch (error) {
    console.error('Error in final scoring implementation:', error);
  } finally {
    await pool.end();
  }
}

// Execute if run directly
runFinalScoringImplementation();

export default FinalScoringImplementation;