import { pool } from '../db';
import { AuthenticFundScoringEngine } from './authentic-fund-scoring-engine';

/**
 * Batch processor to replace synthetic data with authentic calculations
 */
export class AuthenticBatchProcessor {
  private scoringEngine: AuthenticFundScoringEngine;
  
  constructor() {
    this.scoringEngine = new AuthenticFundScoringEngine();
  }
  
  /**
   * Clear synthetic data and recalculate with authentic metrics
   */
  async replaceSyntheticWithAuthentic(): Promise<any> {
    console.log('Starting replacement of synthetic data with authentic calculations...');
    
    try {
      // Clear existing synthetic scores
      await this.clearSyntheticData();
      
      // Get funds with sufficient NAV data for authentic calculation
      const eligibleFunds = await this.getEligibleFunds();
      console.log(`Found ${eligibleFunds.length} funds with sufficient NAV data for authentic scoring`);
      
      // Process funds in batches
      const batchSize = 50;
      let processed = 0;
      let succeeded = 0;
      let failed = 0;
      
      for (let i = 0; i < eligibleFunds.length; i += batchSize) {
        const batch = eligibleFunds.slice(i, i + batchSize);
        console.log(`Processing batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(eligibleFunds.length/batchSize)}`);
        
        const batchResults = await Promise.allSettled(
          batch.map(fund => this.processAuthenticFund(fund.id))
        );
        
        batchResults.forEach((result, index) => {
          if (result.status === 'fulfilled' && result.value) {
            succeeded++;
            console.log(`✓ Fund ${batch[index].id} scored authentically`);
          } else {
            failed++;
            console.log(`✗ Fund ${batch[index].id} failed: ${result.status === 'rejected' ? result.reason : 'No data'}`);
          }
          processed++;
        });
        
        // Brief pause between batches
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      
      // Update quartile rankings with authentic data
      await this.recalculateQuartileRankings();
      
      console.log(`Authentic scoring completed: ${succeeded} succeeded, ${failed} failed`);
      
      return {
        success: true,
        processed,
        succeeded,
        failed,
        message: `Replaced synthetic data with authentic calculations for ${succeeded} funds`
      };
      
    } catch (error) {
      console.error('Error in authentic batch processing:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }
  
  /**
   * Clear synthetic scoring data
   */
  private async clearSyntheticData(): Promise<void> {
    console.log('Clearing synthetic scoring data...');
    
    // Delete synthetic fund_scores (keeping structure)
    await pool.query(`
      DELETE FROM fund_scores 
      WHERE score_date = CURRENT_DATE
      AND (
        expense_ratio_score = 8.5 
        OR total_score IN (65.0, 58.0, 52.0, 48.0)
        OR (return_1y_score = total_score * 0.3)
      )
    `);
    
    // Clear empty risk_analytics records
    await pool.query(`
      DELETE FROM risk_analytics 
      WHERE calculation_date = CURRENT_DATE
      AND sortino_ratio_1y IS NULL
    `);
    
    // Clear synthetic quartile rankings
    await pool.query(`
      DELETE FROM quartile_rankings 
      WHERE calculation_date = CURRENT_DATE
    `);
    
    console.log('Synthetic data cleared');
  }
  
  /**
   * Get funds eligible for authentic scoring
   */
  private async getEligibleFunds(): Promise<any[]> {
    const query = `
      SELECT f.id, f.fund_name, f.category, COUNT(n.nav_value) as nav_count
      FROM funds f
      LEFT JOIN nav_data n ON f.id = n.fund_id
      WHERE f.category IS NOT NULL
      GROUP BY f.id, f.fund_name, f.category
      HAVING COUNT(n.nav_value) >= 252
      ORDER BY COUNT(n.nav_value) DESC
    `;
    
    const result = await pool.query(query);
    return result.rows;
  }
  
  /**
   * Process individual fund with authentic calculations
   */
  private async processAuthenticFund(fundId: number): Promise<boolean> {
    try {
      // Calculate authentic scores
      const scoreData = await this.scoringEngine.scoreFund(fundId);
      
      if (!scoreData) {
        return false;
      }
      
      // Store authentic scores
      await this.scoringEngine.storeFundScore(scoreData);
      
      return true;
      
    } catch (error) {
      console.error(`Error processing fund ${fundId}:`, error);
      return false;
    }
  }
  
  /**
   * Recalculate quartile rankings based on authentic scores
   */
  private async recalculateQuartileRankings(): Promise<void> {
    console.log('Recalculating quartile rankings with authentic scores...');
    
    try {
      const query = `
        WITH authentic_scores AS (
          SELECT 
            fs.fund_id,
            f.category,
            fs.total_score,
            ROW_NUMBER() OVER (PARTITION BY f.category ORDER BY fs.total_score DESC) as rank_in_category,
            COUNT(*) OVER (PARTITION BY f.category) as total_in_category
          FROM fund_scores fs
          JOIN funds f ON fs.fund_id = f.id
          WHERE fs.score_date = CURRENT_DATE
            AND fs.total_score IS NOT NULL
        ),
        quartile_assignments AS (
          SELECT 
            fund_id,
            category,
            total_score,
            rank_in_category,
            total_in_category,
            ROUND(
              (rank_in_category::numeric / total_in_category::numeric) * 100, 
              2
            ) as percentile,
            CASE 
              WHEN rank_in_category <= CEIL(total_in_category * 0.25) THEN 1
              WHEN rank_in_category <= CEIL(total_in_category * 0.50) THEN 2
              WHEN rank_in_category <= CEIL(total_in_category * 0.75) THEN 3
              ELSE 4
            END as quartile,
            CASE 
              WHEN rank_in_category <= CEIL(total_in_category * 0.25) THEN 'Q1 - Top Performer'
              WHEN rank_in_category <= CEIL(total_in_category * 0.50) THEN 'Q2 - Above Average'
              WHEN rank_in_category <= CEIL(total_in_category * 0.75) THEN 'Q3 - Below Average'
              ELSE 'Q4 - Bottom Performer'
            END as quartile_label
          FROM authentic_scores
        )
        INSERT INTO quartile_rankings (
          fund_id,
          category,
          calculation_date,
          quartile,
          quartile_label,
          rank,
          total_funds,
          percentile,
          composite_score
        )
        SELECT 
          fund_id,
          category,
          CURRENT_DATE,
          quartile,
          quartile_label,
          rank_in_category,
          total_in_category,
          percentile,
          total_score
        FROM quartile_assignments
      `;
      
      await pool.query(query);
      
      // Update fund_scores with quartile information
      await pool.query(`
        UPDATE fund_scores fs
        SET 
          quartile = qr.quartile,
          category_rank = qr.rank,
          category_total = qr.total_funds
        FROM quartile_rankings qr
        WHERE fs.fund_id = qr.fund_id 
          AND fs.score_date = CURRENT_DATE
          AND qr.calculation_date = CURRENT_DATE
      `);
      
      console.log('Quartile rankings updated with authentic scores');
      
    } catch (error) {
      console.error('Error recalculating quartile rankings:', error);
      throw error;
    }
  }
  
  /**
   * Get processing status
   */
  async getProcessingStatus(): Promise<any> {
    try {
      const statusQuery = `
        SELECT 
          COUNT(*) as total_scored_funds,
          COUNT(CASE WHEN volatility_1y_percent IS NOT NULL THEN 1 END) as funds_with_risk_data,
          COUNT(CASE WHEN sharpe_ratio_1y IS NOT NULL THEN 1 END) as funds_with_sharpe,
          ROUND(AVG(total_score), 2) as avg_authentic_score,
          COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy_count,
          COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy_count,
          COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold_count,
          COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell_count,
          COUNT(CASE WHEN recommendation = 'STRONG_SELL' THEN 1 END) as strong_sell_count
        FROM fund_scores 
        WHERE score_date = CURRENT_DATE
      `;
      
      const statusResult = await pool.query(statusQuery);
      const status = statusResult.rows[0];
      
      const riskAnalyticsQuery = `
        SELECT COUNT(*) as risk_analytics_records
        FROM risk_analytics 
        WHERE calculation_date = CURRENT_DATE
      `;
      
      const riskResult = await pool.query(riskAnalyticsQuery);
      const riskCount = riskResult.rows[0].risk_analytics_records;
      
      return {
        authentic_scoring_status: {
          total_scored_funds: parseInt(status.total_scored_funds),
          funds_with_risk_data: parseInt(status.funds_with_risk_data),
          funds_with_sharpe: parseInt(status.funds_with_sharpe),
          avg_authentic_score: parseFloat(status.avg_authentic_score),
          risk_analytics_records: parseInt(riskCount)
        },
        recommendation_distribution: {
          strong_buy: parseInt(status.strong_buy_count),
          buy: parseInt(status.buy_count),
          hold: parseInt(status.hold_count),
          sell: parseInt(status.sell_count),
          strong_sell: parseInt(status.strong_sell_count)
        }
      };
      
    } catch (error) {
      console.error('Error getting processing status:', error);
      return { error: error.message };
    }
  }
}

export const authenticBatchProcessor = new AuthenticBatchProcessor();