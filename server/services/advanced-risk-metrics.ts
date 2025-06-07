/**
 * Advanced Risk Metrics Implementation
 * Calculates Calmar, Sortino, VaR using authentic NAV data only
 * Preserves existing scoring system - only adds missing metrics
 */

import { pool } from '../db';

export class AdvancedRiskMetrics {
  
  /**
   * Calculate Calmar Ratio from authentic NAV data
   * Calmar Ratio = Annualized Return / Maximum Drawdown
   */
  static async calculateCalmarRatio(fundId: number): Promise<number | null> {
    try {
      const result = await pool.query(`
        WITH nav_series AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        returns_and_drawdown AS (
          SELECT 
            nav_date,
            nav_value,
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as daily_return,
            nav_value / MAX(nav_value) OVER (ORDER BY nav_date ROWS UNBOUNDED PRECEDING) - 1 as drawdown
          FROM nav_series
          WHERE prev_nav IS NOT NULL
        )
        SELECT 
          POWER((MAX(nav_value) / MIN(nav_value)), 365.0 / COUNT(*)) - 1 as annualized_return,
          ABS(MIN(drawdown)) as max_drawdown
        FROM returns_and_drawdown
      `, [fundId]);

      if (result.rows.length === 0) return null;
      
      const { annualized_return, max_drawdown } = result.rows[0];
      
      if (!annualized_return || !max_drawdown || max_drawdown === 0) return null;
      
      return annualized_return / max_drawdown;
      
    } catch (error) {
      console.error(`Error calculating Calmar ratio for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Calculate Sortino Ratio from authentic NAV data
   * Sortino Ratio = (Return - Risk Free Rate) / Downside Deviation
   */
  static async calculateSortinoRatio(fundId: number, riskFreeRate: number = 0.06): Promise<number | null> {
    try {
      const result = await pool.query(`
        WITH nav_series AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        daily_returns AS (
          SELECT 
            nav_date,
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as daily_return
          FROM nav_series
          WHERE prev_nav IS NOT NULL
        ),
        return_stats AS (
          SELECT 
            AVG(daily_return) * 365 as annualized_return,
            SQRT(AVG(CASE 
              WHEN daily_return < ($2 / 365) 
              THEN POWER(daily_return - ($2 / 365), 2) 
              ELSE 0 
            END)) * SQRT(365) as downside_deviation
          FROM daily_returns
        )
        SELECT 
          annualized_return,
          downside_deviation,
          CASE WHEN downside_deviation > 0 
            THEN (annualized_return - $2) / downside_deviation 
            ELSE NULL 
          END as sortino_ratio
        FROM return_stats
      `, [fundId, riskFreeRate]);

      if (result.rows.length === 0 || !result.rows[0].sortino_ratio) return null;
      
      return result.rows[0].sortino_ratio;
      
    } catch (error) {
      console.error(`Error calculating Sortino ratio for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Calculate Value at Risk (95% confidence) from authentic NAV data
   */
  static async calculateVaR95(fundId: number): Promise<number | null> {
    try {
      const result = await pool.query(`
        WITH nav_series AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        daily_returns AS (
          SELECT 
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as daily_return
          FROM nav_series
          WHERE prev_nav IS NOT NULL
        )
        SELECT 
          PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return) as var_95
        FROM daily_returns
        WHERE daily_return IS NOT NULL
      `, [fundId]);

      if (result.rows.length === 0 || !result.rows[0].var_95) return null;
      
      return Math.abs(result.rows[0].var_95);
      
    } catch (error) {
      console.error(`Error calculating VaR 95% for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Calculate downside deviation from authentic NAV data
   */
  static async calculateDownsideDeviation(fundId: number, targetReturn: number = 0): Promise<number | null> {
    try {
      const result = await pool.query(`
        WITH nav_series AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        daily_returns AS (
          SELECT 
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as daily_return
          FROM nav_series
          WHERE prev_nav IS NOT NULL
        )
        SELECT 
          SQRT(AVG(CASE 
            WHEN daily_return < $2 
            THEN POWER(daily_return - $2, 2) 
            ELSE 0 
          END)) * SQRT(365) as downside_deviation
        FROM daily_returns
      `, [fundId, targetReturn]);

      if (result.rows.length === 0 || !result.rows[0].downside_deviation) return null;
      
      return result.rows[0].downside_deviation;
      
    } catch (error) {
      console.error(`Error calculating downside deviation for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Update fund_scores_corrected with advanced risk metrics
   * Only adds missing metrics - preserves existing scores
   */
  static async updateAdvancedRiskMetrics(fundId: number): Promise<boolean> {
    try {
      const [calmarRatio, sortinoRatio, var95, downsideDeviation] = await Promise.all([
        this.calculateCalmarRatio(fundId),
        this.calculateSortinoRatio(fundId),
        this.calculateVaR95(fundId),
        this.calculateDownsideDeviation(fundId)
      ]);

      await pool.query(`
        UPDATE fund_scores_corrected 
        SET 
          calmar_ratio_1y = $2,
          sortino_ratio_1y = $3,
          var_95_1y = $4,
          downside_deviation_1y = $5
        WHERE fund_id = $1 AND score_date = CURRENT_DATE
      `, [fundId, calmarRatio, sortinoRatio, var95, downsideDeviation]);

      return true;
      
    } catch (error) {
      console.error(`Error updating advanced risk metrics for fund ${fundId}:`, error);
      return false;
    }
  }

  /**
   * Process advanced risk metrics for all funds with sufficient NAV data
   */
  static async processAllFundsAdvancedMetrics(): Promise<void> {
    try {
      const result = await pool.query(`
        SELECT DISTINCT fund_id
        FROM fund_scores_corrected
        WHERE score_date = CURRENT_DATE
        AND (calmar_ratio_1y IS NULL OR sortino_ratio_1y IS NULL OR var_95_1y IS NULL)
        ORDER BY fund_id
        LIMIT 500
      `);

      console.log(`Processing advanced risk metrics for ${result.rows.length} funds...`);

      let processed = 0;
      let errors = 0;

      for (const row of result.rows) {
        try {
          const success = await this.updateAdvancedRiskMetrics(row.fund_id);
          if (success) {
            processed++;
          } else {
            errors++;
          }

          if (processed % 50 === 0) {
            console.log(`Processed ${processed}/${result.rows.length} funds with advanced metrics`);
          }
          
        } catch (error) {
          console.error(`Error processing fund ${row.fund_id}:`, error);
          errors++;
        }
      }

      console.log(`Advanced risk metrics processing complete. Processed: ${processed}, Errors: ${errors}`);
      
    } catch (error) {
      console.error('Error in processAllFundsAdvancedMetrics:', error);
    }
  }
}

export default AdvancedRiskMetrics;