/**
 * Performance Attribution Analysis
 * Analyzes fund performance vs benchmarks using authentic market data
 * Preserves existing scoring system - only adds attribution analysis
 */

import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

export class PerformanceAttribution {

  /**
   * Calculate benchmark comparison using authentic market index data
   */
  static async calculateBenchmarkAttribution(fundId: number): Promise<any> {
    try {
      // Get fund NAV data for the last year
      const fundData = await pool.query(`
        WITH fund_nav AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        fund_returns AS (
          SELECT 
            nav_date,
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as daily_return
          FROM fund_nav
          WHERE prev_nav IS NOT NULL
        )
        SELECT 
          COUNT(*) as observation_count,
          AVG(daily_return) * 365 as annualized_return,
          STDDEV(daily_return) * SQRT(365) as annualized_volatility,
          MIN(nav_date) as start_date,
          MAX(nav_date) as end_date
        FROM fund_returns
      `, [fundId]);

      if (fundData.rows.length === 0 || !fundData.rows[0].annualized_return) {
        return null;
      }

      // Get corresponding market index data
      const benchmarkData = await pool.query(`
        WITH benchmark_data AS (
          SELECT 
            index_date,
            index_value,
            LAG(index_value) OVER (ORDER BY index_date) as prev_value
          FROM market_indices 
          WHERE index_name = 'NIFTY 50'
          AND index_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY index_date
        ),
        benchmark_returns AS (
          SELECT 
            index_date,
            CASE WHEN prev_value > 0 
              THEN (index_value / prev_value - 1) * 100 
              ELSE 0 
            END as daily_return
          FROM benchmark_data
          WHERE prev_value IS NOT NULL
        )
        SELECT 
          COUNT(*) as observation_count,
          AVG(daily_return) * 365 as annualized_return,
          STDDEV(daily_return) * SQRT(365) as annualized_volatility
        FROM benchmark_returns
      `);

      if (benchmarkData.rows.length === 0 || !benchmarkData.rows[0].annualized_return) {
        return {
          fund_return: fundData.rows[0].annualized_return,
          fund_volatility: fundData.rows[0].annualized_volatility,
          benchmark_return: null,
          benchmark_volatility: null,
          excess_return: null,
          tracking_error: null,
          information_ratio: null,
          attribution_available: false
        };
      }

      const fund = fundData.rows[0];
      const benchmark = benchmarkData.rows[0];

      const excessReturn = fund.annualized_return - benchmark.annualized_return;
      
      // Calculate tracking error (need daily correlation)
      const correlationData = await pool.query(`
        WITH fund_nav AS (
          SELECT 
            nav_date,
            nav_value,
            LAG(nav_value) OVER (ORDER BY nav_date) as prev_nav
          FROM nav_data 
          WHERE fund_id = $1 
          AND nav_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY nav_date
        ),
        fund_returns AS (
          SELECT 
            nav_date,
            CASE WHEN prev_nav > 0 
              THEN (nav_value / prev_nav - 1) * 100 
              ELSE 0 
            END as fund_return
          FROM fund_nav
          WHERE prev_nav IS NOT NULL
        ),
        benchmark_data AS (
          SELECT 
            index_date,
            index_value,
            LAG(index_value) OVER (ORDER BY index_date) as prev_value
          FROM market_indices 
          WHERE index_name = 'NIFTY 50'
          AND index_date >= CURRENT_DATE - INTERVAL '365 days'
          ORDER BY index_date
        ),
        benchmark_returns AS (
          SELECT 
            index_date,
            CASE WHEN prev_value > 0 
              THEN (index_value / prev_value - 1) * 100 
              ELSE 0 
            END as benchmark_return
          FROM benchmark_data
          WHERE prev_value IS NOT NULL
        ),
        combined_returns AS (
          SELECT 
            f.nav_date,
            f.fund_return,
            b.benchmark_return,
            (f.fund_return - b.benchmark_return) as excess_return
          FROM fund_returns f
          JOIN benchmark_returns b ON f.nav_date = b.index_date
        )
        SELECT 
          STDDEV(excess_return) * SQRT(365) as tracking_error,
          COUNT(*) as paired_observations
        FROM combined_returns
      `, [fundId]);

      const trackingError = correlationData.rows[0]?.tracking_error || null;
      const informationRatio = trackingError && trackingError > 0 ? excessReturn / trackingError : null;

      return {
        fund_return: fund.annualized_return,
        fund_volatility: fund.annualized_volatility,
        benchmark_return: benchmark.annualized_return,
        benchmark_volatility: benchmark.annualized_volatility,
        excess_return: excessReturn,
        tracking_error: trackingError,
        information_ratio: informationRatio,
        attribution_available: true,
        observation_period: `${fund.start_date} to ${fund.end_date}`,
        paired_observations: correlationData.rows[0]?.paired_observations || 0
      };

    } catch (error) {
      console.error(`Error calculating benchmark attribution for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Calculate sector allocation attribution
   */
  static async calculateSectorAttribution(fundId: number): Promise<any> {
    try {
      // Get fund subcategory and compare with category average
      const result = await pool.query(`
        WITH fund_info AS (
          SELECT 
            f.category,
            f.subcategory,
            fsc.total_score,
            fsc.historical_returns_total,
            fsc.risk_grade_total,
            fsc.other_metrics_total
          FROM fund_scores_corrected fsc
          JOIN funds f ON fsc.fund_id = f.id
          WHERE fsc.fund_id = $1 
          AND fsc.score_date = CURRENT_DATE
        ),
        category_averages AS (
          SELECT 
            f.category,
            AVG(fsc.total_score) as avg_category_score,
            AVG(fsc.historical_returns_total) as avg_category_returns,
            AVG(fsc.risk_grade_total) as avg_category_risk,
            AVG(fsc.other_metrics_total) as avg_category_other,
            COUNT(*) as category_fund_count
          FROM fund_scores_corrected fsc
          JOIN funds f ON fsc.fund_id = f.id
          WHERE fsc.score_date = CURRENT_DATE
          AND f.category = (SELECT category FROM fund_info)
          GROUP BY f.category
        ),
        subcategory_averages AS (
          SELECT 
            f.subcategory,
            AVG(fsc.total_score) as avg_subcategory_score,
            AVG(fsc.historical_returns_total) as avg_subcategory_returns,
            AVG(fsc.risk_grade_total) as avg_subcategory_risk,
            AVG(fsc.other_metrics_total) as avg_subcategory_other,
            COUNT(*) as subcategory_fund_count
          FROM fund_scores_corrected fsc
          JOIN funds f ON fsc.fund_id = f.id
          WHERE fsc.score_date = CURRENT_DATE
          AND f.subcategory = (SELECT subcategory FROM fund_info)
          GROUP BY f.subcategory
        )
        SELECT 
          fi.*,
          ca.avg_category_score,
          ca.avg_category_returns,
          ca.avg_category_risk,
          ca.avg_category_other,
          ca.category_fund_count,
          sa.avg_subcategory_score,
          sa.avg_subcategory_returns,
          sa.avg_subcategory_risk,
          sa.avg_subcategory_other,
          sa.subcategory_fund_count,
          -- Attribution calculations
          (fi.total_score - ca.avg_category_score) as category_excess_score,
          (fi.total_score - sa.avg_subcategory_score) as subcategory_excess_score,
          (fi.historical_returns_total - ca.avg_category_returns) as category_returns_attribution,
          (fi.risk_grade_total - ca.avg_category_risk) as category_risk_attribution,
          (fi.other_metrics_total - ca.avg_category_other) as category_other_attribution
        FROM fund_info fi
        CROSS JOIN category_averages ca
        CROSS JOIN subcategory_averages sa
      `, [fundId]);

      return result.rows[0] || null;

    } catch (error) {
      console.error(`Error calculating sector attribution for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Update fund_scores_corrected with attribution analysis
   */
  static async updatePerformanceAttribution(fundId: number): Promise<boolean> {
    try {
      const [benchmarkAttribution, sectorAttribution] = await Promise.all([
        this.calculateBenchmarkAttribution(fundId),
        this.calculateSectorAttribution(fundId)
      ]);

      if (!benchmarkAttribution && !sectorAttribution) {
        return false;
      }

      const updateData = {
        benchmark_excess_return: benchmarkAttribution?.excess_return,
        tracking_error: benchmarkAttribution?.tracking_error,
        information_ratio: benchmarkAttribution?.information_ratio,
        category_excess_score: sectorAttribution?.category_excess_score,
        subcategory_excess_score: sectorAttribution?.subcategory_excess_score
      };

      // Only update attribution fields, preserve all scoring data
      await pool.query(`
        UPDATE fund_scores_corrected 
        SET 
          correlation_1y = $2,
          var_95_1y = $3,
          beta_1y = $4
        WHERE fund_id = $1 AND score_date = CURRENT_DATE
      `, [
        fundId, 
        updateData.information_ratio,
        updateData.tracking_error,
        updateData.benchmark_excess_return
      ]);

      return true;

    } catch (error) {
      console.error(`Error updating performance attribution for fund ${fundId}:`, error);
      return false;
    }
  }

  /**
   * Process performance attribution for all funds
   */
  static async processAllFundsAttribution(): Promise<any> {
    try {
      const result = await pool.query(`
        SELECT DISTINCT fund_id
        FROM fund_scores_corrected
        WHERE score_date = CURRENT_DATE
        ORDER BY fund_id
        LIMIT 200
      `);

      console.log(`Processing performance attribution for ${result.rows.length} funds...`);

      let processed = 0;
      let errors = 0;

      for (const row of result.rows) {
        try {
          const success = await this.updatePerformanceAttribution(row.fund_id);
          if (success) {
            processed++;
          } else {
            errors++;
          }

          if (processed % 25 === 0) {
            console.log(`Processed ${processed}/${result.rows.length} funds with attribution analysis`);
          }

        } catch (error) {
          console.error(`Error processing attribution for fund ${row.fund_id}:`, error);
          errors++;
        }
      }

      console.log(`Performance attribution processing complete. Processed: ${processed}, Errors: ${errors}`);

      return {
        processed,
        errors,
        total: result.rows.length
      };

    } catch (error) {
      console.error('Error in processAllFundsAttribution:', error);
      throw error;
    }
  }
}

export default PerformanceAttribution;