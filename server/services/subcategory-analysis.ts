/**
 * Subcategory Analysis Framework
 * Implements peer comparison within fund subcategories using authentic data
 * Preserves existing scoring system - only adds missing subcategory rankings
 */

import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

export class SubcategoryAnalysis {

  /**
   * Calculate subcategory rankings and percentiles for all funds
   * Uses authentic subcategory data from funds table
   */
  static async calculateSubcategoryRankings(): Promise<void> {
    try {
      console.log('Calculating subcategory rankings using authentic fund classifications...');

      // Update subcategory rankings based on total scores within each category
      await pool.query(`
        WITH subcategory_rankings AS (
          SELECT 
            fsc.fund_id,
            f.subcategory,
            fsc.total_score,
            ROW_NUMBER() OVER (
              PARTITION BY f.subcategory 
              ORDER BY fsc.total_score DESC NULLS LAST
            ) as subcategory_rank,
            COUNT(*) OVER (PARTITION BY f.subcategory) as subcategory_total,
            PERCENT_RANK() OVER (
              PARTITION BY f.subcategory 
              ORDER BY fsc.total_score DESC NULLS LAST
            ) * 100 as subcategory_percentile,
            CASE 
              WHEN PERCENT_RANK() OVER (
                PARTITION BY f.subcategory 
                ORDER BY fsc.total_score DESC NULLS LAST
              ) <= 0.25 THEN 1
              WHEN PERCENT_RANK() OVER (
                PARTITION BY f.subcategory 
                ORDER BY fsc.total_score DESC NULLS LAST
              ) <= 0.50 THEN 2
              WHEN PERCENT_RANK() OVER (
                PARTITION BY f.subcategory 
                ORDER BY fsc.total_score DESC NULLS LAST
              ) <= 0.75 THEN 3
              ELSE 4
            END as subcategory_quartile
          FROM fund_scores_corrected fsc
          JOIN funds f ON fsc.fund_id = f.id
          WHERE fsc.score_date = CURRENT_DATE 
          AND f.subcategory IS NOT NULL
          AND f.subcategory != ''
        )
        UPDATE fund_scores_corrected 
        SET 
          subcategory_rank = sr.subcategory_rank,
          subcategory_total = sr.subcategory_total,
          subcategory_percentile = ROUND(sr.subcategory_percentile::numeric, 2),
          subcategory_quartile = sr.subcategory_quartile
        FROM subcategory_rankings sr
        WHERE fund_scores_corrected.fund_id = sr.fund_id 
        AND fund_scores_corrected.score_date = CURRENT_DATE
      `);

      console.log('Subcategory rankings calculation completed');
      
    } catch (error) {
      console.error('Error calculating subcategory rankings:', error);
      throw error;
    }
  }

  /**
   * Get subcategory performance statistics
   */
  static async getSubcategoryStats(): Promise<any> {
    try {
      const result = await pool.query(`
        SELECT 
          f.subcategory,
          COUNT(*) as total_funds,
          AVG(fsc.total_score)::numeric(5,2) as avg_score,
          MAX(fsc.total_score) as max_score,
          MIN(fsc.total_score) as min_score,
          COUNT(CASE WHEN fsc.subcategory_quartile = 1 THEN 1 END) as q1_funds,
          COUNT(CASE WHEN fsc.subcategory_quartile = 2 THEN 1 END) as q2_funds,
          COUNT(CASE WHEN fsc.subcategory_quartile = 3 THEN 1 END) as q3_funds,
          COUNT(CASE WHEN fsc.subcategory_quartile = 4 THEN 1 END) as q4_funds
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = CURRENT_DATE
        AND f.subcategory IS NOT NULL
        AND f.subcategory != ''
        GROUP BY f.subcategory
        HAVING COUNT(*) >= 5  -- Only include subcategories with 5+ funds
        ORDER BY total_funds DESC
      `);

      return result.rows;
      
    } catch (error) {
      console.error('Error getting subcategory stats:', error);
      return [];
    }
  }

  /**
   * Calculate category-level performance attribution
   */
  static async calculateCategoryAttribution(): Promise<void> {
    try {
      console.log('Calculating category performance attribution...');

      // Update category performance data
      await pool.query(`
        WITH category_performance AS (
          SELECT 
            f.category,
            f.subcategory,
            AVG(fsc.total_score) as avg_score,
            AVG(fsc.historical_returns_total) as avg_returns,
            AVG(fsc.risk_grade_total) as avg_risk,
            AVG(fsc.other_metrics_total) as avg_other,
            COUNT(*) as fund_count
          FROM fund_scores_corrected fsc
          JOIN funds f ON fsc.fund_id = f.id
          WHERE fsc.score_date = CURRENT_DATE
          AND f.category IS NOT NULL
          GROUP BY f.category, f.subcategory
        )
        UPDATE fund_scores_corrected 
        SET category = (
          SELECT f.category 
          FROM funds f 
          WHERE f.id = fund_scores_corrected.fund_id
        )
        WHERE score_date = CURRENT_DATE
        AND category IS NULL
      `);

      console.log('Category attribution calculation completed');
      
    } catch (error) {
      console.error('Error calculating category attribution:', error);
      throw error;
    }
  }

  /**
   * Process complete subcategory analysis for all funds
   */
  static async processCompleteSubcategoryAnalysis(): Promise<any> {
    try {
      console.log('Starting complete subcategory analysis...');

      // Calculate subcategory rankings
      await this.calculateSubcategoryRankings();

      // Calculate category attribution
      await this.calculateCategoryAttribution();

      // Get final statistics
      const stats = await this.getSubcategoryStats();

      // Verify results
      const verification = await pool.query(`
        SELECT 
          COUNT(*) as total_funds,
          COUNT(CASE WHEN subcategory_rank IS NOT NULL THEN 1 END) as funds_with_rankings,
          COUNT(CASE WHEN subcategory_percentile IS NOT NULL THEN 1 END) as funds_with_percentiles,
          COUNT(DISTINCT subcategory) as total_subcategories
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.score_date = CURRENT_DATE
        AND f.subcategory IS NOT NULL
        AND f.subcategory != ''
      `);

      const results = {
        verification: verification.rows[0],
        subcategoryStats: stats,
        processed: true
      };

      console.log('Subcategory analysis results:', results.verification);
      
      return results;
      
    } catch (error) {
      console.error('Error in complete subcategory analysis:', error);
      throw error;
    }
  }

  /**
   * Get peer comparison data for a specific fund
   */
  static async getPeerComparison(fundId: number): Promise<any> {
    try {
      const result = await pool.query(`
        SELECT 
          fsc.fund_id,
          f.fund_name,
          f.subcategory,
          fsc.total_score,
          fsc.subcategory_rank,
          fsc.subcategory_total,
          fsc.subcategory_percentile,
          fsc.subcategory_quartile,
          fsc.historical_returns_total,
          fsc.risk_grade_total,
          fsc.other_metrics_total
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE fsc.fund_id = $1
        AND fsc.score_date = CURRENT_DATE
      `, [fundId]);

      if (result.rows.length === 0) return null;

      const fundData = result.rows[0];

      // Get peer funds in same subcategory
      const peers = await pool.query(`
        SELECT 
          fsc.fund_id,
          f.fund_name,
          fsc.total_score,
          fsc.subcategory_rank,
          fsc.subcategory_percentile
        FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.subcategory = $1
        AND fsc.score_date = CURRENT_DATE
        AND fsc.fund_id != $2
        ORDER BY fsc.total_score DESC
        LIMIT 10
      `, [fundData.subcategory, fundId]);

      return {
        fund: fundData,
        peers: peers.rows,
        subcategory: fundData.subcategory
      };
      
    } catch (error) {
      console.error(`Error getting peer comparison for fund ${fundId}:`, error);
      return null;
    }
  }
}

export default SubcategoryAnalysis;