/**
 * Fix Recommendation-Quartile Alignment
 * Implements proper mapping between quartile rankings and investment recommendations
 * Based on original documentation requirements
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

class RecommendationQuartileAligner {
  /**
   * Calculate proper recommendation based on quartile and score
   * Aligns with original documentation requirements
   */
  static calculateAlignedRecommendation(totalScore, quartile, subcategoryPercentile) {
    // Primary logic: Quartile-based recommendations
    if (quartile === 1) {
      // Top 25% - Strong performers
      if (subcategoryPercentile >= 90 || totalScore >= 75) {
        return 'STRONG_BUY';
      }
      return 'BUY';
    }
    
    if (quartile === 2) {
      // 26-50% - Above average performers
      if (subcategoryPercentile >= 75 && totalScore >= 65) {
        return 'BUY';
      }
      return 'HOLD';
    }
    
    if (quartile === 3) {
      // 51-75% - Below average performers
      if (totalScore >= 60) {
        return 'HOLD';
      }
      return 'SELL';
    }
    
    if (quartile === 4) {
      // Bottom 25% - Poor performers
      if (totalScore <= 35) {
        return 'STRONG_SELL';
      }
      return 'SELL';
    }
    
    // Fallback for unranked funds
    if (totalScore >= 65) return 'BUY';
    if (totalScore >= 50) return 'HOLD';
    return 'SELL';
  }

  /**
   * Update all recommendations to align with quartile system
   */
  static async updateAlignedRecommendations() {
    console.log('Fixing Recommendation-Quartile Alignment...');
    console.log('Implementing proper mapping between quartiles and recommendations\n');

    // Get all funds with current scores
    const fundsResult = await pool.query(`
      SELECT 
        fund_id,
        total_score,
        quartile,
        subcategory_percentile,
        recommendation as current_recommendation
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE
      ORDER BY total_score DESC
    `);

    console.log(`Processing ${fundsResult.rows.length} funds for recommendation alignment...`);

    let updated = 0;
    let unchanged = 0;

    for (const fund of fundsResult.rows) {
      const newRecommendation = this.calculateAlignedRecommendation(
        fund.total_score,
        fund.quartile,
        fund.subcategory_percentile || 50
      );

      if (newRecommendation !== fund.current_recommendation) {
        await pool.query(`
          UPDATE fund_scores_corrected 
          SET recommendation = $1
          WHERE fund_id = $2 AND score_date = CURRENT_DATE
        `, [newRecommendation, fund.fund_id]);
        updated++;
      } else {
        unchanged++;
      }

      if (updated % 100 === 0 && updated > 0) {
        console.log(`Progress: ${updated} recommendations updated...`);
      }
    }

    console.log(`\nRecommendation Alignment Complete:`);
    console.log(`Updated: ${updated} funds`);
    console.log(`Unchanged: ${unchanged} funds`);
    console.log(`Total processed: ${updated + unchanged} funds\n`);

    return { updated, unchanged };
  }

  /**
   * Validate the aligned recommendation distribution
   */
  static async validateAlignedDistribution() {
    console.log('Validating Aligned Recommendation Distribution...\n');

    // Overall distribution
    const distributionResult = await pool.query(`
      SELECT 
        recommendation,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE
      GROUP BY recommendation
      ORDER BY 
        CASE recommendation 
          WHEN 'STRONG_BUY' THEN 1 
          WHEN 'BUY' THEN 2 
          WHEN 'HOLD' THEN 3 
          WHEN 'SELL' THEN 4 
          WHEN 'STRONG_SELL' THEN 5 
        END
    `);

    console.log('Overall Recommendation Distribution:');
    for (const row of distributionResult.rows) {
      console.log(`${row.recommendation}: ${row.count} funds (${row.percentage}%)`);
    }

    // Quartile breakdown
    const quartileBreakdown = await pool.query(`
      SELECT 
        quartile,
        recommendation,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE AND quartile IS NOT NULL
      GROUP BY quartile, recommendation
      ORDER BY quartile, 
        CASE recommendation 
          WHEN 'STRONG_BUY' THEN 1 
          WHEN 'BUY' THEN 2 
          WHEN 'HOLD' THEN 3 
          WHEN 'SELL' THEN 4 
          WHEN 'STRONG_SELL' THEN 5 
        END
    `);

    console.log('\nQuartile-Recommendation Breakdown:');
    let currentQuartile = null;
    for (const row of quartileBreakdown.rows) {
      if (row.quartile !== currentQuartile) {
        console.log(`\nQ${row.quartile}:`);
        currentQuartile = row.quartile;
      }
      console.log(`  ${row.recommendation}: ${row.count} funds`);
    }

    // Score ranges by recommendation
    const scoreRanges = await pool.query(`
      SELECT 
        recommendation,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score,
        ROUND(AVG(total_score), 2) as avg_score,
        COUNT(*) as count
      FROM fund_scores_corrected 
      WHERE score_date = CURRENT_DATE
      GROUP BY recommendation
      ORDER BY avg_score DESC
    `);

    console.log('\nScore Ranges by Recommendation:');
    for (const row of scoreRanges.rows) {
      console.log(`${row.recommendation}: ${row.min_score}-${row.max_score} (avg: ${row.avg_score}, count: ${row.count})`);
    }

    return {
      distribution: distributionResult.rows,
      quartileBreakdown: quartileBreakdown.rows,
      scoreRanges: scoreRanges.rows
    };
  }

  /**
   * Main execution function
   */
  static async execute() {
    try {
      console.log('='.repeat(80));
      console.log('RECOMMENDATION-QUARTILE ALIGNMENT FIX');
      console.log('Ensuring proper mapping between quartiles and recommendations');
      console.log('='.repeat(80));

      // Step 1: Update recommendations
      const updateResult = await this.updateAlignedRecommendations();

      // Step 2: Validate the new distribution
      const validationResult = await this.validateAlignedDistribution();

      console.log('\n' + '='.repeat(80));
      console.log('ALIGNMENT COMPLETE');
      console.log('Recommendation system now properly aligned with quartile rankings');
      console.log('='.repeat(80));

      return {
        success: true,
        updatedFunds: updateResult.updated,
        totalFunds: updateResult.updated + updateResult.unchanged,
        distribution: validationResult.distribution
      };

    } catch (error) {
      console.error('Error in recommendation-quartile alignment:', error);
      return {
        success: false,
        error: error.message
      };
    }
  }
}

// Execute the alignment fix
RecommendationQuartileAligner.execute()
  .then((result) => {
    if (result.success) {
      console.log(`\nAlignment completed successfully!`);
      console.log(`Updated ${result.updatedFunds} out of ${result.totalFunds} total funds`);
    } else {
      console.error(`Alignment failed: ${result.error}`);
    }
    process.exit(result.success ? 0 : 1);
  })
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });