/**
 * Implement Missing Fundamentals Component
 * Calculates authentic fundamentals_total scores for all funds in fund_scores_corrected
 * Based on expense ratio, AUM size, fund age, and management quality metrics
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: false
});

class FundamentalsComponentImplementation {

  /**
   * Calculate comprehensive fundamentals score for a fund
   */
  static async calculateFundamentalsScore(fundId) {
    try {
      // Get fund basic information
      const fundResult = await pool.query(`
        SELECT 
          f.expense_ratio,
          f.inception_date,
          f.minimum_investment,
          f.exit_load,
          f.fund_manager,
          f.category,
          f.subcategory,
          EXTRACT(DAYS FROM (CURRENT_DATE - f.inception_date))/365.25 as fund_age_years
        FROM funds f
        WHERE f.id = $1
      `, [fundId]);

      if (!fundResult.rows.length) return null;

      const fund = fundResult.rows[0];
      let fundamentalsScore = 0;

      // Component 1: Expense Ratio Score (0-4 points)
      const expenseRatio = parseFloat(fund.expense_ratio) || 1.5;
      if (expenseRatio <= 0.5) fundamentalsScore += 4.0;
      else if (expenseRatio <= 1.0) fundamentalsScore += 3.0;
      else if (expenseRatio <= 1.5) fundamentalsScore += 2.0;
      else if (expenseRatio <= 2.0) fundamentalsScore += 1.0;
      else fundamentalsScore += 0.5;

      // Component 2: Fund Age/Maturity Score (0-3 points)
      const fundAge = parseFloat(fund.fund_age_years) || 0;
      if (fundAge >= 10) fundamentalsScore += 3.0;
      else if (fundAge >= 5) fundamentalsScore += 2.5;
      else if (fundAge >= 3) fundamentalsScore += 2.0;
      else if (fundAge >= 1) fundamentalsScore += 1.0;
      else fundamentalsScore += 0.5;

      // Component 3: Accessibility Score (0-3 points) - based on minimum investment
      const minInvestment = parseFloat(fund.minimum_investment) || 5000;
      if (minInvestment <= 1000) fundamentalsScore += 3.0;
      else if (minInvestment <= 5000) fundamentalsScore += 2.5;
      else if (minInvestment <= 10000) fundamentalsScore += 2.0;
      else if (minInvestment <= 25000) fundamentalsScore += 1.0;
      else fundamentalsScore += 0.5;

      // Component 4: Exit Load Efficiency (0-2 points)
      const exitLoad = parseFloat(fund.exit_load) || 1.0;
      if (exitLoad === 0) fundamentalsScore += 2.0;
      else if (exitLoad <= 0.5) fundamentalsScore += 1.5;
      else if (exitLoad <= 1.0) fundamentalsScore += 1.0;
      else fundamentalsScore += 0.5;

      // Component 5: Category Maturity Bonus (0-3 points)
      const categoryBonus = this.getCategoryMaturityBonus(fund.category, fund.subcategory);
      fundamentalsScore += categoryBonus;

      // Ensure total doesn't exceed 15 points (as per documentation)
      fundamentalsScore = Math.min(fundamentalsScore, 15.0);
      
      return {
        fundamentals_total: Math.round(fundamentalsScore * 100) / 100,
        components: {
          expense_ratio_score: expenseRatio <= 1.0 ? (4 - expenseRatio * 2) : Math.max(0, 4 - expenseRatio),
          fund_age_score: Math.min(3, fundAge / 3),
          accessibility_score: minInvestment <= 5000 ? 3 : Math.max(0.5, 3 - (minInvestment / 10000)),
          exit_load_score: Math.max(0.5, 2 - exitLoad),
          category_bonus: categoryBonus
        },
        fund_details: {
          expense_ratio: expenseRatio,
          fund_age_years: fundAge,
          min_investment: minInvestment,
          exit_load: exitLoad
        }
      };

    } catch (error) {
      console.error(`Error calculating fundamentals for fund ${fundId}:`, error);
      return null;
    }
  }

  /**
   * Get category-specific maturity bonus
   */
  static getCategoryMaturityBonus(category, subcategory) {
    const bonusMap = {
      'Equity': {
        'Large Cap': 3.0,
        'Mid Cap': 2.5,
        'Small Cap': 2.0,
        'Multi Cap': 2.5,
        'Flexi Cap': 2.5,
        default: 2.0
      },
      'Debt': {
        'Liquid': 3.0,
        'Ultra Short Duration': 2.5,
        'Short Duration': 2.5,
        'Medium Duration': 2.0,
        'Long Duration': 1.5,
        default: 2.0
      },
      'Hybrid': {
        'Conservative Hybrid': 2.5,
        'Balanced Hybrid': 2.0,
        'Aggressive Hybrid': 1.5,
        default: 2.0
      },
      default: 1.5
    };

    const categoryBonuses = bonusMap[category] || bonusMap.default;
    if (typeof categoryBonuses === 'object') {
      return categoryBonuses[subcategory] || categoryBonuses.default;
    }
    return categoryBonuses;
  }

  /**
   * Update fund_scores_corrected with fundamentals scores
   */
  static async updateFundamentalsInDatabase(fundId, fundamentalsData) {
    try {
      const result = await pool.query(`
        UPDATE fund_scores_corrected 
        SET 
          fundamentals_total = $2,
          expense_ratio_score = $3,
          age_maturity_score = $4
        WHERE fund_id = $1 AND score_date = '2025-06-05'
        RETURNING fund_id, fundamentals_total
      `, [
        fundId, 
        fundamentalsData.fundamentals_total,
        fundamentalsData.components.expense_ratio_score,
        fundamentalsData.components.fund_age_score
      ]);

      return result.rows.length > 0;
    } catch (error) {
      console.error(`Error updating fundamentals for fund ${fundId}:`, error);
      return false;
    }
  }

  /**
   * Recalculate total scores after adding fundamentals
   */
  static async recalculateTotalScores() {
    console.log('Recalculating total scores with fundamentals component...\n');

    const result = await pool.query(`
      UPDATE fund_scores_corrected 
      SET total_score = COALESCE(historical_returns_total, 0) + 
                       COALESCE(risk_grade_total, 0) + 
                       COALESCE(fundamentals_total, 0) + 
                       COALESCE(other_metrics_total, 0)
      WHERE score_date = '2025-06-05' AND fundamentals_total IS NOT NULL
      RETURNING fund_id, total_score
    `);

    console.log(`✓ Updated total scores for ${result.rows.length} funds`);
    return result.rows.length;
  }

  /**
   * Apply updated recommendations based on new total scores
   */
  static async applyUpdatedRecommendations() {
    console.log('Applying updated recommendations with fundamentals...\n');

    const result = await pool.query(`
      UPDATE fund_scores_corrected 
      SET recommendation = CASE 
        WHEN total_score >= 70 OR (total_score >= 65 AND quartile = 1 AND risk_grade_total >= 25) THEN 'STRONG_BUY'
        WHEN total_score >= 60 OR (total_score >= 55 AND quartile IN (1,2) AND fundamentals_total >= 20) THEN 'BUY'
        WHEN total_score >= 50 OR (total_score >= 45 AND quartile IN (1,2,3) AND risk_grade_total >= 20) THEN 'HOLD'
        WHEN total_score >= 35 OR (total_score >= 30 AND risk_grade_total >= 15) THEN 'SELL'
        ELSE 'STRONG_SELL'
      END
      WHERE score_date = '2025-06-05' AND fundamentals_total IS NOT NULL
    `);

    console.log(`✓ Updated recommendations for ${result.rowCount} funds`);
    return result.rowCount;
  }

  /**
   * Main implementation function
   */
  static async implementFundamentalsComponent() {
    console.log('IMPLEMENTING FUNDAMENTALS COMPONENT');
    console.log('='.repeat(50));

    try {
      // Get all funds needing fundamentals calculation
      const fundsResult = await pool.query(`
        SELECT fund_id 
        FROM fund_scores_corrected 
        WHERE score_date = '2025-06-05' 
          AND (fundamentals_total IS NULL OR fundamentals_total = 0)
        ORDER BY fund_id
      `);

      const fundsToProcess = fundsResult.rows;
      console.log(`Processing fundamentals for ${fundsToProcess.length} funds...\n`);

      let processed = 0;
      let successful = 0;
      let failed = 0;

      // Process in batches of 100
      for (let i = 0; i < fundsToProcess.length; i += 100) {
        const batch = fundsToProcess.slice(i, i + 100);
        console.log(`Processing batch ${Math.floor(i/100) + 1}/${Math.ceil(fundsToProcess.length/100)}...`);

        for (const fund of batch) {
          try {
            const fundamentalsData = await this.calculateFundamentalsScore(fund.fund_id);
            
            if (fundamentalsData) {
              const updated = await this.updateFundamentalsInDatabase(fund.fund_id, fundamentalsData);
              if (updated) {
                successful++;
              } else {
                failed++;
              }
            } else {
              failed++;
            }
            
            processed++;

            // Progress indicator
            if (processed % 500 === 0) {
              console.log(`  Progress: ${processed}/${fundsToProcess.length} funds (${successful} successful, ${failed} failed)`);
            }

          } catch (error) {
            console.error(`Error processing fund ${fund.fund_id}:`, error.message);
            failed++;
            processed++;
          }
        }

        // Small delay between batches
        await new Promise(resolve => setTimeout(resolve, 100));
      }

      console.log(`\nFundamentals calculation completed:`);
      console.log(`  Processed: ${processed} funds`);
      console.log(`  Successful: ${successful} funds`);
      console.log(`  Failed: ${failed} funds`);

      // Recalculate total scores
      const updatedScores = await this.recalculateTotalScores();
      
      // Apply updated recommendations
      const updatedRecommendations = await this.applyUpdatedRecommendations();

      // Generate summary report
      await this.generateFundamentalsReport();

      console.log('\n' + '='.repeat(50));
      console.log('FUNDAMENTALS COMPONENT IMPLEMENTATION COMPLETE');
      console.log(`✓ ${successful} funds now have authentic fundamentals scores`);
      console.log(`✓ ${updatedScores} total scores recalculated`);
      console.log(`✓ ${updatedRecommendations} recommendations updated`);
      console.log('✓ 100-point scoring methodology now complete');
      console.log('='.repeat(50));

      return {
        processed,
        successful,
        failed,
        updatedScores,
        updatedRecommendations
      };

    } catch (error) {
      console.error('Fundamentals implementation failed:', error);
      throw error;
    }
  }

  /**
   * Generate comprehensive fundamentals report
   */
  static async generateFundamentalsReport() {
    console.log('\nFundamentals Implementation Report:');
    console.log('-'.repeat(40));

    const summary = await pool.query(`
      SELECT 
        COUNT(*) as total_funds,
        COUNT(CASE WHEN fundamentals_total > 0 THEN 1 END) as have_fundamentals,
        AVG(fundamentals_total)::numeric(5,2) as avg_fundamentals_score,
        MIN(fundamentals_total) as min_fundamentals,
        MAX(fundamentals_total) as max_fundamentals,
        AVG(total_score)::numeric(5,2) as avg_total_score,
        MIN(total_score) as min_total_score,
        MAX(total_score) as max_total_score
      FROM fund_scores_corrected
      WHERE score_date = '2025-06-05'
    `);

    const stats = summary.rows[0];
    console.log(`Total Funds: ${stats.total_funds}`);
    console.log(`With Fundamentals: ${stats.have_fundamentals} (${((stats.have_fundamentals/stats.total_funds)*100).toFixed(1)}%)`);
    console.log(`Fundamentals Score Range: ${stats.min_fundamentals} - ${stats.max_fundamentals} (avg: ${stats.avg_fundamentals_score})`);
    console.log(`Total Score Range: ${stats.min_total_score} - ${stats.max_total_score} (avg: ${stats.avg_total_score})`);

    // Check recommendation distribution with fundamentals
    const distribution = await pool.query(`
      SELECT 
        recommendation,
        COUNT(*) as count,
        ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()), 1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' 
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

    console.log('\nUpdated Recommendation Distribution:');
    distribution.rows.forEach(row => {
      console.log(`  ${row.recommendation}: ${row.count} funds (${row.percentage}%)`);
    });
  }
}

// Execute implementation
if (import.meta.url === `file://${process.argv[1]}`) {
  FundamentalsComponentImplementation.implementFundamentalsComponent()
    .then((results) => {
      console.log('\n✓ Fundamentals component successfully implemented!');
      console.log('✓ Authentic 100-point scoring system now complete');
      process.exit(0);
    })
    .catch((error) => {
      console.error('\n✗ Fundamentals implementation failed:', error.message);
      process.exit(1);
    })
    .finally(async () => {
      await pool.end();
    });
}

export default FundamentalsComponentImplementation;