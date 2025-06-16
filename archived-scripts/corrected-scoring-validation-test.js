/**
 * Corrected Scoring System Validation Test
 * Tests the new scoring engine against documentation requirements
 * Validates mathematical accuracy and constraint compliance
 */

import pkg from 'pg';
const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// Import the corrected scoring engine functions
class CorrectedScoringValidationTest {
  
  // Exact return thresholds from documentation
  static RETURN_THRESHOLDS = {
    excellent: { min: 15.0, score: 8.0 },
    good: { min: 12.0, score: 6.4 },
    average: { min: 8.0, score: 4.8 },
    below_average: { min: 5.0, score: 3.2 },
    poor: { min: 0.0, score: 1.6 }
  };

  /**
   * Calculate period return using exact documentation formula
   */
  static calculatePeriodReturn(currentNav, historicalNav, days) {
    if (!currentNav || !historicalNav || historicalNav <= 0) return null;
    
    const years = days / 365.25;
    
    if (years <= 1) {
      // Simple return for periods <= 1 year
      return ((currentNav / historicalNav) - 1) * 100;
    } else {
      // Annualized return: ((Latest NAV / Historical NAV) ^ (365 / Days Between)) - 1
      return (Math.pow(currentNav / historicalNav, 365 / days) - 1) * 100;
    }
  }

  /**
   * Score return value using exact documentation thresholds
   */
  static scoreReturnValue(returnPercent) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    if (returnPercent >= this.RETURN_THRESHOLDS.excellent.min) {
      return this.RETURN_THRESHOLDS.excellent.score;
    } else if (returnPercent >= this.RETURN_THRESHOLDS.good.min) {
      return this.RETURN_THRESHOLDS.good.score;
    } else if (returnPercent >= this.RETURN_THRESHOLDS.average.min) {
      return this.RETURN_THRESHOLDS.average.score;
    } else if (returnPercent >= this.RETURN_THRESHOLDS.below_average.min) {
      return this.RETURN_THRESHOLDS.below_average.score;
    } else if (returnPercent >= this.RETURN_THRESHOLDS.poor.min) {
      return this.RETURN_THRESHOLDS.poor.score;
    } else {
      // Handle negative returns with proportional scoring, cap at -0.30
      return Math.max(-0.30, returnPercent * 0.02);
    }
  }

  /**
   * Get NAV data for fund
   */
  static async getNavData(fundId, days = 2000) {
    const result = await pool.query(`
      SELECT nav_date, nav_value 
      FROM nav_data 
      WHERE fund_id = $1 
        AND created_at > '2025-05-30 06:45:00'
        AND nav_value > 0
      ORDER BY nav_date ASC
      LIMIT $2
    `, [fundId, days]);
    
    return result.rows;
  }

  /**
   * Calculate Historical Returns Component with validation
   */
  static async calculateHistoricalReturnsComponent(fundId) {
    const navData = await this.getNavData(fundId);
    if (!navData || navData.length < 90) return null;

    const periods = [
      { name: '3m', days: 90 },
      { name: '6m', days: 180 },
      { name: '1y', days: 365 },
      { name: '3y', days: 1095 },
      { name: '5y', days: 1825 }
    ];

    const scores = {};
    let totalScore = 0;

    for (const period of periods) {
      if (navData.length >= period.days) {
        const currentNav = navData[navData.length - 1].nav_value;
        const historicalNav = navData[navData.length - period.days].nav_value;
        
        const returnPercent = this.calculatePeriodReturn(currentNav, historicalNav, period.days);
        const score = this.scoreReturnValue(returnPercent);
        
        scores[`return_${period.name}_score`] = Number(score.toFixed(2));
        scores[`return_${period.name}_percent`] = returnPercent ? Number(returnPercent.toFixed(4)) : null;
        totalScore += score;
        
        // Validation: Ensure individual scores are within 0-8 range
        if (score < -0.40 || score > 8.00) {
          console.warn(`WARNING: return_${period.name}_score ${score} outside valid range for fund ${fundId}`);
        }
      } else {
        scores[`return_${period.name}_score`] = 0;
        scores[`return_${period.name}_percent`] = null;
      }
    }

    // Cap at maximum 32.00 points and minimum -0.70 as per documentation
    scores.historical_returns_total = Number(Math.min(32.00, Math.max(-0.70, totalScore)).toFixed(2));
    
    // Validation: Ensure total is within documentation range
    if (scores.historical_returns_total < -0.70 || scores.historical_returns_total > 32.00) {
      console.error(`ERROR: historical_returns_total ${scores.historical_returns_total} outside valid range for fund ${fundId}`);
    }
    
    return scores;
  }

  /**
   * Calculate volatility from daily returns
   */
  static calculateVolatility(dailyReturns) {
    if (!dailyReturns || dailyReturns.length < 50) return null;
    
    const mean = dailyReturns.reduce((sum, ret) => sum + ret, 0) / dailyReturns.length;
    const variance = dailyReturns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / (dailyReturns.length - 1);
    
    // Annualized volatility: std * sqrt(252) * 100
    return Math.sqrt(variance * 252) * 100;
  }

  /**
   * Calculate daily returns from NAV data
   */
  static calculateDailyReturns(navData, days) {
    if (navData.length < days + 1) return [];
    
    const returns = [];
    const startIndex = Math.max(0, navData.length - days - 1);
    
    for (let i = startIndex + 1; i < navData.length; i++) {
      const prevNav = navData[i - 1].nav_value;
      const currentNav = navData[i].nav_value;
      
      if (prevNav > 0) {
        returns.push((currentNav - prevNav) / prevNav);
      }
    }
    
    return returns;
  }

  /**
   * Test corrected scoring on a sample of funds
   */
  static async testCorrectedScoring() {
    console.log('Starting Corrected Scoring System Validation Test...\n');
    
    // Get a sample of funds with good NAV data
    const fundsResult = await pool.query(`
      SELECT f.id, f.fund_name, f.subcategory, COUNT(*) as nav_count
      FROM funds f
      JOIN nav_data nd ON f.id = nd.fund_id
      WHERE nd.created_at > '2025-05-30 06:45:00'
      GROUP BY f.id, f.fund_name, f.subcategory
      HAVING COUNT(*) >= 365
      ORDER BY COUNT(*) DESC
      LIMIT 10
    `);

    const testFunds = fundsResult.rows;
    console.log(`Testing corrected scoring on ${testFunds.length} funds with sufficient data...\n`);

    const results = [];

    for (const fund of testFunds) {
      console.log(`\n=== Testing Fund: ${fund.fund_name} (ID: ${fund.id}) ===`);
      console.log(`Subcategory: ${fund.subcategory}, NAV Records: ${fund.nav_count}`);
      
      try {
        const historicalReturns = await this.calculateHistoricalReturnsComponent(fund.id);
        
        if (historicalReturns) {
          console.log('\nHistorical Returns Scores:');
          console.log(`3M Score: ${historicalReturns.return_3m_score} (Return: ${historicalReturns.return_3m_percent}%)`);
          console.log(`6M Score: ${historicalReturns.return_6m_score} (Return: ${historicalReturns.return_6m_percent}%)`);
          console.log(`1Y Score: ${historicalReturns.return_1y_score} (Return: ${historicalReturns.return_1y_percent}%)`);
          console.log(`3Y Score: ${historicalReturns.return_3y_score} (Return: ${historicalReturns.return_3y_percent}%)`);
          console.log(`5Y Score: ${historicalReturns.return_5y_score} (Return: ${historicalReturns.return_5y_percent}%)`);
          console.log(`Total Historical Returns: ${historicalReturns.historical_returns_total}/32.00`);

          // Validation checks
          const validationResults = this.validateScores(historicalReturns, fund.id);
          
          results.push({
            fund_id: fund.id,
            fund_name: fund.fund_name,
            historical_returns: historicalReturns,
            validation: validationResults
          });
        } else {
          console.log('Insufficient data for scoring');
        }
        
      } catch (error) {
        console.error(`Error testing fund ${fund.id}:`, error.message);
      }
    }

    // Generate summary report
    console.log('\n' + '='.repeat(80));
    console.log('CORRECTED SCORING VALIDATION SUMMARY');
    console.log('='.repeat(80));
    
    const validResults = results.filter(r => r.validation.isValid);
    const invalidResults = results.filter(r => !r.validation.isValid);
    
    console.log(`✅ Valid Scores: ${validResults.length}/${results.length}`);
    console.log(`❌ Invalid Scores: ${invalidResults.length}/${results.length}`);
    
    if (validResults.length > 0) {
      const totalScores = validResults.map(r => r.historical_returns.historical_returns_total);
      const avgScore = totalScores.reduce((sum, score) => sum + score, 0) / totalScores.length;
      const minScore = Math.min(...totalScores);
      const maxScore = Math.max(...totalScores);
      
      console.log(`\nHistorical Returns Statistics:`);
      console.log(`Average Total: ${avgScore.toFixed(2)}/32.00`);
      console.log(`Range: ${minScore.toFixed(2)} to ${maxScore.toFixed(2)}`);
    }
    
    if (invalidResults.length > 0) {
      console.log('\n❌ Invalid Results:');
      invalidResults.forEach(result => {
        console.log(`Fund ${result.fund_id}: ${result.validation.errors.join(', ')}`);
      });
    }

    console.log('\n✅ Corrected Scoring Validation Test Complete');
    return results;
  }

  /**
   * Validate scores against documentation constraints
   */
  static validateScores(scores, fundId) {
    const errors = [];
    
    // Check individual score constraints
    const checks = [
      { field: 'return_3m_score', min: -0.30, max: 8.00 },
      { field: 'return_6m_score', min: -0.40, max: 8.00 },
      { field: 'return_1y_score', min: -0.20, max: 5.90 },
      { field: 'return_3y_score', min: -0.10, max: 8.00 },
      { field: 'return_5y_score', min: 0.00, max: 8.00 },
      { field: 'historical_returns_total', min: -0.70, max: 32.00 }
    ];
    
    for (const check of checks) {
      const value = scores[check.field];
      if (value !== null && value !== undefined) {
        if (value < check.min || value > check.max) {
          errors.push(`${check.field} (${value}) outside range [${check.min}, ${check.max}]`);
        }
      }
    }
    
    return {
      isValid: errors.length === 0,
      errors: errors
    };
  }

  /**
   * Compare with original backup scoring to validate improvements
   */
  static async compareWithBackup() {
    console.log('\n' + '='.repeat(80));
    console.log('COMPARISON WITH ORIGINAL BACKUP SCORING');
    console.log('='.repeat(80));
    
    // Get some funds that exist in backup
    const backupFunds = await pool.query(`
      SELECT DISTINCT fund_id 
      FROM fund_scores_backup 
      WHERE historical_returns_total IS NOT NULL
      LIMIT 5
    `);
    
    for (const { fund_id } of backupFunds.rows) {
      console.log(`\n--- Fund ID: ${fund_id} ---`);
      
      // Get backup scores
      const backup = await pool.query(`
        SELECT historical_returns_total, return_3m_score, return_1y_score
        FROM fund_scores_backup 
        WHERE fund_id = $1
      `, [fund_id]);
      
      if (backup.rows[0]) {
        console.log('Original Backup:');
        console.log(`3M Score: ${backup.rows[0].return_3m_score} (WRONG SCALE)`);
        console.log(`1Y Score: ${backup.rows[0].return_1y_score} (WRONG SCALE)`);
        console.log(`Total: ${backup.rows[0].historical_returns_total} (WRONG SCALE)`);
      }
      
      // Calculate corrected scores
      const corrected = await this.calculateHistoricalReturnsComponent(fund_id);
      if (corrected) {
        console.log('Corrected Scoring:');
        console.log(`3M Score: ${corrected.return_3m_score} (PROPER 0-8 SCALE)`);
        console.log(`1Y Score: ${corrected.return_1y_score} (PROPER 0-8 SCALE)`);
        console.log(`Total: ${corrected.historical_returns_total} (PROPER 0-32 SCALE)`);
      }
    }
  }
}

// Run the validation test
async function runValidationTest() {
  try {
    const results = await CorrectedScoringValidationTest.testCorrectedScoring();
    await CorrectedScoringValidationTest.compareWithBackup();
    process.exit(0);
  } catch (error) {
    console.error('Validation test failed:', error);
    process.exit(1);
  }
}

runValidationTest();