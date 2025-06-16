/**
 * Fix Realistic Returns Scoring
 * Corrects the historical returns scoring to use realistic market-based thresholds
 * instead of unrealistic 15%+ requirements
 */

import { executeRawQuery } from './server/db.js';

class RealisticReturnsScoringFix {
  
  /**
   * Realistic scoring thresholds based on actual mutual fund performance data
   */
  static REALISTIC_THRESHOLDS = {
    // 1-Year Returns (8 points max)
    '1y': {
      excellent: 12.0,  // Top 10% performers
      good: 8.0,        // Top 25% performers  
      average: 5.0,     // Average market performance
      below_average: 2.0, // Below average but positive
      poor: 0.0,        // Break-even
      negative: -5.0    // Loss threshold
    },
    
    // 3-Year Returns (8 points max)
    '3y': {
      excellent: 15.0,  // Exceptional 3-year performance
      good: 10.0,       // Good 3-year performance
      average: 6.0,     // Average 3-year performance
      below_average: 3.0, // Below average but positive
      poor: 0.0,        // Break-even
      negative: -3.0    // Loss threshold
    },
    
    // 5-Year Returns (8 points max)
    '5y': {
      excellent: 12.0,  // Excellent long-term performance
      good: 8.0,        // Good long-term performance
      average: 5.0,     // Average long-term performance
      below_average: 2.0, // Below average but positive
      poor: 0.0,        // Break-even
      negative: -2.0    // Loss threshold
    },
    
    // Short-term periods (3M, 6M) - more volatile
    'short': {
      excellent: 8.0,   // Strong short-term performance
      good: 4.0,        // Good short-term performance
      average: 1.0,     // Average short-term performance
      below_average: -1.0, // Slight negative
      poor: -3.0,       // Poor performance
      negative: -8.0    // Very poor performance
    }
  };

  /**
   * Calculate realistic score based on return percentage
   */
  static calculateRealisticScore(returnPercent, period) {
    if (returnPercent === null || returnPercent === undefined) return 0;
    
    // Determine which threshold set to use
    let thresholds;
    if (period === '1y') {
      thresholds = this.REALISTIC_THRESHOLDS['1y'];
    } else if (period === '3y') {
      thresholds = this.REALISTIC_THRESHOLDS['3y'];
    } else if (period === '5y') {
      thresholds = this.REALISTIC_THRESHOLDS['5y'];
    } else {
      // 3M, 6M, YTD use short-term thresholds
      thresholds = this.REALISTIC_THRESHOLDS['short'];
    }
    
    // Apply scoring logic with smooth transitions
    if (returnPercent >= thresholds.excellent) {
      return 8.0;
    } else if (returnPercent >= thresholds.good) {
      // Linear interpolation between good and excellent
      const ratio = (returnPercent - thresholds.good) / (thresholds.excellent - thresholds.good);
      return 6.4 + (1.6 * ratio);
    } else if (returnPercent >= thresholds.average) {
      // Linear interpolation between average and good
      const ratio = (returnPercent - thresholds.average) / (thresholds.good - thresholds.average);
      return 4.8 + (1.6 * ratio);
    } else if (returnPercent >= thresholds.below_average) {
      // Linear interpolation between below_average and average
      const ratio = (returnPercent - thresholds.below_average) / (thresholds.average - thresholds.below_average);
      return 3.2 + (1.6 * ratio);
    } else if (returnPercent >= thresholds.poor) {
      // Linear interpolation between poor and below_average
      const ratio = (returnPercent - thresholds.poor) / (thresholds.below_average - thresholds.poor);
      return 1.6 + (1.6 * ratio);
    } else if (returnPercent >= thresholds.negative) {
      // Linear interpolation between negative and poor
      const ratio = (returnPercent - thresholds.negative) / (thresholds.poor - thresholds.negative);
      return 0.0 + (1.6 * ratio);
    } else {
      // Very poor performance - minimal score
      return 0.0;
    }
  }

  /**
   * Update all historical returns scores with realistic thresholds
   */
  static async updateRealisticHistoricalScores() {
    console.log('🔧 Starting realistic returns scoring fix...');
    
    // Get all funds with return data
    const fundsQuery = `
      SELECT 
        fund_id,
        return_3m_absolute,
        return_6m_absolute, 
        return_1y_absolute,
        return_3y_absolute,
        return_5y_absolute
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05'
        AND return_1y_absolute IS NOT NULL
      ORDER BY fund_id
    `;
    
    const fundsResult = await executeRawQuery(fundsQuery);
    console.log(`📊 Processing ${fundsResult.rows.length} funds with return data...`);
    
    let processed = 0;
    const batchSize = 500;
    
    for (let i = 0; i < fundsResult.rows.length; i += batchSize) {
      const batch = fundsResult.rows.slice(i, i + batchSize);
      
      for (const fund of batch) {
        // Calculate realistic scores for each period
        const scores = {
          return_3m_score: this.calculateRealisticScore(fund.return_3m_absolute, '3m'),
          return_6m_score: this.calculateRealisticScore(fund.return_6m_absolute, '6m'),
          return_1y_score: this.calculateRealisticScore(fund.return_1y_absolute, '1y'),
          return_3y_score: this.calculateRealisticScore(fund.return_3y_absolute, '3y'),
          return_5y_score: this.calculateRealisticScore(fund.return_5y_absolute, '5y')
        };
        
        // Calculate new historical returns total
        const historicalTotal = scores.return_3m_score + scores.return_6m_score + 
                               scores.return_1y_score + scores.return_3y_score + 
                               scores.return_5y_score;
        
        // Update the fund's scores
        const updateQuery = `
          UPDATE fund_scores_corrected 
          SET 
            return_3m_score = $2,
            return_6m_score = $3,
            return_1y_score = $4,
            return_3y_score = $5,
            return_5y_score = $6,
            historical_returns_total = $7
          WHERE fund_id = $1 AND score_date = '2025-06-05'
        `;
        
        await executeRawQuery(updateQuery, [
          fund.fund_id,
          scores.return_3m_score.toFixed(2),
          scores.return_6m_score.toFixed(2),
          scores.return_1y_score.toFixed(2),
          scores.return_3y_score.toFixed(2),
          scores.return_5y_score.toFixed(2),
          historicalTotal.toFixed(2)
        ]);
        
        processed++;
      }
      
      console.log(`✅ Processed ${Math.min(processed, fundsResult.rows.length)} / ${fundsResult.rows.length} funds...`);
    }
    
    console.log('📈 Recalculating total scores with improved historical returns...');
    await this.recalculateTotalScores();
    
    console.log('🎯 Updating quartiles and recommendations...');
    await this.updateQuartilesAndRecommendations();
    
    console.log('✅ Realistic returns scoring fix completed!');
    return processed;
  }

  /**
   * Recalculate total scores with new historical returns
   */
  static async recalculateTotalScores() {
    const updateQuery = `
      UPDATE fund_scores_corrected 
      SET total_score = (
        COALESCE(historical_returns_total, 0) +
        COALESCE(risk_grade_total, 0) +
        COALESCE(fundamentals_total, 0) +
        COALESCE(other_metrics_total, 0)
      )
      WHERE score_date = '2025-06-05'
    `;
    
    await executeRawQuery(updateQuery);
    console.log('✅ Total scores recalculated');
  }

  /**
   * Update quartiles and recommendations based on new scores
   */
  static async updateQuartilesAndRecommendations() {
    // Update quartiles based on score ranges
    const quartileQuery = `
      UPDATE fund_scores_corrected 
      SET 
        quartile = CASE 
          WHEN total_score >= 80 THEN 1
          WHEN total_score >= 65 THEN 2
          WHEN total_score >= 50 THEN 3
          ELSE 4
        END,
        recommendation = CASE 
          WHEN total_score >= 80 THEN 'STRONG_BUY'
          WHEN total_score >= 65 THEN 'BUY'
          WHEN total_score >= 50 THEN 'HOLD'
          ELSE 'SELL'
        END
      WHERE score_date = '2025-06-05'
    `;
    
    await executeRawQuery(quartileQuery);
    console.log('✅ Quartiles and recommendations updated');
  }

  /**
   * Generate impact analysis report
   */
  static async generateImpactReport() {
    console.log('\n📊 REALISTIC RETURNS SCORING IMPACT ANALYSIS');
    console.log('=' .repeat(60));
    
    // Before vs After comparison
    const impactQuery = `
      SELECT 
        'UPDATED SCORES' as status,
        COUNT(*) as total_funds,
        AVG(historical_returns_total)::numeric(8,2) as avg_historical_score,
        AVG(total_score)::numeric(8,2) as avg_total_score,
        MIN(total_score)::numeric(8,2) as min_score,
        MAX(total_score)::numeric(8,2) as max_score
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05'
    `;
    
    const result = await executeRawQuery(impactQuery);
    console.table(result.rows);
    
    // New quartile distribution
    const quartileQuery = `
      SELECT 
        'Q' || quartile as quartile_label,
        COUNT(*) as fund_count,
        (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric(5,1) as percentage,
        AVG(total_score)::numeric(8,2) as avg_score
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND quartile IS NOT NULL
      GROUP BY quartile
      ORDER BY quartile
    `;
    
    const quartileResult = await executeRawQuery(quartileQuery);
    console.log('\n📈 NEW QUARTILE DISTRIBUTION:');
    console.table(quartileResult.rows);
    
    // New recommendation distribution
    const recQuery = `
      SELECT 
        recommendation,
        COUNT(*) as fund_count,
        (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER())::numeric(5,1) as percentage
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05' AND recommendation IS NOT NULL
      GROUP BY recommendation
      ORDER BY 
        CASE recommendation 
          WHEN 'STRONG_BUY' THEN 1 
          WHEN 'BUY' THEN 2 
          WHEN 'HOLD' THEN 3 
          WHEN 'SELL' THEN 4 
        END
    `;
    
    const recResult = await executeRawQuery(recQuery);
    console.log('\n🎯 NEW RECOMMENDATION DISTRIBUTION:');
    console.table(recResult.rows);
  }
}

// Execute the fix
async function runRealisticReturnsScoringFix() {
  try {
    const processed = await RealisticReturnsScoringFix.updateRealisticHistoricalScores();
    await RealisticReturnsScoringFix.generateImpactReport();
    
    console.log(`\n🎉 SUCCESS: Fixed realistic returns scoring for ${processed} funds`);
    console.log('✅ Historical returns now properly reflect actual market performance');
    console.log('✅ Scoring thresholds aligned with mutual fund industry standards');
    
  } catch (error) {
    console.error('❌ Error in realistic returns scoring fix:', error);
    throw error;
  }
}

runRealisticReturnsScoringFix();