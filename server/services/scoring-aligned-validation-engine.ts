/**
 * Scoring-Aligned Validation Engine
 * Implements validation framework that integrates with the 100-point scoring methodology
 * Uses only authentic data and proper investment logic
 */

import { pool } from '../db';

export class ScoringAlignedValidationEngine {
  
  /**
   * Create authentic baseline using current scoring system for future validation
   * Archives June 2025 scores for December 2025 validation
   */
  static async createAuthenticBaseline() {
    console.log('Creating authentic baseline from current scoring system...');
    
    try {
      // Archive current scores as genuine baseline for future validation
      const baselineQuery = `
        INSERT INTO scoring_aligned_predictions (
          fund_id, prediction_date, total_score, recommendation, quartile,
          subcategory, subcategory_quartile,
          return_1y_absolute, return_3y_absolute, return_5y_absolute,
          risk_grade_score, fundamentals_score, other_metrics_score,
          validation_horizon_months, score_methodology_version,
          created_at
        )
        SELECT 
          fund_id,
          score_date as prediction_date,
          total_score,
          recommendation,
          quartile,
          subcategory,
          subcategory_quartile,
          return_1y_absolute,
          return_3y_absolute,
          return_5y_absolute,
          risk_grade_score,
          fundamentals_score,
          other_metrics_score,
          6 as validation_horizon_months,
          'FINAL_SPECIFICATION_100_POINTS' as score_methodology_version,
          CURRENT_TIMESTAMP
        FROM fund_scores_corrected 
        WHERE score_date = '2025-06-05'
        AND total_score IS NOT NULL
        AND recommendation IS NOT NULL
        ON CONFLICT (fund_id, prediction_date) DO NOTHING
      `;
      
      // Create table if doesn't exist
      await this.createScoringAlignedPredictionsTable();
      
      const result = await pool.query(baselineQuery);
      console.log(`Created ${result.rowCount} authentic baseline predictions`);
      
      return result.rowCount;
      
    } catch (error) {
      console.error('Error creating authentic baseline:', error);
      throw error;
    }
  }
  
  /**
   * Create scoring-aligned predictions table
   */
  static async createScoringAlignedPredictionsTable() {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS scoring_aligned_predictions (
        id SERIAL PRIMARY KEY,
        fund_id INTEGER NOT NULL,
        prediction_date DATE NOT NULL,
        total_score NUMERIC(6,2),
        recommendation TEXT,
        quartile INTEGER,
        subcategory VARCHAR(100),
        subcategory_quartile INTEGER,
        return_1y_absolute NUMERIC(10,4),
        return_3y_absolute NUMERIC(10,4),
        return_5y_absolute NUMERIC(10,4),
        risk_grade_score NUMERIC(6,2),
        fundamentals_score NUMERIC(6,2),
        other_metrics_score NUMERIC(6,2),
        validation_horizon_months INTEGER DEFAULT 6,
        score_methodology_version TEXT DEFAULT 'FINAL_SPECIFICATION_100_POINTS',
        validated BOOLEAN DEFAULT FALSE,
        validation_date DATE,
        actual_forward_return_6m NUMERIC(10,4),
        actual_forward_return_1y NUMERIC(10,4),
        scoring_prediction_accurate BOOLEAN,
        recommendation_prediction_accurate BOOLEAN,
        quartile_stability BOOLEAN,
        score_correlation_coefficient NUMERIC(8,4),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fund_id, prediction_date)
      )
    `;
    
    await pool.query(createTableQuery);
  }
  
  /**
   * Define proper recommendation accuracy thresholds based on scoring methodology
   */
  static getRecommendationThresholds() {
    return {
      STRONG_BUY: {
        minScore: 70,
        expectedReturn6M: 12,  // 24% annualized
        expectedReturn1Y: 20
      },
      BUY: {
        minScore: 60,
        expectedReturn6M: 8,   // 16% annualized
        expectedReturn1Y: 15
      },
      HOLD: {
        minScore: 40,
        expectedReturn6M: 4,   // 8% annualized
        expectedReturn1Y: 8
      },
      SELL: {
        maxScore: 40,
        expectedReturn6M: 2,   // 4% annualized
        expectedReturn1Y: 5
      },
      STRONG_SELL: {
        maxScore: 30,
        expectedReturn6M: 0,   // 0% annualized
        expectedReturn1Y: 0
      }
    };
  }
  
  /**
   * Calculate forward performance when validation period completes
   */
  static async calculateForwardPerformance(predictionDate: string, validationDate: string) {
    console.log(`Calculating forward performance from ${predictionDate} to ${validationDate}...`);
    
    const forwardPerformanceQuery = `
      UPDATE scoring_aligned_predictions 
      SET 
        validation_date = $2,
        actual_forward_return_6m = (
          SELECT (vd.nav_value - pd.nav_value) / pd.nav_value * 100
          FROM nav_data pd, nav_data vd
          WHERE pd.fund_id = scoring_aligned_predictions.fund_id
          AND vd.fund_id = scoring_aligned_predictions.fund_id
          AND pd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = scoring_aligned_predictions.fund_id AND nav_date <= $1)
          AND vd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = scoring_aligned_predictions.fund_id AND nav_date <= $2)
        ),
        actual_forward_return_1y = (
          SELECT (vd.nav_value - pd.nav_value) / pd.nav_value * 100
          FROM nav_data pd, nav_data vd
          WHERE pd.fund_id = scoring_aligned_predictions.fund_id
          AND vd.fund_id = scoring_aligned_predictions.fund_id
          AND pd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = scoring_aligned_predictions.fund_id AND nav_date <= $1)
          AND vd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = scoring_aligned_predictions.fund_id AND nav_date <= $1 + INTERVAL '1 year')
        )
      WHERE prediction_date = $1
    `;
    
    const result = await pool.query(forwardPerformanceQuery, [predictionDate, validationDate]);
    console.log(`Updated forward performance for ${result.rowCount} predictions`);
    
    return result.rowCount;
  }
  
  /**
   * Validate predictions using scoring methodology thresholds
   */
  static async validatePredictionsUsingScoring(predictionDate: string) {
    console.log(`Validating predictions using scoring methodology for ${predictionDate}...`);
    
    const validationQuery = `
      UPDATE scoring_aligned_predictions
      SET 
        scoring_prediction_accurate = (
          CASE 
            WHEN total_score >= 70 AND actual_forward_return_6m >= 12 THEN true
            WHEN total_score >= 60 AND total_score < 70 AND actual_forward_return_6m >= 8 THEN true
            WHEN total_score >= 40 AND total_score < 60 AND actual_forward_return_6m >= 4 THEN true
            WHEN total_score < 40 AND actual_forward_return_6m <= 5 THEN true
            ELSE false
          END
        ),
        recommendation_prediction_accurate = (
          CASE 
            WHEN recommendation = 'STRONG_BUY' AND actual_forward_return_6m >= 12 THEN true
            WHEN recommendation = 'BUY' AND actual_forward_return_6m >= 8 THEN true
            WHEN recommendation = 'HOLD' AND actual_forward_return_6m BETWEEN 4 AND 12 THEN true
            WHEN recommendation = 'SELL' AND actual_forward_return_6m <= 5 THEN true
            WHEN recommendation = 'STRONG_SELL' AND actual_forward_return_6m <= 0 THEN true
            ELSE false
          END
        ),
        quartile_stability = (
          CASE 
            WHEN quartile = 1 AND actual_forward_return_6m >= 8 THEN true
            WHEN quartile = 2 AND actual_forward_return_6m >= 6 THEN true
            WHEN quartile = 3 AND actual_forward_return_6m >= 4 THEN true
            WHEN quartile = 4 AND actual_forward_return_6m >= 2 THEN true
            ELSE false
          END
        ),
        validated = true
      WHERE prediction_date = $1
      AND actual_forward_return_6m IS NOT NULL
    `;
    
    const result = await pool.query(validationQuery, [predictionDate]);
    return result.rowCount;
  }
  
  /**
   * Calculate comprehensive validation statistics
   */
  static async calculateValidationStatistics(predictionDate: string) {
    console.log(`Calculating validation statistics for ${predictionDate}...`);
    
    const statsQuery = `
      WITH validation_analysis AS (
        SELECT 
          fund_id,
          total_score,
          recommendation,
          quartile,
          actual_forward_return_6m,
          scoring_prediction_accurate,
          recommendation_prediction_accurate,
          quartile_stability
        FROM scoring_aligned_predictions
        WHERE prediction_date = $1
        AND validated = true
        AND actual_forward_return_6m IS NOT NULL
      ),
      comprehensive_stats AS (
        SELECT 
          COUNT(*) as total_validated_predictions,
          
          -- Overall accuracy metrics
          COUNT(CASE WHEN scoring_prediction_accurate = true THEN 1 END) as accurate_score_predictions,
          COUNT(CASE WHEN recommendation_prediction_accurate = true THEN 1 END) as accurate_recommendation_predictions,
          COUNT(CASE WHEN quartile_stability = true THEN 1 END) as stable_quartiles,
          
          -- Accuracy percentages
          AVG(CASE WHEN scoring_prediction_accurate = true THEN 100.0 ELSE 0.0 END) as scoring_accuracy_pct,
          AVG(CASE WHEN recommendation_prediction_accurate = true THEN 100.0 ELSE 0.0 END) as recommendation_accuracy_pct,
          AVG(CASE WHEN quartile_stability = true THEN 100.0 ELSE 0.0 END) as quartile_stability_pct,
          
          -- Performance statistics
          AVG(actual_forward_return_6m) as avg_forward_return,
          STDDEV(actual_forward_return_6m) as stddev_forward_return,
          MIN(actual_forward_return_6m) as min_forward_return,
          MAX(actual_forward_return_6m) as max_forward_return,
          
          -- Score correlation
          CORR(total_score, actual_forward_return_6m) as score_return_correlation
        FROM validation_analysis
      ),
      recommendation_breakdown AS (
        SELECT 
          recommendation,
          COUNT(*) as count,
          COUNT(CASE WHEN recommendation_prediction_accurate = true THEN 1 END) as accurate,
          AVG(actual_forward_return_6m) as avg_return,
          AVG(total_score) as avg_score
        FROM validation_analysis
        GROUP BY recommendation
      )
      SELECT 
        cs.*,
        (
          SELECT JSON_AGG(
            JSON_BUILD_OBJECT(
              'recommendation', recommendation,
              'count', count,
              'accurate', accurate,
              'accuracy_rate', ROUND((accurate::float / count * 100), 2),
              'avg_return', ROUND(avg_return, 2),
              'avg_score', ROUND(avg_score, 2)
            )
          )
          FROM recommendation_breakdown
        ) as recommendation_breakdown
      FROM comprehensive_stats cs
    `;
    
    const result = await pool.query(statsQuery, [predictionDate]);
    return result.rows[0];
  }
  
  /**
   * Store validation results
   */
  static async storeValidationResults(predictionDate: string, validationDate: string, stats: any) {
    const insertQuery = `
      INSERT INTO scoring_methodology_validation_results (
        prediction_date, validation_date, total_funds_validated,
        scoring_accuracy_pct, recommendation_accuracy_pct, quartile_stability_pct,
        score_return_correlation, avg_forward_return, stddev_forward_return,
        min_forward_return, max_forward_return, recommendation_breakdown,
        validation_methodology, created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
      ON CONFLICT (prediction_date, validation_date) DO UPDATE SET
        total_funds_validated = EXCLUDED.total_funds_validated,
        scoring_accuracy_pct = EXCLUDED.scoring_accuracy_pct,
        recommendation_accuracy_pct = EXCLUDED.recommendation_accuracy_pct,
        quartile_stability_pct = EXCLUDED.quartile_stability_pct
    `;
    
    await pool.query(insertQuery, [
      predictionDate,
      validationDate,
      stats.total_validated_predictions,
      stats.scoring_accuracy_pct,
      stats.recommendation_accuracy_pct,
      stats.quartile_stability_pct,
      stats.score_return_correlation,
      stats.avg_forward_return,
      stats.stddev_forward_return,
      stats.min_forward_return,
      stats.max_forward_return,
      stats.recommendation_breakdown,
      'SCORING_METHODOLOGY_ALIGNED_VALIDATION'
    ]);
  }
  
  /**
   * Create validation results table
   */
  static async createValidationResultsTable() {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS scoring_methodology_validation_results (
        id SERIAL PRIMARY KEY,
        prediction_date DATE NOT NULL,
        validation_date DATE NOT NULL,
        total_funds_validated INTEGER NOT NULL,
        scoring_accuracy_pct NUMERIC(5,2),
        recommendation_accuracy_pct NUMERIC(5,2),
        quartile_stability_pct NUMERIC(5,2),
        score_return_correlation NUMERIC(8,4),
        avg_forward_return NUMERIC(10,4),
        stddev_forward_return NUMERIC(10,4),
        min_forward_return NUMERIC(10,4),
        max_forward_return NUMERIC(10,4),
        recommendation_breakdown JSONB,
        validation_methodology TEXT DEFAULT 'SCORING_METHODOLOGY_ALIGNED_VALIDATION',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(prediction_date, validation_date)
      )
    `;
    
    await pool.query(createTableQuery);
  }
  
  /**
   * Initialize complete validation system
   */
  static async initializeValidationSystem() {
    console.log('Initializing scoring-aligned validation system...');
    
    try {
      // Create tables
      await this.createScoringAlignedPredictionsTable();
      await this.createValidationResultsTable();
      
      // Create authentic baseline
      const baselineCount = await this.createAuthenticBaseline();
      
      return {
        success: true,
        baselinePredictions: baselineCount,
        nextValidationDate: '2025-12-05',
        methodology: 'SCORING_METHODOLOGY_ALIGNED',
        message: 'Authentic baseline created for 6-month forward validation'
      };
      
    } catch (error) {
      console.error('Error initializing validation system:', error);
      throw error;
    }
  }
  
  /**
   * Get validation system status
   */
  static async getValidationSystemStatus() {
    const statusQuery = `
      SELECT 
        'Scoring-Aligned Predictions' as component,
        COUNT(*) as records,
        MIN(prediction_date) as earliest_prediction,
        MAX(prediction_date) as latest_prediction,
        COUNT(CASE WHEN validated = true THEN 1 END) as validated_records,
        score_methodology_version
      FROM scoring_aligned_predictions
      GROUP BY score_methodology_version
      
      UNION ALL
      
      SELECT 
        'Validation Results',
        COUNT(*),
        MIN(prediction_date),
        MAX(validation_date),
        COUNT(*),
        validation_methodology
      FROM scoring_methodology_validation_results
      GROUP BY validation_methodology
    `;
    
    const result = await pool.query(statusQuery);
    return result.rows;
  }
}