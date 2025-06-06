/**
 * Authentic Validation Engine
 * Implements proper backtesting with point-in-time scoring and forward performance tracking
 * Based on original documentation requirements
 */

import { pool } from '../db';

export class AuthenticValidationEngine {
  
  /**
   * Create point-in-time historical scoring baseline
   * Calculates scores using only data available at the historical date
   */
  static async createPointInTimeBaseline(historicalDate: string, validationHorizonMonths: number = 6) {
    console.log(`Creating point-in-time baseline for ${historicalDate}...`);
    
    try {
      // Calculate scores using only NAV data available up to historical date
      const pointInTimeQuery = `
        WITH historical_nav_data AS (
          SELECT 
            f.id as fund_id,
            f.fund_name,
            f.category,
            f.subcategory,
            COUNT(nd.nav_value) as nav_count,
            
            -- Calculate returns using only data up to historical date
            CASE 
              WHEN COUNT(nd.nav_value) >= 252 THEN
                (SELECT (latest.nav_value - earliest.nav_value) / earliest.nav_value * 100
                 FROM nav_data latest, nav_data earliest
                 WHERE latest.fund_id = f.id AND earliest.fund_id = f.id
                 AND latest.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id AND nav_date <= $1)
                 AND earliest.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id AND nav_date <= $1 - INTERVAL '1 year'))
            END as return_1y_historical,
            
            CASE 
              WHEN COUNT(nd.nav_value) >= 63 THEN
                (SELECT (latest.nav_value - earliest.nav_value) / earliest.nav_value * 100
                 FROM nav_data latest, nav_data earliest
                 WHERE latest.fund_id = f.id AND earliest.fund_id = f.id
                 AND latest.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id AND nav_date <= $1)
                 AND earliest.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = f.id AND nav_date <= $1 - INTERVAL '3 months'))
            END as return_3m_historical,
            
            -- Calculate volatility using historical data only
            CASE 
              WHEN COUNT(nd.nav_value) >= 252 THEN
                (SELECT STDDEV(daily_return) * SQRT(252)
                 FROM (
                   SELECT (nav_value / LAG(nav_value) OVER (ORDER BY nav_date) - 1) * 100 as daily_return
                   FROM nav_data 
                   WHERE fund_id = f.id AND nav_date <= $1
                 ) daily_returns
                 WHERE daily_return IS NOT NULL)
            END as volatility_1y_historical
            
          FROM funds f
          LEFT JOIN nav_data nd ON f.id = nd.fund_id AND nd.nav_date <= $1
          WHERE f.status = 'Active'
          GROUP BY f.id, f.fund_name, f.category, f.subcategory
          HAVING COUNT(nd.nav_value) >= 63  -- Minimum 3 months data
        ),
        historical_scores AS (
          SELECT 
            fund_id,
            fund_name,
            category,
            subcategory,
            nav_count,
            return_1y_historical,
            return_3m_historical,
            volatility_1y_historical,
            
            -- Calculate historical scores based on available data
            CASE 
              WHEN return_1y_historical IS NOT NULL THEN
                LEAST(40, GREATEST(0, 20 + (return_1y_historical - 12) * 1.5))
              ELSE NULL
            END as historical_return_score,
            
            CASE 
              WHEN volatility_1y_historical IS NOT NULL THEN
                LEAST(30, GREATEST(0, 30 - (volatility_1y_historical - 15) * 0.8))
              ELSE NULL
            END as historical_risk_score,
            
            15 as historical_other_score  -- Base score for other metrics
            
          FROM historical_nav_data
        ),
        final_historical_scores AS (
          SELECT 
            *,
            COALESCE(historical_return_score, 0) + 
            COALESCE(historical_risk_score, 0) + 
            historical_other_score as total_historical_score,
            
            -- Generate historical recommendations
            CASE 
              WHEN (COALESCE(historical_return_score, 0) + COALESCE(historical_risk_score, 0) + historical_other_score) >= 70 THEN 'STRONG_BUY'
              WHEN (COALESCE(historical_return_score, 0) + COALESCE(historical_risk_score, 0) + historical_other_score) >= 55 THEN 'BUY'
              WHEN (COALESCE(historical_return_score, 0) + COALESCE(historical_risk_score, 0) + historical_other_score) >= 40 THEN 'HOLD'
              WHEN (COALESCE(historical_return_score, 0) + COALESCE(historical_risk_score, 0) + historical_other_score) >= 25 THEN 'SELL'
              ELSE 'STRONG_SELL'
            END as historical_recommendation
            
          FROM historical_scores
        )
        INSERT INTO point_in_time_predictions (
          fund_id, prediction_date, total_score, recommendation, 
          return_1y_historical, return_3m_historical, volatility_1y_historical,
          validation_horizon_months, nav_count_at_prediction,
          created_at
        )
        SELECT 
          fund_id, $1::date, total_historical_score, historical_recommendation,
          return_1y_historical, return_3m_historical, volatility_1y_historical,
          $2, nav_count, CURRENT_TIMESTAMP
        FROM final_historical_scores
        WHERE total_historical_score IS NOT NULL
        ON CONFLICT (fund_id, prediction_date) DO UPDATE SET
          total_score = EXCLUDED.total_score,
          recommendation = EXCLUDED.recommendation
      `;
      
      // First create the table if it doesn't exist
      await this.createPointInTimePredictionsTable();
      
      const result = await pool.query(pointInTimeQuery, [historicalDate, validationHorizonMonths]);
      console.log(`Created ${result.rowCount} point-in-time predictions for ${historicalDate}`);
      
      return result.rowCount;
      
    } catch (error) {
      console.error('Error creating point-in-time baseline:', error);
      throw error;
    }
  }
  
  /**
   * Create point-in-time predictions table
   */
  static async createPointInTimePredictionsTable() {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS point_in_time_predictions (
        id SERIAL PRIMARY KEY,
        fund_id INTEGER NOT NULL,
        prediction_date DATE NOT NULL,
        total_score NUMERIC(6,2),
        recommendation TEXT,
        return_1y_historical NUMERIC(10,4),
        return_3m_historical NUMERIC(10,4),
        volatility_1y_historical NUMERIC(10,4),
        validation_horizon_months INTEGER DEFAULT 6,
        nav_count_at_prediction INTEGER,
        validated BOOLEAN DEFAULT FALSE,
        validation_date DATE,
        actual_forward_return_3m NUMERIC(10,4),
        actual_forward_return_6m NUMERIC(10,4),
        actual_forward_return_1y NUMERIC(10,4),
        prediction_accuracy_3m BOOLEAN,
        prediction_accuracy_6m BOOLEAN,
        prediction_accuracy_1y BOOLEAN,
        quartile_at_prediction INTEGER,
        quartile_at_validation INTEGER,
        quartile_stability BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fund_id, prediction_date)
      )
    `;
    
    await pool.query(createTableQuery);
  }
  
  /**
   * Track forward performance for validation
   */
  static async trackForwardPerformance(predictionDate: string, validationDate: string) {
    console.log(`Tracking forward performance from ${predictionDate} to ${validationDate}...`);
    
    const forwardPerformanceQuery = `
      UPDATE point_in_time_predictions 
      SET 
        validation_date = $2,
        actual_forward_return_3m = (
          SELECT (vd.nav_value - pd.nav_value) / pd.nav_value * 100
          FROM nav_data pd, nav_data vd
          WHERE pd.fund_id = point_in_time_predictions.fund_id
          AND vd.fund_id = point_in_time_predictions.fund_id
          AND pd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1)
          AND vd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1 + INTERVAL '3 months')
        ),
        actual_forward_return_6m = (
          SELECT (vd.nav_value - pd.nav_value) / pd.nav_value * 100
          FROM nav_data pd, nav_data vd
          WHERE pd.fund_id = point_in_time_predictions.fund_id
          AND vd.fund_id = point_in_time_predictions.fund_id
          AND pd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1)
          AND vd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1 + INTERVAL '6 months')
        ),
        actual_forward_return_1y = (
          SELECT (vd.nav_value - pd.nav_value) / pd.nav_value * 100
          FROM nav_data pd, nav_data vd
          WHERE pd.fund_id = point_in_time_predictions.fund_id
          AND vd.fund_id = point_in_time_predictions.fund_id
          AND pd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1)
          AND vd.nav_date = (SELECT MAX(nav_date) FROM nav_data WHERE fund_id = point_in_time_predictions.fund_id AND nav_date <= $1 + INTERVAL '1 year')
        )
      WHERE prediction_date = $1
    `;
    
    const result = await pool.query(forwardPerformanceQuery, [predictionDate, validationDate]);
    console.log(`Updated forward performance for ${result.rowCount} predictions`);
    
    return result.rowCount;
  }
  
  /**
   * Calculate authentic validation accuracy using forward performance
   */
  static async calculateValidationAccuracy(predictionDate: string) {
    console.log(`Calculating validation accuracy for predictions from ${predictionDate}...`);
    
    const accuracyQuery = `
      WITH prediction_validation AS (
        SELECT 
          fund_id,
          total_score,
          recommendation,
          actual_forward_return_6m,
          
          -- Authentic recommendation accuracy based on forward performance
          CASE 
            WHEN recommendation = 'STRONG_BUY' AND total_score >= 70 AND actual_forward_return_6m > 15 THEN true
            WHEN recommendation = 'BUY' AND total_score >= 55 AND actual_forward_return_6m > 10 THEN true
            WHEN recommendation = 'HOLD' AND total_score >= 40 AND actual_forward_return_6m BETWEEN 5 AND 15 THEN true
            WHEN recommendation = 'SELL' AND total_score < 40 AND actual_forward_return_6m < 8 THEN true
            WHEN recommendation = 'STRONG_SELL' AND total_score < 25 AND actual_forward_return_6m < 0 THEN true
            ELSE false
          END as prediction_accurate_6m,
          
          -- Score correlation validation
          CASE 
            WHEN total_score > 60 AND actual_forward_return_6m > 12 THEN true
            WHEN total_score BETWEEN 40 AND 60 AND actual_forward_return_6m BETWEEN 6 AND 15 THEN true
            WHEN total_score < 40 AND actual_forward_return_6m < 10 THEN true
            ELSE false
          END as score_correlation_valid
          
        FROM point_in_time_predictions
        WHERE prediction_date = $1
        AND actual_forward_return_6m IS NOT NULL
      ),
      accuracy_statistics AS (
        SELECT 
          COUNT(*) as total_predictions,
          COUNT(CASE WHEN prediction_accurate_6m THEN 1 END) as accurate_predictions,
          COUNT(CASE WHEN score_correlation_valid THEN 1 END) as valid_correlations,
          AVG(CASE WHEN prediction_accurate_6m THEN 1.0 ELSE 0.0 END) as accuracy_rate,
          AVG(CASE WHEN score_correlation_valid THEN 1.0 ELSE 0.0 END) as correlation_rate
        FROM prediction_validation
      ),
      confidence_intervals AS (
        SELECT 
          *,
          1.96 * SQRT((accuracy_rate * (1 - accuracy_rate)) / total_predictions) as accuracy_ci_margin,
          1.96 * SQRT((correlation_rate * (1 - correlation_rate)) / total_predictions) as correlation_ci_margin
        FROM accuracy_statistics
      )
      SELECT 
        total_predictions,
        accurate_predictions,
        valid_correlations,
        ROUND(accuracy_rate * 100, 2) as accuracy_percentage,
        ROUND(correlation_rate * 100, 2) as correlation_percentage,
        ROUND(accuracy_ci_margin * 100, 2) as accuracy_confidence_margin,
        ROUND(correlation_ci_margin * 100, 2) as correlation_confidence_margin,
        CASE 
          WHEN total_predictions >= 1000 THEN 'STATISTICALLY_SIGNIFICANT'
          WHEN total_predictions >= 100 THEN 'MODERATELY_SIGNIFICANT'
          ELSE 'INSUFFICIENT_SAMPLE'
        END as statistical_significance
      FROM confidence_intervals
    `;
    
    const result = await pool.query(accuracyQuery, [predictionDate]);
    return result.rows[0];
  }
  
  /**
   * Update prediction accuracy flags in database
   */
  static async updatePredictionAccuracy(predictionDate: string) {
    const updateQuery = `
      UPDATE point_in_time_predictions
      SET 
        prediction_accuracy_6m = (
          CASE 
            WHEN recommendation = 'STRONG_BUY' AND total_score >= 70 AND actual_forward_return_6m > 15 THEN true
            WHEN recommendation = 'BUY' AND total_score >= 55 AND actual_forward_return_6m > 10 THEN true
            WHEN recommendation = 'HOLD' AND total_score >= 40 AND actual_forward_return_6m BETWEEN 5 AND 15 THEN true
            WHEN recommendation = 'SELL' AND total_score < 40 AND actual_forward_return_6m < 8 THEN true
            WHEN recommendation = 'STRONG_SELL' AND total_score < 25 AND actual_forward_return_6m < 0 THEN true
            ELSE false
          END
        ),
        validated = true
      WHERE prediction_date = $1
      AND actual_forward_return_6m IS NOT NULL
    `;
    
    const result = await pool.query(updateQuery, [predictionDate]);
    return result.rowCount;
  }
  
  /**
   * Create authentic backtesting validation record
   */
  static async createAuthenticValidationRecord(predictionDate: string, validationDate: string) {
    console.log(`Creating authentic validation record for ${predictionDate} -> ${validationDate}...`);
    
    try {
      // Calculate accuracy statistics
      const accuracyStats = await this.calculateValidationAccuracy(predictionDate);
      
      // Insert validation summary
      const insertQuery = `
        INSERT INTO authentic_backtesting_results (
          prediction_date, validation_date, total_funds_tested,
          validation_period_months, overall_accuracy_6m,
          score_correlation_6m, statistical_significance,
          accuracy_confidence_margin, correlation_confidence_margin,
          validation_methodology, created_at
        )
        VALUES ($1, $2, $3, 6, $4, $5, $6, $7, $8, 'POINT_IN_TIME_FORWARD_VALIDATION', CURRENT_TIMESTAMP)
        ON CONFLICT (prediction_date, validation_date) DO UPDATE SET
          total_funds_tested = EXCLUDED.total_funds_tested,
          overall_accuracy_6m = EXCLUDED.overall_accuracy_6m,
          score_correlation_6m = EXCLUDED.score_correlation_6m,
          statistical_significance = EXCLUDED.statistical_significance
      `;
      
      await pool.query(insertQuery, [
        predictionDate,
        validationDate,
        accuracyStats.total_predictions,
        accuracyStats.accuracy_percentage,
        accuracyStats.correlation_percentage,
        accuracyStats.statistical_significance,
        accuracyStats.accuracy_confidence_margin,
        accuracyStats.correlation_confidence_margin
      ]);
      
      console.log('Authentic validation record created successfully');
      return accuracyStats;
      
    } catch (error) {
      console.error('Error creating validation record:', error);
      throw error;
    }
  }
  
  /**
   * Create authentic backtesting results table
   */
  static async createAuthenticBacktestingTable() {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS authentic_backtesting_results (
        id SERIAL PRIMARY KEY,
        prediction_date DATE NOT NULL,
        validation_date DATE NOT NULL,
        total_funds_tested INTEGER NOT NULL,
        validation_period_months INTEGER NOT NULL,
        overall_accuracy_6m NUMERIC(5,2),
        score_correlation_6m NUMERIC(5,2),
        statistical_significance TEXT,
        accuracy_confidence_margin NUMERIC(5,2),
        correlation_confidence_margin NUMERIC(5,2),
        validation_methodology TEXT DEFAULT 'POINT_IN_TIME_FORWARD_VALIDATION',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(prediction_date, validation_date)
      )
    `;
    
    await pool.query(createTableQuery);
  }
  
  /**
   * Get validation system status
   */
  static async getValidationSystemStatus() {
    const statusQuery = `
      SELECT 
        'Point-in-Time Predictions' as component,
        COUNT(*) as records,
        MIN(prediction_date) as earliest_prediction,
        MAX(prediction_date) as latest_prediction,
        COUNT(CASE WHEN validated = true THEN 1 END) as validated_records
      FROM point_in_time_predictions
      
      UNION ALL
      
      SELECT 
        'Authentic Backtesting Results',
        COUNT(*),
        MIN(prediction_date),
        MAX(validation_date),
        COUNT(*)
      FROM authentic_backtesting_results
      
      UNION ALL
      
      SELECT 
        'Forward Performance Tracking',
        COUNT(CASE WHEN actual_forward_return_6m IS NOT NULL THEN 1 END),
        NULL,
        NULL,
        COUNT(CASE WHEN prediction_accuracy_6m IS NOT NULL THEN 1 END)
      FROM point_in_time_predictions
    `;
    
    const result = await pool.query(statusQuery);
    return result.rows;
  }
}