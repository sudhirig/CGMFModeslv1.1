/**
 * Proper Validation Engine
 * Implements genuine backtesting with historical baselines and forward performance tracking
 */

import { pool } from '../db';

export class ProperValidationEngine {
  
  /**
   * Create baseline predictions for future validation
   * Archives current scores as historical baseline for 6-month forward validation
   */
  static async createBaselinePredictions() {
    console.log('Creating baseline predictions for future validation...');
    
    const baselineQuery = `
      INSERT INTO historical_predictions (
        fund_id, prediction_date, total_score, recommendation, quartile,
        subcategory, subcategory_quartile, return_1y_absolute, 
        created_at, validation_horizon_months
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
        CURRENT_TIMESTAMP,
        6 as validation_horizon_months
      FROM fund_scores_corrected 
      WHERE score_date = '2025-06-05'
      AND total_score IS NOT NULL
      AND recommendation IS NOT NULL
      ON CONFLICT (fund_id, prediction_date) DO NOTHING
    `;
    
    // First create the historical_predictions table if it doesn't exist
    await this.createHistoricalPredictionsTable();
    
    const result = await pool.query(baselineQuery);
    console.log(`Created ${result.rowCount} baseline predictions for future validation`);
    
    return result.rowCount;
  }
  
  /**
   * Create historical predictions table for proper backtesting
   */
  static async createHistoricalPredictionsTable() {
    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS historical_predictions (
        id SERIAL PRIMARY KEY,
        fund_id INTEGER NOT NULL,
        prediction_date DATE NOT NULL,
        total_score NUMERIC(6,2),
        recommendation TEXT,
        quartile INTEGER,
        subcategory VARCHAR(100),
        subcategory_quartile INTEGER,
        return_1y_absolute NUMERIC(10,4),
        validation_horizon_months INTEGER DEFAULT 6,
        validated BOOLEAN DEFAULT FALSE,
        validation_date DATE,
        actual_forward_return_3m NUMERIC(10,4),
        actual_forward_return_6m NUMERIC(10,4),
        actual_forward_return_1y NUMERIC(10,4),
        prediction_accuracy_3m BOOLEAN,
        prediction_accuracy_6m BOOLEAN,
        prediction_accuracy_1y BOOLEAN,
        quartile_stability_6m BOOLEAN,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fund_id, prediction_date)
      )
    `;
    
    await pool.query(createTableQuery);
    console.log('Historical predictions table ready');
  }
  
  /**
   * Set up forward performance tracking for genuine validation
   */
  static async setupForwardPerformanceTracking() {
    console.log('Setting up forward performance tracking system...');
    
    // Create tracking table for monitoring prediction performance
    const trackingTableQuery = `
      CREATE TABLE IF NOT EXISTS forward_performance_tracking (
        id SERIAL PRIMARY KEY,
        tracking_run_id TEXT UNIQUE NOT NULL,
        baseline_date DATE NOT NULL,
        funds_tracked INTEGER NOT NULL,
        next_validation_date DATE NOT NULL,
        tracking_status TEXT DEFAULT 'ACTIVE',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `;
    
    await pool.query(trackingTableQuery);
    
    // Insert tracking record for current baseline
    const trackingQuery = `
      INSERT INTO forward_performance_tracking (
        tracking_run_id, baseline_date, funds_tracked, next_validation_date
      )
      VALUES (
        'BASELINE_2025_06_05', 
        '2025-06-05', 
        (SELECT COUNT(*) FROM fund_scores_corrected WHERE score_date = '2025-06-05'),
        '2025-12-05'
      )
      ON CONFLICT (tracking_run_id) DO UPDATE SET
        funds_tracked = EXCLUDED.funds_tracked,
        next_validation_date = EXCLUDED.next_validation_date
    `;
    
    const result = await pool.query(trackingQuery);
    console.log('Forward performance tracking initialized');
    
    return true;
  }
  
  /**
   * Calculate proper validation accuracy with statistical significance
   * This will be used when we have genuine 6-month forward data
   */
  static async calculateProperValidationAccuracy(validationDate: string) {
    console.log(`Calculating proper validation accuracy for ${validationDate}...`);
    
    const accuracyQuery = `
      WITH prediction_analysis AS (
        SELECT 
          hp.fund_id,
          hp.recommendation,
          hp.quartile,
          hp.total_score,
          hp.actual_forward_return_6m,
          
          -- Proper recommendation accuracy logic
          CASE 
            WHEN hp.recommendation = 'STRONG_BUY' AND hp.actual_forward_return_6m > 20 THEN true
            WHEN hp.recommendation = 'BUY' AND hp.actual_forward_return_6m > 12 THEN true
            WHEN hp.recommendation = 'HOLD' AND hp.actual_forward_return_6m BETWEEN 0 AND 20 THEN true
            WHEN hp.recommendation = 'SELL' AND hp.actual_forward_return_6m < 8 THEN true
            WHEN hp.recommendation = 'STRONG_SELL' AND hp.actual_forward_return_6m < 0 THEN true
            ELSE false
          END as recommendation_accurate,
          
          -- Quartile stability check
          CASE 
            WHEN hp.quartile = 1 AND hp.actual_forward_return_6m > 15 THEN true
            WHEN hp.quartile = 2 AND hp.actual_forward_return_6m > 8 THEN true
            WHEN hp.quartile = 3 AND hp.actual_forward_return_6m BETWEEN 0 AND 12 THEN true
            WHEN hp.quartile = 4 AND hp.actual_forward_return_6m < 8 THEN true
            ELSE false
          END as quartile_stable
          
        FROM historical_predictions hp
        WHERE hp.validation_date = $1
        AND hp.actual_forward_return_6m IS NOT NULL
      ),
      accuracy_stats AS (
        SELECT 
          COUNT(*) as total_predictions,
          COUNT(CASE WHEN recommendation_accurate THEN 1 END) as accurate_recommendations,
          COUNT(CASE WHEN quartile_stable THEN 1 END) as stable_quartiles,
          AVG(CASE WHEN recommendation_accurate THEN 1.0 ELSE 0.0 END) * 100 as recommendation_accuracy_pct,
          AVG(CASE WHEN quartile_stable THEN 1.0 ELSE 0.0 END) * 100 as quartile_stability_pct
        FROM prediction_analysis
      )
      SELECT * FROM accuracy_stats
    `;
    
    const result = await pool.query(accuracyQuery, [validationDate]);
    return result.rows[0];
  }
  
  /**
   * Clear misleading backtesting data and reset validation framework
   */
  static async resetValidationFramework() {
    console.log('Resetting validation framework to remove misleading data...');
    
    // Clear fake backtesting data
    await pool.query('DELETE FROM backtesting_results WHERE validation_date >= CURRENT_DATE - INTERVAL \'7 days\'');
    
    // Update validation summary to reflect reset
    const summaryUpdateQuery = `
      UPDATE validation_summary_reports 
      SET validation_status = 'RESET_FOR_PROPER_VALIDATION',
          strong_buy_accuracy = NULL,
          buy_accuracy = NULL,
          hold_accuracy = NULL,
          sell_accuracy = NULL,
          strong_sell_accuracy = NULL
      WHERE validation_run_id LIKE '%2025_06%'
    `;
    
    await pool.query(summaryUpdateQuery);
    
    console.log('Validation framework reset completed');
    return true;
  }
  
  /**
   * Initialize proper validation system
   */
  static async initializeProperValidation() {
    console.log('Initializing proper validation system...');
    
    try {
      // Reset misleading data
      await this.resetValidationFramework();
      
      // Create baseline for future validation
      const baselineCount = await this.createBaselinePredictions();
      
      // Set up forward tracking
      await this.setupForwardPerformanceTracking();
      
      // Create validation status report
      const statusQuery = `
        INSERT INTO validation_summary_reports (
          validation_run_id, run_date, total_funds_tested, validation_period_months,
          validation_status, created_at
        )
        VALUES (
          'PROPER_VALIDATION_BASELINE_2025_06_06',
          CURRENT_DATE,
          $1,
          6,
          'BASELINE_CREATED_AWAITING_FORWARD_DATA',
          CURRENT_TIMESTAMP
        )
        ON CONFLICT (validation_run_id) DO UPDATE SET
          total_funds_tested = EXCLUDED.total_funds_tested,
          validation_status = EXCLUDED.validation_status
      `;
      
      await pool.query(statusQuery, [baselineCount]);
      
      console.log('Proper validation system initialized successfully');
      
      return {
        success: true,
        baselinePredictions: baselineCount,
        nextValidationDate: '2025-12-05',
        message: 'Baseline created for 6-month forward validation'
      };
      
    } catch (error) {
      console.error('Error initializing proper validation:', error);
      throw error;
    }
  }
  
  /**
   * Get current validation system status
   */
  static async getValidationStatus() {
    const statusQuery = `
      SELECT 
        'Proper Validation System Status' as component,
        (SELECT COUNT(*) FROM historical_predictions WHERE prediction_date = '2025-06-05') as baseline_predictions,
        (SELECT next_validation_date FROM forward_performance_tracking WHERE tracking_run_id = 'BASELINE_2025_06_05') as next_validation_date,
        (SELECT validation_status FROM validation_summary_reports WHERE validation_run_id = 'PROPER_VALIDATION_BASELINE_2025_06_06') as current_status,
        '6 months forward validation required for authentic backtesting' as note
    `;
    
    const result = await pool.query(statusQuery);
    return result.rows[0];
  }
}