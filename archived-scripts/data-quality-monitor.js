/**
 * Real-time Data Quality Monitoring System
 * Automated monitoring for data freshness, performance metrics validation, and pipeline health
 */

import { Pool } from 'pg';
import cron from 'node-cron';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

class DataQualityMonitor {
  constructor() {
    this.monitoringResults = {
      dataFreshness: {},
      performanceMetrics: {},
      pipelineHealth: {},
      lastCheck: null
    };
  }

  /**
   * Start real-time monitoring with scheduled checks
   */
  startMonitoring() {
    console.log('🚀 Starting Real-time Data Quality Monitoring System');
    
    // Check every 15 minutes for critical data freshness
    cron.schedule('*/15 * * * *', async () => {
      await this.checkDataFreshness();
    });

    // Validate performance metrics every hour
    cron.schedule('0 * * * *', async () => {
      await this.validatePerformanceMetrics();
    });

    // Monitor pipeline health every 5 minutes
    cron.schedule('*/5 * * * *', async () => {
      await this.monitorPipelineHealth();
    });

    // Comprehensive health check every 4 hours
    cron.schedule('0 */4 * * *', async () => {
      await this.comprehensiveHealthCheck();
    });

    // Initial check on startup
    this.comprehensiveHealthCheck();
  }

  /**
   * Automated Data Freshness Checks
   */
  async checkDataFreshness() {
    try {
      console.log('📊 Checking data freshness...');
      
      const client = await pool.connect();
      
      // Check NAV data freshness
      const navFreshnessQuery = `
        SELECT 
          'NAV_DATA_FRESHNESS' as check_type,
          COUNT(*) as total_records,
          MAX(nav_date) as latest_nav_date,
          COUNT(CASE WHEN nav_date >= CURRENT_DATE - INTERVAL '7 days' THEN 1 END) as recent_records,
          COUNT(DISTINCT fund_id) as funds_with_recent_data,
          EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))/3600 as hours_since_last_update
        FROM nav_data;
      `;
      
      const navResult = await client.query(navFreshnessQuery);
      
      // Check fund scores freshness
      const scoresFreshnessQuery = `
        SELECT 
          'FUND_SCORES_FRESHNESS' as check_type,
          COUNT(*) as total_scores,
          MAX(score_date) as latest_score_date,
          COUNT(CASE WHEN score_date >= CURRENT_DATE - INTERVAL '1 day' THEN 1 END) as recent_scores,
          EXTRACT(EPOCH FROM (NOW() - MAX(created_at)))/3600 as hours_since_last_calculation
        FROM fund_scores;
      `;
      
      const scoresResult = await client.query(scoresFreshnessQuery);
      
      // Check fund details freshness
      const fundDetailsQuery = `
        SELECT 
          'FUND_DETAILS_FRESHNESS' as check_type,
          COUNT(*) as total_funds,
          COUNT(CASE WHEN fund_manager != 'Fund Manager Name' THEN 1 END) as authentic_managers,
          COUNT(CASE WHEN benchmark_name != 'Nifty 50 TRI' THEN 1 END) as authentic_benchmarks,
          COUNT(CASE WHEN minimum_additional IS NOT NULL THEN 1 END) as complete_investment_data,
          EXTRACT(EPOCH FROM (NOW() - MAX(updated_at)))/3600 as hours_since_last_update
        FROM funds;
      `;
      
      const fundsResult = await client.query(fundDetailsQuery);
      
      client.release();
      
      // Analyze freshness results
      const navData = navResult.rows[0];
      const scoresData = scoresResult.rows[0];
      const fundsData = fundsResult.rows[0];
      
      this.monitoringResults.dataFreshness = {
        timestamp: new Date(),
        navData: {
          ...navData,
          status: this.assessDataFreshness(navData.hours_since_last_update, 24), // Alert if no updates in 24 hours
          alertLevel: navData.hours_since_last_update > 24 ? 'HIGH' : navData.hours_since_last_update > 12 ? 'MEDIUM' : 'LOW'
        },
        scoresData: {
          ...scoresData,
          status: this.assessDataFreshness(scoresData.hours_since_last_calculation, 48), // Alert if no calculations in 48 hours
          alertLevel: scoresData.hours_since_last_calculation > 48 ? 'HIGH' : scoresData.hours_since_last_calculation > 24 ? 'MEDIUM' : 'LOW'
        },
        fundsData: {
          ...fundsData,
          status: fundsData.authentic_managers > 16000 ? 'FRESH' : 'STALE',
          alertLevel: fundsData.authentic_managers < 16000 ? 'HIGH' : 'LOW'
        }
      };
      
      // Log alerts for stale data
      if (this.monitoringResults.dataFreshness.navData.alertLevel !== 'LOW') {
        console.warn(`⚠️  NAV Data Alert: ${navData.hours_since_last_update} hours since last update`);
      }
      
      console.log('✅ Data freshness check completed');
      
    } catch (error) {
      console.error('❌ Data freshness check failed:', error.message);
      this.monitoringResults.dataFreshness.error = error.message;
    }
  }

  /**
   * Performance Metric Calculation Validation
   */
  async validatePerformanceMetrics() {
    try {
      console.log('🔍 Validating performance metrics...');
      
      const client = await pool.connect();
      
      // Validate fund scores calculation consistency
      const calculationValidationQuery = `
        WITH score_validation AS (
          SELECT 
            fund_id,
            total_score,
            (COALESCE(historical_returns_total, 0) + COALESCE(risk_grade_total, 0) + COALESCE(fundamentals_total, 0)) as calculated_total,
            ABS(total_score - (COALESCE(historical_returns_total, 0) + COALESCE(risk_grade_total, 0) + COALESCE(fundamentals_total, 0))) as calculation_error
          FROM fund_scores
          WHERE score_date = CURRENT_DATE
        )
        SELECT 
          'CALCULATION_VALIDATION' as check_type,
          COUNT(*) as total_funds,
          COUNT(CASE WHEN calculation_error > 0.1 THEN 1 END) as funds_with_errors,
          MAX(calculation_error) as max_error,
          AVG(calculation_error) as avg_error,
          COUNT(CASE WHEN total_score BETWEEN 0 AND 100 THEN 1 END) as valid_score_range
        FROM score_validation;
      `;
      
      const calculationResult = await client.query(calculationValidationQuery);
      
      // Validate quartile distribution
      const quartileValidationQuery = `
        SELECT 
          'QUARTILE_DISTRIBUTION' as check_type,
          quartile,
          COUNT(*) as fund_count,
          ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM fund_scores
        WHERE score_date = CURRENT_DATE
        GROUP BY quartile
        ORDER BY quartile;
      `;
      
      const quartileResult = await client.query(quartileValidationQuery);
      
      // Validate performance metrics data quality
      const metricsQualityQuery = `
        SELECT 
          'METRICS_QUALITY' as check_type,
          COUNT(*) as total_records,
          COUNT(CASE WHEN volatility IS NOT NULL THEN 1 END) as has_volatility,
          COUNT(CASE WHEN sharpe_ratio IS NOT NULL THEN 1 END) as has_sharpe_ratio,
          COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) as good_quality_records,
          ROUND(AVG(data_quality_score), 3) as avg_quality_score
        FROM fund_performance_metrics;
      `;
      
      const qualityResult = await client.query(metricsQualityQuery);
      
      client.release();
      
      // Analyze validation results
      const calculationData = calculationResult.rows[0];
      const quartileData = quartileResult.rows;
      const qualityData = qualityResult.rows[0];
      
      this.monitoringResults.performanceMetrics = {
        timestamp: new Date(),
        calculationAccuracy: {
          ...calculationData,
          status: calculationData.funds_with_errors === 0 ? 'ACCURATE' : 'NEEDS_CORRECTION',
          alertLevel: calculationData.funds_with_errors > 0 ? 'HIGH' : 'LOW'
        },
        quartileBalance: {
          data: quartileData,
          status: this.validateQuartileBalance(quartileData),
          alertLevel: this.validateQuartileBalance(quartileData) !== 'BALANCED' ? 'MEDIUM' : 'LOW'
        },
        dataQuality: {
          ...qualityData,
          volatilityCoverage: Math.round((qualityData.has_volatility / qualityData.total_records) * 100),
          sharpeCoverage: Math.round((qualityData.has_sharpe_ratio / qualityData.total_records) * 100),
          status: qualityData.avg_quality_score >= 0.7 ? 'GOOD' : 'NEEDS_IMPROVEMENT',
          alertLevel: qualityData.avg_quality_score < 0.5 ? 'HIGH' : qualityData.avg_quality_score < 0.7 ? 'MEDIUM' : 'LOW'
        }
      };
      
      // Log alerts for metric issues
      if (calculationData.funds_with_errors > 0) {
        console.warn(`⚠️  Calculation Error Alert: ${calculationData.funds_with_errors} funds have score calculation errors`);
      }
      
      console.log('✅ Performance metrics validation completed');
      
    } catch (error) {
      console.error('❌ Performance metrics validation failed:', error.message);
      this.monitoringResults.performanceMetrics.error = error.message;
    }
  }

  /**
   * Pipeline Health Monitoring
   */
  async monitorPipelineHealth() {
    try {
      console.log('🔧 Monitoring pipeline health...');
      
      const client = await pool.connect();
      
      // Check current pipeline status
      const pipelineStatusQuery = `
        SELECT 
          'PIPELINE_STATUS' as check_type,
          status,
          COUNT(*) as pipeline_count,
          MAX(start_time) as latest_start,
          AVG(EXTRACT(EPOCH FROM (COALESCE(end_time, NOW()) - start_time))/60) as avg_duration_minutes
        FROM etl_pipeline_runs
        WHERE start_time >= NOW() - INTERVAL '24 hours'
        GROUP BY status
        ORDER BY COUNT(*) DESC;
      `;
      
      const statusResult = await client.query(pipelineStatusQuery);
      
      // Check for stuck pipelines
      const stuckPipelinesQuery = `
        SELECT 
          'STUCK_PIPELINES' as check_type,
          COUNT(*) as stuck_count,
          string_agg(pipeline_name, ', ') as stuck_pipeline_names
        FROM etl_pipeline_runs
        WHERE status = 'RUNNING' 
          AND start_time < NOW() - INTERVAL '2 hours';
      `;
      
      const stuckResult = await client.query(stuckPipelinesQuery);
      
      // Check recent pipeline success rate
      const successRateQuery = `
        SELECT 
          'SUCCESS_RATE' as check_type,
          COUNT(*) as total_runs,
          COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_runs,
          COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_runs,
          ROUND(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) * 100.0 / COUNT(*), 2) as success_rate_percent
        FROM etl_pipeline_runs
        WHERE start_time >= NOW() - INTERVAL '24 hours';
      `;
      
      const successResult = await client.query(successRateQuery);
      
      client.release();
      
      // Analyze pipeline health
      const statusData = statusResult.rows;
      const stuckData = stuckResult.rows[0];
      const successData = successResult.rows[0];
      
      this.monitoringResults.pipelineHealth = {
        timestamp: new Date(),
        currentStatus: {
          data: statusData,
          status: stuckData.stuck_count > 0 ? 'DEGRADED' : 'HEALTHY',
          alertLevel: stuckData.stuck_count > 3 ? 'HIGH' : stuckData.stuck_count > 0 ? 'MEDIUM' : 'LOW'
        },
        stuckPipelines: {
          ...stuckData,
          status: stuckData.stuck_count === 0 ? 'CLEAR' : 'BLOCKED',
          alertLevel: stuckData.stuck_count > 0 ? 'HIGH' : 'LOW'
        },
        successRate: {
          ...successData,
          status: successData.success_rate_percent >= 90 ? 'EXCELLENT' : 
                  successData.success_rate_percent >= 75 ? 'GOOD' : 'POOR',
          alertLevel: successData.success_rate_percent < 75 ? 'HIGH' : 
                     successData.success_rate_percent < 90 ? 'MEDIUM' : 'LOW'
        }
      };
      
      // Log alerts for pipeline issues
      if (stuckData.stuck_count > 0) {
        console.warn(`⚠️  Pipeline Alert: ${stuckData.stuck_count} stuck pipelines detected: ${stuckData.stuck_pipeline_names}`);
      }
      
      if (successData.success_rate_percent < 90) {
        console.warn(`⚠️  Success Rate Alert: Only ${successData.success_rate_percent}% pipeline success rate in last 24 hours`);
      }
      
      console.log('✅ Pipeline health monitoring completed');
      
    } catch (error) {
      console.error('❌ Pipeline health monitoring failed:', error.message);
      this.monitoringResults.pipelineHealth.error = error.message;
    }
  }

  /**
   * Comprehensive Health Check
   */
  async comprehensiveHealthCheck() {
    console.log('🏥 Running comprehensive health check...');
    
    await this.checkDataFreshness();
    await this.validatePerformanceMetrics();
    await this.monitorPipelineHealth();
    
    // Generate overall system health summary
    const overallHealth = this.calculateOverallHealth();
    
    this.monitoringResults.lastCheck = new Date();
    this.monitoringResults.overallHealth = overallHealth;
    
    console.log(`🎯 Overall System Health: ${overallHealth.status} (Score: ${overallHealth.score}/100)`);
    
    // Store monitoring results in database for dashboard
    await this.storeMonitoringResults();
  }

  /**
   * Calculate overall system health score
   */
  calculateOverallHealth() {
    let totalScore = 0;
    let maxScore = 100;
    
    const { dataFreshness, performanceMetrics, pipelineHealth } = this.monitoringResults;
    
    // Data freshness score (40 points)
    if (dataFreshness.navData?.alertLevel === 'LOW') totalScore += 15;
    else if (dataFreshness.navData?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (dataFreshness.navData?.alertLevel === 'HIGH') totalScore += 5;
    
    if (dataFreshness.scoresData?.alertLevel === 'LOW') totalScore += 15;
    else if (dataFreshness.scoresData?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (dataFreshness.scoresData?.alertLevel === 'HIGH') totalScore += 5;
    
    if (dataFreshness.fundsData?.alertLevel === 'LOW') totalScore += 10;
    else if (dataFreshness.fundsData?.alertLevel === 'MEDIUM') totalScore += 5;
    
    // Performance metrics score (30 points)
    if (performanceMetrics.calculationAccuracy?.alertLevel === 'LOW') totalScore += 15;
    else if (performanceMetrics.calculationAccuracy?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (performanceMetrics.calculationAccuracy?.alertLevel === 'HIGH') totalScore += 5;
    
    if (performanceMetrics.dataQuality?.alertLevel === 'LOW') totalScore += 15;
    else if (performanceMetrics.dataQuality?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (performanceMetrics.dataQuality?.alertLevel === 'HIGH') totalScore += 5;
    
    // Pipeline health score (30 points)
    if (pipelineHealth.stuckPipelines?.alertLevel === 'LOW') totalScore += 15;
    else if (pipelineHealth.stuckPipelines?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (pipelineHealth.stuckPipelines?.alertLevel === 'HIGH') totalScore += 5;
    
    if (pipelineHealth.successRate?.alertLevel === 'LOW') totalScore += 15;
    else if (pipelineHealth.successRate?.alertLevel === 'MEDIUM') totalScore += 10;
    else if (pipelineHealth.successRate?.alertLevel === 'HIGH') totalScore += 5;
    
    const healthStatus = totalScore >= 90 ? 'EXCELLENT' : 
                        totalScore >= 75 ? 'GOOD' : 
                        totalScore >= 60 ? 'FAIR' : 'POOR';
    
    return {
      score: totalScore,
      status: healthStatus,
      alertLevel: totalScore < 60 ? 'HIGH' : totalScore < 80 ? 'MEDIUM' : 'LOW'
    };
  }

  /**
   * Store monitoring results for dashboard access
   */
  async storeMonitoringResults() {
    try {
      const client = await pool.connect();
      
      const insertQuery = `
        INSERT INTO etl_pipeline_runs (
          pipeline_name, 
          status, 
          start_time, 
          end_time, 
          records_processed, 
          error_message
        ) VALUES ($1, $2, $3, $4, $5, $6)
      `;
      
      await client.query(insertQuery, [
        'Data Quality Monitor',
        this.monitoringResults.overallHealth.status,
        this.monitoringResults.lastCheck,
        this.monitoringResults.lastCheck,
        Object.keys(this.monitoringResults).length,
        JSON.stringify(this.monitoringResults)
      ]);
      
      client.release();
      
    } catch (error) {
      console.error('Error storing monitoring results:', error.message);
    }
  }

  /**
   * Helper methods
   */
  assessDataFreshness(hoursOld, threshold) {
    return hoursOld <= threshold ? 'FRESH' : 'STALE';
  }

  validateQuartileBalance(quartileData) {
    // Check if quartiles are roughly balanced (20-30% each)
    for (const quartile of quartileData) {
      if (quartile.percentage < 20 || quartile.percentage > 30) {
        return 'IMBALANCED';
      }
    }
    return 'BALANCED';
  }

  /**
   * Get current monitoring status for API access
   */
  getMonitoringStatus() {
    return {
      ...this.monitoringResults,
      systemStatus: this.monitoringResults.overallHealth?.status || 'UNKNOWN',
      lastUpdate: this.monitoringResults.lastCheck
    };
  }
}

// Export for use in routes
export default DataQualityMonitor;

// Auto-start monitoring if run directly
if (import.meta.url === `file://${process.argv[1]}`) {
  const monitor = new DataQualityMonitor();
  monitor.startMonitoring();
  
  // Keep the process running
  process.on('SIGINT', () => {
    console.log('\n🛑 Data Quality Monitor shutting down...');
    process.exit(0);
  });
}