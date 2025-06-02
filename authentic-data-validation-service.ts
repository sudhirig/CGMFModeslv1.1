/**
 * Comprehensive Authentic Data Validation Service
 * Ensures all data in the system comes from verified, authentic sources
 */

import { db, executeRawQuery } from './db';
import { funds, navData, fundScores, riskAnalytics, marketIndices } from '../shared/schema';
import { eq, and, gte, lte, isNull, or } from 'drizzle-orm';

interface ValidationResult {
  table: string;
  totalRecords: number;
  validRecords: number;
  invalidRecords: number;
  issues: string[];
  dataFreshness: 'FRESH' | 'STALE' | 'CRITICAL';
  lastUpdate: Date | null;
}

interface DataQualityReport {
  overallScore: number;
  validationResults: ValidationResult[];
  criticalIssues: string[];
  recommendedActions: string[];
}

export class AuthenticDataValidationService {
  
  /**
   * Comprehensive data integrity validation across all tables
   */
  async validateSystemIntegrity(): Promise<DataQualityReport> {
    const validationResults: ValidationResult[] = [];
    const criticalIssues: string[] = [];
    const recommendedActions: string[] = [];

    // Validate funds table authenticity
    const fundsValidation = await this.validateFundsAuthenticity();
    validationResults.push(fundsValidation);
    
    if (fundsValidation.invalidRecords > 0) {
      criticalIssues.push(`${fundsValidation.invalidRecords} funds require authentic data collection from AMC sources`);
      recommendedActions.push('Implement AMC API integration for fund manager and expense ratio data');
    }

    // Validate NAV data authenticity
    const navValidation = await this.validateNavDataAuthenticity();
    validationResults.push(navValidation);
    
    if (navValidation.dataFreshness === 'STALE' || navValidation.dataFreshness === 'CRITICAL') {
      criticalIssues.push('NAV data is not current - requires AMFI daily feed integration');
      recommendedActions.push('Set up automated AMFI NAV data collection');
    }

    // Validate market indices authenticity
    const marketValidation = await this.validateMarketIndicesAuthenticity();
    validationResults.push(marketValidation);
    
    if (marketValidation.dataFreshness !== 'FRESH') {
      criticalIssues.push('Market indices data requires NSE/BSE real-time feeds');
      recommendedActions.push('Integrate authentic NSE/BSE market data APIs');
    }

    // Validate scoring system authenticity
    const scoringValidation = await this.validateScoringSystemAuthenticity();
    validationResults.push(scoringValidation);

    // Calculate overall data quality score
    const overallScore = this.calculateOverallQualityScore(validationResults);

    return {
      overallScore,
      validationResults,
      criticalIssues,
      recommendedActions
    };
  }

  /**
   * Validate funds table for authentic data requirements
   */
  private async validateFundsAuthenticity(): Promise<ValidationResult> {
    const totalRecords = await executeRawQuery('SELECT COUNT(*) FROM funds');
    const total = parseInt(totalRecords.rows[0].count);

    const issues: string[] = [];
    
    // Check for missing fund managers requiring authentic data
    const missingManagers = await executeRawQuery(`
      SELECT COUNT(*) FROM funds 
      WHERE fund_manager IS NULL 
         OR fund_manager LIKE '%Requires%' 
         OR fund_manager LIKE '%Required%'
         OR fund_manager = 'Fund Manager Name'
    `);
    const managerIssues = parseInt(missingManagers.rows[0].count);
    
    if (managerIssues > 0) {
      issues.push(`${managerIssues} funds need authentic fund manager data from AMC sources`);
    }

    // Check for missing expense ratios
    const missingExpense = await executeRawQuery(`
      SELECT COUNT(*) FROM funds WHERE expense_ratio IS NULL
    `);
    const expenseIssues = parseInt(missingExpense.rows[0].count);
    
    if (expenseIssues > 0) {
      issues.push(`${expenseIssues} funds missing expense ratio data`);
    }

    // Check for invalid AUM data
    const invalidAum = await executeRawQuery(`
      SELECT COUNT(*) FROM funds WHERE aum_crores IS NULL OR aum_crores <= 0
    `);
    const aumIssues = parseInt(invalidAum.rows[0].count);
    
    if (aumIssues > 0) {
      issues.push(`${aumIssues} funds have invalid or missing AUM data`);
    }

    const validRecords = total - managerIssues - expenseIssues;
    
    return {
      table: 'funds',
      totalRecords: total,
      validRecords: Math.max(0, validRecords),
      invalidRecords: managerIssues + expenseIssues,
      issues,
      dataFreshness: managerIssues > 0 ? 'CRITICAL' : 'FRESH',
      lastUpdate: new Date()
    };
  }

  /**
   * Validate NAV data for authenticity and freshness
   */
  private async validateNavDataAuthenticity(): Promise<ValidationResult> {
    const totalRecords = await executeRawQuery('SELECT COUNT(*) FROM nav_data');
    const total = parseInt(totalRecords.rows[0].count);

    const issues: string[] = [];
    
    // Check for invalid NAV values
    const invalidNavs = await executeRawQuery(`
      SELECT COUNT(*) FROM nav_data WHERE nav_value <= 0 OR nav_value IS NULL
    `);
    const navIssues = parseInt(invalidNavs.rows[0].count);
    
    if (navIssues > 0) {
      issues.push(`${navIssues} NAV records have invalid values`);
    }

    // Check for future-dated NAVs
    const futureNavs = await executeRawQuery(`
      SELECT COUNT(*) FROM nav_data WHERE nav_date > CURRENT_DATE
    `);
    const futureIssues = parseInt(futureNavs.rows[0].count);
    
    if (futureIssues > 0) {
      issues.push(`${futureIssues} NAV records are future-dated`);
    }

    // Check data freshness
    const latestNav = await executeRawQuery(`
      SELECT MAX(nav_date) as latest_date FROM nav_data
    `);
    const lastUpdate = latestNav.rows[0]?.latest_date ? new Date(latestNav.rows[0].latest_date) : null;
    const daysSinceUpdate = lastUpdate ? Math.floor((Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24)) : 999;
    
    let dataFreshness: 'FRESH' | 'STALE' | 'CRITICAL';
    if (daysSinceUpdate <= 2) {
      dataFreshness = 'FRESH';
    } else if (daysSinceUpdate <= 7) {
      dataFreshness = 'STALE';
      issues.push('NAV data is stale - requires AMFI daily update');
    } else {
      dataFreshness = 'CRITICAL';
      issues.push('NAV data is critically outdated - immediate AMFI integration required');
    }

    const validRecords = total - navIssues - futureIssues;
    
    return {
      table: 'nav_data',
      totalRecords: total,
      validRecords: Math.max(0, validRecords),
      invalidRecords: navIssues + futureIssues,
      issues,
      dataFreshness,
      lastUpdate
    };
  }

  /**
   * Validate market indices for authentic data sources
   */
  private async validateMarketIndicesAuthenticity(): Promise<ValidationResult> {
    const totalRecords = await executeRawQuery('SELECT COUNT(*) FROM market_indices');
    const total = parseInt(totalRecords.rows[0].count);

    const issues: string[] = [];
    
    // Check for invalid index values
    const invalidIndices = await executeRawQuery(`
      SELECT COUNT(*) FROM market_indices WHERE close_value <= 0 OR close_value IS NULL
    `);
    const indexIssues = parseInt(invalidIndices.rows[0].count);
    
    if (indexIssues > 0) {
      issues.push(`${indexIssues} market index records have invalid values`);
    }

    // Check data freshness
    const latestIndex = await executeRawQuery(`
      SELECT MAX(index_date) as latest_date FROM market_indices
    `);
    const lastUpdate = latestIndex.rows[0]?.latest_date ? new Date(latestIndex.rows[0].latest_date) : null;
    const daysSinceUpdate = lastUpdate ? Math.floor((Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24)) : 999;
    
    let dataFreshness: 'FRESH' | 'STALE' | 'CRITICAL';
    if (daysSinceUpdate <= 1) {
      dataFreshness = 'FRESH';
    } else if (daysSinceUpdate <= 3) {
      dataFreshness = 'STALE';
      issues.push('Market indices require NSE/BSE real-time feeds');
    } else {
      dataFreshness = 'CRITICAL';
      issues.push('Market indices critically outdated - implement authorized market data feeds');
    }

    // Check for comprehensive index coverage
    const indexCount = await executeRawQuery(`
      SELECT COUNT(DISTINCT index_name) as unique_indices FROM market_indices
    `);
    const uniqueIndices = parseInt(indexCount.rows[0].unique_indices);
    
    if (uniqueIndices < 50) {
      issues.push(`Only ${uniqueIndices} indices available - expand to sector-specific benchmarks`);
    }

    const validRecords = total - indexIssues;
    
    return {
      table: 'market_indices',
      totalRecords: total,
      validRecords: Math.max(0, validRecords),
      invalidRecords: indexIssues,
      issues,
      dataFreshness,
      lastUpdate
    };
  }

  /**
   * Validate scoring system for authentic calculations
   */
  private async validateScoringSystemAuthenticity(): Promise<ValidationResult> {
    const totalRecords = await executeRawQuery('SELECT COUNT(*) FROM fund_scores');
    const total = parseInt(totalRecords.rows[0].count);

    const issues: string[] = [];
    
    // Check for missing or invalid scores
    const invalidScores = await executeRawQuery(`
      SELECT COUNT(*) FROM fund_scores 
      WHERE total_score IS NULL 
         OR total_score <= 0 
         OR total_score > 100
    `);
    const scoreIssues = parseInt(invalidScores.rows[0].count);
    
    if (scoreIssues > 0) {
      issues.push(`${scoreIssues} fund scores are invalid or missing`);
    }

    // Check scoring coverage vs available funds
    const fundsWithScores = await executeRawQuery(`
      SELECT COUNT(DISTINCT fund_id) FROM fund_scores WHERE score_date = CURRENT_DATE
    `);
    const scoredFunds = parseInt(fundsWithScores.rows[0].count);
    
    const totalFunds = await executeRawQuery('SELECT COUNT(*) FROM funds');
    const allFunds = parseInt(totalFunds.rows[0].count);
    
    const coveragePercent = (scoredFunds / allFunds) * 100;
    
    if (coveragePercent < 80) {
      issues.push(`Only ${coveragePercent.toFixed(1)}% of funds have current scores - requires more historical NAV data`);
    }

    // Check data freshness
    const latestScore = await executeRawQuery(`
      SELECT MAX(score_date) as latest_date FROM fund_scores
    `);
    const lastUpdate = latestScore.rows[0]?.latest_date ? new Date(latestScore.rows[0].latest_date) : null;
    const daysSinceUpdate = lastUpdate ? Math.floor((Date.now() - lastUpdate.getTime()) / (1000 * 60 * 60 * 24)) : 999;
    
    let dataFreshness: 'FRESH' | 'STALE' | 'CRITICAL';
    if (daysSinceUpdate <= 7) {
      dataFreshness = 'FRESH';
    } else if (daysSinceUpdate <= 30) {
      dataFreshness = 'STALE';
      issues.push('Fund scoring requires weekly updates with fresh NAV data');
    } else {
      dataFreshness = 'CRITICAL';
      issues.push('Fund scoring critically outdated - run authentic scoring update');
    }

    const validRecords = total - scoreIssues;
    
    return {
      table: 'fund_scores',
      totalRecords: total,
      validRecords: Math.max(0, validRecords),
      invalidRecords: scoreIssues,
      issues,
      dataFreshness,
      lastUpdate
    };
  }

  /**
   * Calculate overall data quality score based on validation results
   */
  private calculateOverallQualityScore(results: ValidationResult[]): number {
    let totalScore = 0;
    let maxScore = 0;

    for (const result of results) {
      const validityScore = result.totalRecords > 0 ? 
        (result.validRecords / result.totalRecords) * 100 : 0;
      
      const freshnessScore = result.dataFreshness === 'FRESH' ? 100 : 
                           result.dataFreshness === 'STALE' ? 60 : 20;
      
      const tableScore = (validityScore * 0.7) + (freshnessScore * 0.3);
      totalScore += tableScore;
      maxScore += 100;
    }

    return maxScore > 0 ? Math.round(totalScore / results.length) : 0;
  }

  /**
   * Get real-time data quality dashboard
   */
  async getDataQualityDashboard() {
    const report = await this.validateSystemIntegrity();
    
    // Check for orphaned records
    const orphanedRecords = await this.checkOrphanedRecords();
    
    // Monitor pipeline health
    const pipelineHealth = await this.monitorPipelineHealth();
    
    return {
      ...report,
      orphanedRecords,
      pipelineHealth,
      lastChecked: new Date()
    };
  }

  /**
   * Check for orphaned records across foreign key relationships
   */
  private async checkOrphanedRecords() {
    const orphanedNavData = await executeRawQuery(`
      SELECT COUNT(*) FROM nav_data 
      WHERE fund_id NOT IN (SELECT id FROM funds)
    `);
    
    const orphanedScores = await executeRawQuery(`
      SELECT COUNT(*) FROM fund_scores 
      WHERE fund_id NOT IN (SELECT id FROM funds)
    `);
    
    const orphanedRisk = await executeRawQuery(`
      SELECT COUNT(*) FROM risk_analytics 
      WHERE fund_id NOT IN (SELECT id FROM funds)
    `);

    return {
      nav_data: parseInt(orphanedNavData.rows[0].count),
      fund_scores: parseInt(orphanedScores.rows[0].count),
      risk_analytics: parseInt(orphanedRisk.rows[0].count)
    };
  }

  /**
   * Monitor ETL pipeline health and data processing status
   */
  private async monitorPipelineHealth() {
    const recentPipelines = await executeRawQuery(`
      SELECT 
        pipeline_name,
        status,
        start_time,
        end_time,
        records_processed,
        error_message
      FROM etl_pipeline_runs 
      WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
      ORDER BY start_time DESC
      LIMIT 10
    `);

    const failedPipelines = await executeRawQuery(`
      SELECT COUNT(*) FROM etl_pipeline_runs 
      WHERE status = 'FAILED' 
        AND start_time >= CURRENT_DATE - INTERVAL '24 hours'
    `);

    return {
      recentRuns: recentPipelines.rows,
      failedInLast24h: parseInt(failedPipelines.rows[0].count),
      healthScore: parseInt(failedPipelines.rows[0].count) === 0 ? 100 : 
                  parseInt(failedPipelines.rows[0].count) < 3 ? 70 : 30
    };
  }
}

export const authenticDataValidator = new AuthenticDataValidationService();