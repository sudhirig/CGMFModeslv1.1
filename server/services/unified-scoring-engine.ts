/**
 * Unified Scoring Engine
 * Consolidates all scoring logic into a single, optimized service
 * Replaces 8 different scoring engines with consistent methodology
 */

import { pool } from '../db';

interface FundScoringData {
  fundId: number;
  category: string;
  subcategory: string;
  expenseRatio: number;
  aum: number;
  inceptionDate: Date;
  returns: {
    '3m': number | null;
    '6m': number | null;
    '1y': number | null;
    '3y': number | null;
    '5y': number | null;
  };
  riskMetrics: {
    volatility1y: number | null;
    volatility3y: number | null;
    sharpeRatio: number | null;
    maxDrawdown: number | null;
    beta: number | null;
  };
}

interface ScoringResult {
  fundId: number;
  scoreDate: string;
  historicalReturnsTotal: number;
  riskGradeTotal: number;
  fundamentalsTotal: number;
  otherMetricsTotal: number;
  totalScore: number;
  quartile: number;
  recommendation: string;
}

export class UnifiedScoringEngine {
  
  /**
   * Calculate comprehensive scores for all funds
   */
  static async calculateAllScores(scoreDate: string = new Date().toISOString().split('T')[0]): Promise<void> {
    console.log(`Starting unified scoring calculation for ${scoreDate}`);
    
    const funds = await this.getFundData();
    const scoringResults: ScoringResult[] = [];
    
    for (const fund of funds) {
      const score = await this.calculateFundScore(fund, scoreDate);
      scoringResults.push(score);
    }
    
    // Calculate quartiles based on total scores
    this.calculateQuartiles(scoringResults);
    
    // Save results to database
    await this.saveScores(scoringResults);
    
    console.log(`Completed unified scoring for ${scoringResults.length} funds`);
  }
  
  /**
   * Get fund data with returns and metrics
   */
  private static async getFundData(): Promise<FundScoringData[]> {
    const query = `
      SELECT 
        f.id as fund_id,
        f.category,
        f.subcategory,
        f.expense_ratio,
        COALESCE(f.fund_aum, 0) as aum,
        f.inception_date,
        fpm.returns_3m,
        fpm.returns_6m, 
        fpm.returns_1y,
        fpm.returns_3y,
        fpm.returns_5y,
        fpm.volatility as volatility_1y,
        fpm.volatility_3y,
        fpm.sharpe_ratio,
        fpm.max_drawdown,
        fpm.beta
      FROM funds f
      LEFT JOIN fund_performance_metrics fpm ON f.id = fpm.fund_id
      WHERE f.status = 'ACTIVE'
      ORDER BY f.id
    `;
    
    const result = await pool.query(query);
    
    return result.rows.map(row => ({
      fundId: row.fund_id,
      category: row.category,
      subcategory: row.subcategory,
      expenseRatio: parseFloat(row.expense_ratio) || 0,
      aum: parseFloat(row.aum) || 0,
      inceptionDate: row.inception_date,
      returns: {
        '3m': row.returns_3m ? parseFloat(row.returns_3m) : null,
        '6m': row.returns_6m ? parseFloat(row.returns_6m) : null,
        '1y': row.returns_1y ? parseFloat(row.returns_1y) : null,
        '3y': row.returns_3y ? parseFloat(row.returns_3y) : null,
        '5y': row.returns_5y ? parseFloat(row.returns_5y) : null,
      },
      riskMetrics: {
        volatility1y: row.volatility_1y ? parseFloat(row.volatility_1y) : null,
        volatility3y: row.volatility_3y ? parseFloat(row.volatility_3y) : null,
        sharpeRatio: row.sharpe_ratio ? parseFloat(row.sharpe_ratio) : null,
        maxDrawdown: row.max_drawdown ? parseFloat(row.max_drawdown) : null,
        beta: row.beta ? parseFloat(row.beta) : null,
      }
    }));
  }
  
  /**
   * Calculate comprehensive score for a single fund
   */
  private static async calculateFundScore(fund: FundScoringData, scoreDate: string): Promise<ScoringResult> {
    const historicalReturns = this.calculateHistoricalReturnsScore(fund.returns);
    const riskGrade = this.calculateRiskGradeScore(fund.riskMetrics);
    const fundamentals = this.calculateFundamentalsScore(fund);
    const otherMetrics = this.calculateOtherMetricsScore(fund);
    
    const totalScore = historicalReturns + riskGrade + fundamentals + otherMetrics;
    
    return {
      fundId: fund.fundId,
      scoreDate,
      historicalReturnsTotal: historicalReturns,
      riskGradeTotal: riskGrade,
      fundamentalsTotal: fundamentals,
      otherMetricsTotal: otherMetrics,
      totalScore: Math.min(100, Math.max(0, totalScore)),
      quartile: 0, // Will be calculated later
      recommendation: 'HOLD' // Will be calculated later
    };
  }
  
  /**
   * Calculate historical returns score (40 points max)
   */
  private static calculateHistoricalReturnsScore(returns: FundScoringData['returns']): number {
    let score = 0;
    
    // 3M returns (8 points max)
    if (returns['3m'] !== null) {
      score += this.scoreReturn(returns['3m'], '3m');
    }
    
    // 6M returns (8 points max)
    if (returns['6m'] !== null) {
      score += this.scoreReturn(returns['6m'], '6m');
    }
    
    // 1Y returns (8 points max)
    if (returns['1y'] !== null) {
      score += this.scoreReturn(returns['1y'], '1y');
    }
    
    // 3Y returns (8 points max)
    if (returns['3y'] !== null) {
      score += this.scoreReturn(returns['3y'], '3y');
    }
    
    // 5Y returns (8 points max)
    if (returns['5y'] !== null) {
      score += this.scoreReturn(returns['5y'], '5y');
    }
    
    return Math.min(40, score);
  }
  
  /**
   * Score individual return periods with realistic thresholds
   */
  private static scoreReturn(returnValue: number, period: string): number {
    const thresholds = {
      '3m': { excellent: 15, good: 12, average: 8, fair: 6, poor: 4, minimal: 2, negative: 0 },
      '6m': { excellent: 12, good: 8, average: 6, fair: 4, poor: 2, minimal: 0, negative: -2 },
      '1y': { excellent: 20, good: 15, average: 12, fair: 8, poor: 5, minimal: 2, negative: 0 },
      '3y': { excellent: 18, good: 14, average: 10, fair: 7, poor: 4, minimal: 1, negative: -1 },
      '5y': { excellent: 16, good: 12, average: 9, fair: 6, poor: 3, minimal: 0, negative: -2 }
    };
    
    const t = thresholds[period as keyof typeof thresholds];
    
    if (returnValue >= t.excellent) return 8;
    if (returnValue >= t.good) return 7;
    if (returnValue >= t.average) return 6;
    if (returnValue >= t.fair) return 5;
    if (returnValue >= t.poor) return 4;
    if (returnValue >= t.minimal) return 3;
    if (returnValue >= t.negative) return 2;
    if (returnValue >= -5) return 1;
    return 0;
  }
  
  /**
   * Calculate risk grade score (30 points max)
   */
  private static calculateRiskGradeScore(metrics: FundScoringData['riskMetrics']): number {
    let score = 0;
    
    // Volatility scoring (10 points)
    if (metrics.volatility1y !== null) {
      score += this.scoreVolatility(metrics.volatility1y);
    }
    
    // Sharpe ratio scoring (10 points)
    if (metrics.sharpeRatio !== null) {
      score += this.scoreSharpeRatio(metrics.sharpeRatio);
    }
    
    // Max drawdown scoring (10 points)
    if (metrics.maxDrawdown !== null) {
      score += this.scoreMaxDrawdown(metrics.maxDrawdown);
    }
    
    return Math.min(30, score);
  }
  
  /**
   * Score volatility (lower is better)
   */
  private static scoreVolatility(volatility: number): number {
    if (volatility <= 10) return 10;
    if (volatility <= 15) return 8;
    if (volatility <= 20) return 6;
    if (volatility <= 25) return 4;
    if (volatility <= 30) return 2;
    return 0;
  }
  
  /**
   * Score Sharpe ratio (higher is better)
   */
  private static scoreSharpeRatio(sharpe: number): number {
    if (sharpe >= 2.0) return 10;
    if (sharpe >= 1.5) return 8;
    if (sharpe >= 1.0) return 6;
    if (sharpe >= 0.5) return 4;
    if (sharpe >= 0.2) return 2;
    return 0;
  }
  
  /**
   * Score max drawdown (lower absolute value is better)
   */
  private static scoreMaxDrawdown(drawdown: number): number {
    const absDrawdown = Math.abs(drawdown);
    if (absDrawdown <= 5) return 10;
    if (absDrawdown <= 10) return 8;
    if (absDrawdown <= 15) return 6;
    if (absDrawdown <= 20) return 4;
    if (absDrawdown <= 30) return 2;
    return 0;
  }
  
  /**
   * Calculate fundamentals score (20 points max)
   */
  private static calculateFundamentalsScore(fund: FundScoringData): number {
    let score = 0;
    
    // Expense ratio scoring (10 points)
    score += this.scoreExpenseRatio(fund.expenseRatio, fund.category);
    
    // AUM size scoring (10 points)
    score += this.scoreAumSize(fund.aum);
    
    return Math.min(20, score);
  }
  
  /**
   * Score expense ratio relative to category
   */
  private static scoreExpenseRatio(expenseRatio: number, category: string): number {
    // Category-specific thresholds
    const thresholds = {
      'Equity': { excellent: 1.0, good: 1.5, average: 2.0, poor: 2.5 },
      'Debt': { excellent: 0.5, good: 1.0, average: 1.5, poor: 2.0 },
      'Hybrid': { excellent: 1.2, good: 1.8, average: 2.3, poor: 2.8 }
    };
    
    const categoryThreshold = thresholds[category as keyof typeof thresholds] || thresholds['Equity'];
    
    if (expenseRatio <= categoryThreshold.excellent) return 10;
    if (expenseRatio <= categoryThreshold.good) return 7;
    if (expenseRatio <= categoryThreshold.average) return 5;
    if (expenseRatio <= categoryThreshold.poor) return 2;
    return 0;
  }
  
  /**
   * Score AUM size
   */
  private static scoreAumSize(aum: number): number {
    if (aum >= 10000) return 10; // > 10,000 Cr
    if (aum >= 5000) return 8;   // > 5,000 Cr
    if (aum >= 1000) return 6;   // > 1,000 Cr
    if (aum >= 500) return 4;    // > 500 Cr
    if (aum >= 100) return 2;    // > 100 Cr
    return 0;
  }
  
  /**
   * Calculate other metrics score (10 points max)
   */
  private static calculateOtherMetricsScore(fund: FundScoringData): number {
    let score = 0;
    
    // Fund age/maturity scoring (5 points)
    const ageInYears = fund.inceptionDate ? 
      (Date.now() - new Date(fund.inceptionDate).getTime()) / (1000 * 60 * 60 * 24 * 365) : 0;
    
    if (ageInYears >= 10) score += 5;
    else if (ageInYears >= 5) score += 4;
    else if (ageInYears >= 3) score += 3;
    else if (ageInYears >= 1) score += 2;
    else score += 1;
    
    // Category consistency bonus (5 points)
    if (fund.subcategory && fund.subcategory.length > 0) {
      score += 3; // Well-defined subcategory
    }
    
    return Math.min(10, score);
  }
  
  /**
   * Calculate quartiles based on total scores
   */
  private static calculateQuartiles(results: ScoringResult[]): void {
    const sortedScores = results.sort((a, b) => b.totalScore - a.totalScore);
    
    for (let i = 0; i < sortedScores.length; i++) {
      const result = sortedScores[i];
      
      // Assign quartiles and recommendations based on score ranges
      if (result.totalScore >= 80) {
        result.quartile = 1;
        result.recommendation = 'STRONG_BUY';
      } else if (result.totalScore >= 65) {
        result.quartile = 2;
        result.recommendation = 'BUY';
      } else if (result.totalScore >= 50) {
        result.quartile = 3;
        result.recommendation = 'HOLD';
      } else {
        result.quartile = 4;
        result.recommendation = 'SELL';
      }
    }
  }
  
  /**
   * Save scoring results to database
   */
  private static async saveScores(results: ScoringResult[]): Promise<void> {
    const client = await pool.connect();
    
    try {
      await client.query('BEGIN');
      
      // Clear existing scores for the date
      await client.query(
        'DELETE FROM fund_scores_corrected WHERE score_date = $1',
        [results[0].scoreDate]
      );
      
      // Insert new scores
      for (const result of results) {
        await client.query(`
          INSERT INTO fund_scores_corrected (
            fund_id, score_date, historical_returns_total, risk_grade_total,
            fundamentals_total, other_metrics_total, total_score, quartile, recommendation
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        `, [
          result.fundId, result.scoreDate, result.historicalReturnsTotal,
          result.riskGradeTotal, result.fundamentalsTotal, result.otherMetricsTotal,
          result.totalScore, result.quartile, result.recommendation
        ]);
      }
      
      await client.query('COMMIT');
      console.log(`Saved ${results.length} scoring records to database`);
      
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }
  
  /**
   * Get scoring summary statistics
   */
  static async getScoringStats(scoreDate?: string): Promise<any> {
    const date = scoreDate || new Date().toISOString().split('T')[0];
    
    const query = `
      SELECT 
        COUNT(*) as total_funds,
        AVG(total_score)::numeric(6,2) as avg_score,
        MIN(total_score) as min_score,
        MAX(total_score) as max_score,
        COUNT(CASE WHEN quartile = 1 THEN 1 END) as q1_count,
        COUNT(CASE WHEN quartile = 2 THEN 1 END) as q2_count,
        COUNT(CASE WHEN quartile = 3 THEN 1 END) as q3_count,
        COUNT(CASE WHEN quartile = 4 THEN 1 END) as q4_count,
        COUNT(CASE WHEN recommendation = 'STRONG_BUY' THEN 1 END) as strong_buy_count,
        COUNT(CASE WHEN recommendation = 'BUY' THEN 1 END) as buy_count,
        COUNT(CASE WHEN recommendation = 'HOLD' THEN 1 END) as hold_count,
        COUNT(CASE WHEN recommendation = 'SELL' THEN 1 END) as sell_count
      FROM fund_scores_corrected
      WHERE score_date = $1
    `;
    
    const result = await pool.query(query, [date]);
    return result.rows[0];
  }
}

export const unifiedScoringEngine = UnifiedScoringEngine;