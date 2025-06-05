import { pool } from '../db';
import { AuthenticPerformanceCalculator } from './authentic-performance-calculator';

/**
 * Authentic Fund Scoring Engine
 * Generates real fund scores based on calculated performance metrics from NAV data
 */
export class AuthenticFundScoringEngine {
  private performanceCalculator: AuthenticPerformanceCalculator;
  
  constructor() {
    this.performanceCalculator = new AuthenticPerformanceCalculator();
  }
  
  /**
   * Score fund using authentic performance calculations
   */
  async scoreFund(fundId: number): Promise<any> {
    try {
      // Get fund basic info
      const fundQuery = `
        SELECT id, fund_name, category, subcategory, expense_ratio, aum_crores
        FROM funds 
        WHERE id = $1
      `;
      const fundResult = await pool.query(fundQuery, [fundId]);
      
      if (fundResult.rows.length === 0) {
        return null;
      }
      
      const fund = fundResult.rows[0];
      
      // Calculate authentic performance metrics
      const performance = await this.performanceCalculator.calculateFundPerformance(fundId);
      
      if (!performance) {
        console.log(`Insufficient NAV data for fund ${fundId}, skipping authentic scoring`);
        return null;
      }
      
      // Calculate component scores based on authentic data
      const returnScores = this.calculateReturnScores(performance.returns);
      const riskScores = this.calculateRiskScores(performance.riskMetrics);
      const qualityScores = await this.calculateQualityScores(fund, performance.dataQuality);
      
      // Calculate total score
      const totalScore = returnScores.total + riskScores.total + qualityScores.total;
      
      // Determine recommendation based on authentic scoring
      const recommendation = this.determineRecommendation(totalScore, performance);
      
      // Get category ranking
      const categoryRanking = await this.calculateCategoryRanking(totalScore, fund.category);
      
      return {
        fundId,
        totalScore: Math.round(totalScore * 10) / 10,
        recommendation,
        returnScores,
        riskScores,
        qualityScores,
        riskMetrics: performance.riskMetrics,
        advancedMetrics: performance.advancedMetrics,
        categoryRanking,
        calculationDate: new Date().toISOString().split('T')[0]
      };
      
    } catch (error) {
      console.error(`Error in authentic scoring for fund ${fundId}:`, error);
      return null;
    }
  }
  
  /**
   * Calculate return-based scores (40 points maximum)
   */
  private calculateReturnScores(returns: any): any {
    const scores = {
      return_3m_score: this.scoreReturn(returns.return_3m, 'quarterly') * 0.125, // 5 points max
      return_6m_score: this.scoreReturn(returns.return_6m, 'semi_annual') * 0.25, // 10 points max  
      return_1y_score: this.scoreReturn(returns.return_1y, 'annual') * 0.25, // 10 points max
      return_3y_score: this.scoreReturn(returns.return_3y, 'three_year') * 0.20, // 8 points max
      return_5y_score: this.scoreReturn(returns.return_5y, 'five_year') * 0.175 // 7 points max
    };
    
    scores.total = Object.values(scores).reduce((sum: number, score: number) => sum + score, 0);
    return scores;
  }
  
  /**
   * Score individual return based on performance thresholds
   */
  private scoreReturn(returnValue: number | null, period: string): number {
    if (returnValue === null) return 0;
    
    // Define performance thresholds by period
    const thresholds = {
      quarterly: { excellent: 8, good: 4, average: 1, poor: -2 },
      semi_annual: { excellent: 15, good: 8, average: 3, poor: -3 },
      annual: { excellent: 18, good: 12, average: 6, poor: -5 },
      three_year: { excellent: 15, good: 10, average: 5, poor: -2 },
      five_year: { excellent: 12, good: 8, average: 4, poor: 0 }
    };
    
    const threshold = thresholds[period];
    
    if (returnValue >= threshold.excellent) return 40;
    if (returnValue >= threshold.good) return 30;
    if (returnValue >= threshold.average) return 20;
    if (returnValue >= threshold.poor) return 10;
    return 5;
  }
  
  /**
   * Calculate risk-based scores (30 points maximum)
   */
  private calculateRiskScores(riskMetrics: any): any {
    const scores = {
      volatility_score: this.scoreVolatility(riskMetrics.volatility_1y_percent) * 0.25, // 10 points max (5+5)
      sharpe_score: this.scoreSharpeRatio(riskMetrics.sharpe_ratio_1y) * 0.4, // 16 points max (8+8)
      drawdown_score: this.scoreMaxDrawdown(riskMetrics.max_drawdown_percent) * 0.1 // 4 points max
    };
    
    scores.total = Object.values(scores).reduce((sum: number, score: number) => sum + score, 0);
    return scores;
  }
  
  /**
   * Score volatility (lower is better)
   */
  private scoreVolatility(volatility: number): number {
    if (volatility === null || volatility === undefined) return 0;
    
    if (volatility <= 8) return 40; // Very low volatility
    if (volatility <= 12) return 35; // Low volatility  
    if (volatility <= 18) return 25; // Moderate volatility
    if (volatility <= 25) return 15; // High volatility
    return 5; // Very high volatility
  }
  
  /**
   * Score Sharpe ratio (higher is better)
   */
  private scoreSharpeRatio(sharpe: number): number {
    if (sharpe === null || sharpe === undefined) return 0;
    
    if (sharpe >= 1.5) return 40; // Excellent risk-adjusted returns
    if (sharpe >= 1.0) return 35; // Good risk-adjusted returns
    if (sharpe >= 0.5) return 25; // Decent risk-adjusted returns
    if (sharpe >= 0.0) return 15; // Poor risk-adjusted returns
    return 5; // Negative risk-adjusted returns
  }
  
  /**
   * Score maximum drawdown (lower is better)
   */
  private scoreMaxDrawdown(maxDrawdown: number): number {
    if (maxDrawdown === null || maxDrawdown === undefined) return 0;
    
    if (maxDrawdown <= 5) return 40; // Very low drawdown
    if (maxDrawdown <= 10) return 35; // Low drawdown
    if (maxDrawdown <= 20) return 25; // Moderate drawdown
    if (maxDrawdown <= 35) return 15; // High drawdown
    return 5; // Very high drawdown
  }
  
  /**
   * Calculate quality and fundamental scores (30 points maximum)
   */
  private async calculateQualityScores(fund: any, dataQuality: any): Promise<any> {
    const scores = {
      expense_ratio_score: this.scoreExpenseRatio(fund.expense_ratio, fund.category) * 0.167, // 5 points max
      aum_size_score: this.scoreAUMSize(fund.aum_crores, fund.category) * 0.167, // 5 points max
      data_quality_score: this.scoreDataQuality(dataQuality) * 0.333, // 10 points max
      consistency_score: await this.calculateConsistencyScore(fund.id) * 0.333 // 10 points max
    };
    
    scores.total = Object.values(scores).reduce((sum: number, score: number) => sum + score, 0);
    return scores;
  }
  
  /**
   * Score expense ratio (lower is better)
   */
  private scoreExpenseRatio(expenseRatio: number, category: string): number {
    if (!expenseRatio) return 20; // Neutral if no data
    
    // Category-specific expense ratio thresholds
    const thresholds = {
      'Equity': { excellent: 1.0, good: 1.5, average: 2.0, poor: 2.5 },
      'Debt': { excellent: 0.5, good: 1.0, average: 1.5, poor: 2.0 },
      'Hybrid': { excellent: 1.2, good: 1.8, average: 2.3, poor: 2.8 }
    };
    
    const threshold = thresholds[category] || thresholds['Equity'];
    
    if (expenseRatio <= threshold.excellent) return 40;
    if (expenseRatio <= threshold.good) return 30;
    if (expenseRatio <= threshold.average) return 20;
    if (expenseRatio <= threshold.poor) return 10;
    return 5;
  }
  
  /**
   * Score AUM size (optimal range varies by category)
   */
  private scoreAUMSize(aum: number, category: string): number {
    if (!aum) return 20; // Neutral if no data
    
    // Category-specific AUM thresholds (in crores)
    const thresholds = {
      'Equity': { min: 100, optimal_min: 500, optimal_max: 10000, max: 50000 },
      'Debt': { min: 50, optimal_min: 200, optimal_max: 5000, max: 25000 },
      'Hybrid': { min: 75, optimal_min: 300, optimal_max: 7500, max: 35000 }
    };
    
    const threshold = thresholds[category] || thresholds['Equity'];
    
    if (aum >= threshold.optimal_min && aum <= threshold.optimal_max) return 40;
    if (aum >= threshold.min && aum <= threshold.max) return 30;
    if (aum >= threshold.min * 0.5) return 20;
    return 10;
  }
  
  /**
   * Score data quality based on availability and recency
   */
  private scoreDataQuality(dataQuality: any): number {
    const { totalDays, endDate } = dataQuality;
    
    // Score based on data history length
    let historyScore = 0;
    if (totalDays >= 1825) historyScore = 20; // 5+ years
    else if (totalDays >= 1095) historyScore = 18; // 3+ years
    else if (totalDays >= 730) historyScore = 15; // 2+ years  
    else if (totalDays >= 365) historyScore = 12; // 1+ year
    else historyScore = 8;
    
    // Score based on data recency
    const daysSinceLastUpdate = Math.floor((Date.now() - new Date(endDate).getTime()) / (1000 * 60 * 60 * 24));
    let recencyScore = 0;
    if (daysSinceLastUpdate <= 2) recencyScore = 20;
    else if (daysSinceLastUpdate <= 7) recencyScore = 18;
    else if (daysSinceLastUpdate <= 15) recencyScore = 15;
    else if (daysSinceLastUpdate <= 30) recencyScore = 10;
    else recencyScore = 5;
    
    return historyScore + recencyScore;
  }
  
  /**
   * Calculate consistency score based on performance patterns
   */
  private async calculateConsistencyScore(fundId: number): Promise<number> {
    // This would analyze return consistency, but for now return base score
    return 25; // Placeholder - implement detailed consistency analysis
  }
  
  /**
   * Determine investment recommendation based on total score and risk metrics
   */
  private determineRecommendation(totalScore: number, performance: any): string {
    const { riskMetrics } = performance;
    
    // Base recommendation on total score
    let baseRecommendation = '';
    if (totalScore >= 85) baseRecommendation = 'STRONG_BUY';
    else if (totalScore >= 70) baseRecommendation = 'BUY';
    else if (totalScore >= 55) baseRecommendation = 'HOLD';
    else if (totalScore >= 40) baseRecommendation = 'SELL';
    else baseRecommendation = 'STRONG_SELL';
    
    // Adjust based on risk metrics
    if (riskMetrics.max_drawdown_percent > 30 && baseRecommendation.includes('BUY')) {
      return 'HOLD'; // Downgrade for excessive risk
    }
    
    if (riskMetrics.sharpe_ratio_1y < 0 && totalScore < 60) {
      return 'SELL'; // Downgrade for poor risk-adjusted returns
    }
    
    return baseRecommendation;
  }
  
  /**
   * Calculate category ranking based on score
   */
  private async calculateCategoryRanking(totalScore: number, category: string): Promise<any> {
    try {
      const categoryQuery = `
        SELECT COUNT(*) as total_funds,
               COUNT(CASE WHEN total_score > $1 THEN 1 END) as funds_above
        FROM fund_scores fs
        JOIN funds f ON fs.fund_id = f.id
        WHERE f.category = $2 AND fs.score_date = CURRENT_DATE
      `;
      
      const result = await pool.query(categoryQuery, [totalScore, category]);
      const { total_funds, funds_above } = result.rows[0];
      
      const rank = parseInt(funds_above) + 1;
      const percentile = total_funds > 0 ? ((total_funds - rank + 1) / total_funds) * 100 : 0;
      
      let quartile = 4;
      if (percentile >= 75) quartile = 1;
      else if (percentile >= 50) quartile = 2;
      else if (percentile >= 25) quartile = 3;
      
      return {
        rank,
        total_funds: parseInt(total_funds),
        percentile: Math.round(percentile * 100) / 100,
        quartile
      };
      
    } catch (error) {
      console.error('Error calculating category ranking:', error);
      return { rank: 0, total_funds: 0, percentile: 0, quartile: 4 };
    }
  }
  
  /**
   * Store authentic scores in database
   */
  async storeFundScore(scoreData: any): Promise<void> {
    try {
      const insertQuery = `
        INSERT INTO fund_scores (
          fund_id, score_date, total_score, recommendation,
          return_3m_score, return_6m_score, return_1y_score, return_3y_score, return_5y_score,
          volatility_1y_percent, volatility_3y_percent,
          sharpe_ratio_1y, sharpe_ratio_3y,
          max_drawdown_percent, max_drawdown_start_date, max_drawdown_end_date,
          beta_1y, expense_ratio_score, data_quality_score,
          quartile, category_rank, category_total,
          created_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, NOW()
        )
        ON CONFLICT (fund_id, score_date) DO UPDATE SET
          total_score = EXCLUDED.total_score,
          recommendation = EXCLUDED.recommendation,
          return_3m_score = EXCLUDED.return_3m_score,
          return_6m_score = EXCLUDED.return_6m_score,
          return_1y_score = EXCLUDED.return_1y_score,
          return_3y_score = EXCLUDED.return_3y_score,
          return_5y_score = EXCLUDED.return_5y_score,
          volatility_1y_percent = EXCLUDED.volatility_1y_percent,
          volatility_3y_percent = EXCLUDED.volatility_3y_percent,
          sharpe_ratio_1y = EXCLUDED.sharpe_ratio_1y,
          sharpe_ratio_3y = EXCLUDED.sharpe_ratio_3y,
          max_drawdown_percent = EXCLUDED.max_drawdown_percent,
          max_drawdown_start_date = EXCLUDED.max_drawdown_start_date,
          max_drawdown_end_date = EXCLUDED.max_drawdown_end_date,
          beta_1y = EXCLUDED.beta_1y,
          expense_ratio_score = EXCLUDED.expense_ratio_score,
          data_quality_score = EXCLUDED.data_quality_score,
          quartile = EXCLUDED.quartile,
          category_rank = EXCLUDED.category_rank,
          category_total = EXCLUDED.category_total
      `;
      
      await pool.query(insertQuery, [
        scoreData.fundId,
        scoreData.calculationDate,
        scoreData.totalScore,
        scoreData.recommendation,
        scoreData.returnScores.return_3m_score,
        scoreData.returnScores.return_6m_score,
        scoreData.returnScores.return_1y_score,
        scoreData.returnScores.return_3y_score,
        scoreData.returnScores.return_5y_score,
        scoreData.riskMetrics.volatility_1y_percent,
        scoreData.riskMetrics.volatility_3y_percent,
        scoreData.riskMetrics.sharpe_ratio_1y,
        scoreData.riskMetrics.sharpe_ratio_3y,
        scoreData.riskMetrics.max_drawdown_percent,
        scoreData.riskMetrics.max_drawdown_start_date,
        scoreData.riskMetrics.max_drawdown_end_date,
        scoreData.riskMetrics.beta_1y,
        scoreData.qualityScores.expense_ratio_score,
        scoreData.qualityScores.data_quality_score,
        scoreData.categoryRanking.quartile,
        scoreData.categoryRanking.rank,
        scoreData.categoryRanking.total_funds
      ]);
      
      // Store advanced risk analytics
      await this.storeAdvancedRiskAnalytics(scoreData);
      
    } catch (error) {
      console.error(`Error storing authentic fund score for ${scoreData.fundId}:`, error);
      throw error;
    }
  }
  
  /**
   * Store advanced risk analytics in risk_analytics table
   */
  private async storeAdvancedRiskAnalytics(scoreData: any): Promise<void> {
    try {
      const insertQuery = `
        INSERT INTO risk_analytics (
          fund_id, calculation_date,
          sortino_ratio_1y, downside_deviation_1y,
          rolling_volatility_3m, rolling_volatility_6m, rolling_volatility_12m,
          positive_months_percentage, negative_months_percentage,
          consecutive_positive_months_max, consecutive_negative_months_max
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (fund_id, calculation_date) DO UPDATE SET
          sortino_ratio_1y = EXCLUDED.sortino_ratio_1y,
          downside_deviation_1y = EXCLUDED.downside_deviation_1y,
          rolling_volatility_3m = EXCLUDED.rolling_volatility_3m,
          rolling_volatility_6m = EXCLUDED.rolling_volatility_6m,
          rolling_volatility_12m = EXCLUDED.rolling_volatility_12m,
          positive_months_percentage = EXCLUDED.positive_months_percentage,
          negative_months_percentage = EXCLUDED.negative_months_percentage,
          consecutive_positive_months_max = EXCLUDED.consecutive_positive_months_max,
          consecutive_negative_months_max = EXCLUDED.consecutive_negative_months_max
      `;
      
      const { advancedMetrics } = scoreData;
      
      await pool.query(insertQuery, [
        scoreData.fundId,
        scoreData.calculationDate,
        advancedMetrics.sortino_ratio_1y,
        advancedMetrics.downside_deviation_1y,
        advancedMetrics.rolling_volatility_3m,
        advancedMetrics.rolling_volatility_6m,
        advancedMetrics.rolling_volatility_12m,
        advancedMetrics.positive_months_percentage,
        advancedMetrics.negative_months_percentage,
        advancedMetrics.consecutive_positive_months_max,
        advancedMetrics.consecutive_negative_months_max
      ]);
      
    } catch (error) {
      console.error(`Error storing advanced risk analytics for ${scoreData.fundId}:`, error);
    }
  }
}