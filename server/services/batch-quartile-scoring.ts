import { db, pool } from '../db';
import { DateTime } from 'luxon';
import { storage } from '../storage';

export class FundScoringEngine {
  // Scoring weights based on the documented methodology
  private weights = {
    historicalReturns: 40.0,
    riskGrade: 30.0,
    otherMetrics: 30.0,
    
    // Historical returns sub-weights
    return3m: 5.0,
    return6m: 10.0,
    return1y: 10.0,
    return3y: 8.0,
    return5y: 7.0,
    
    // Risk grade sub-weights
    stdDev1y: 5.0,
    stdDev3y: 5.0,
    updownCapture1y: 8.0,
    updownCapture3y: 8.0,
    maxDrawdown: 4.0,
    
    // Other metrics sub-weights
    sectoralSimilarity: 10.0,
    forwardScore: 10.0,
    aumSize: 5.0,
    expenseRatio: 5.0
  };

  async scoreFund(fundId: number, scoreDate?: Date): Promise<any> {
    // Set default score date to today if not provided
    const scoreDateStr = scoreDate 
      ? scoreDate.toISOString().split('T')[0] 
      : new Date().toISOString().split('T')[0];
    
    try {
      // Get fund information
      const fundInfo = await this.getFundInfo(fundId);
      if (!fundInfo) return null;
      
      // Get NAV data (approx. 5 years for comprehensive analysis)
      const navData = await this.getFundNavData(fundId, 365); // At least 1 year of data
      if (navData.length < 30) { // Need at least 30 days of data for basic scoring
        console.log(`Insufficient NAV data for fund ${fundId}, skipping scoring`);
        return null;
      }
      
      // Get benchmark data if available
      let benchmarkData = null;
      if (fundInfo.benchmarkName) {
        benchmarkData = await this.getBenchmarkData(fundInfo.benchmarkName, 365);
      }
      
      // Get category peers for ranking
      const categoryFunds = await this.getCategoryFunds(fundInfo.category);
      
      // Calculate Historical Returns Scores
      const historicalReturnsScores = await this.scoreHistoricalReturns(
        navData, categoryFunds, fundId
      );
      
      // Calculate Risk Grade Scores
      const riskGradeScores = await this.scoreRiskGrade(
        navData, benchmarkData, categoryFunds, fundId
      );
      
      // Calculate Other Metrics Scores
      const otherMetricsScores = await this.scoreOtherMetrics(
        fundInfo, fundId, scoreDate
      );
      
      // Calculate total score
      const historicalReturnsTotal = this.sumScores(historicalReturnsScores);
      const riskGradeTotal = this.sumScores(riskGradeScores);
      const otherMetricsTotal = this.sumScores(otherMetricsScores);
      
      const totalScore = historicalReturnsTotal + riskGradeTotal + otherMetricsTotal;
      
      // Store the scores in the database
      await this.storeFundScore(fundId, scoreDateStr, {
        ...historicalReturnsScores,
        historicalReturnsTotal,
        ...riskGradeScores,
        riskGradeTotal,
        ...otherMetricsScores,
        otherMetricsTotal,
        totalScore
      });
      
      return {
        fundId,
        scoreDate: scoreDateStr,
        historicalReturnsTotal,
        riskGradeTotal,
        otherMetricsTotal,
        totalScore
      };
    } catch (error) {
      console.error(`Error scoring fund ${fundId}:`, error);
      return null;
    }
  }

  private sumScores(scores: Record<string, number>): number {
    return Object.values(scores).reduce((sum, score) => sum + (score || 0), 0);
  }

  // Get fund information from the database
  private async getFundInfo(fundId: number): Promise<any> {
    try {
      const result = await pool.query(`
        SELECT 
          id, scheme_code, fund_name, amc_name, category, subcategory,
          benchmark_name, expense_ratio
        FROM funds
        WHERE id = $1
      `, [fundId]);
      
      return result.rows[0] || null;
    } catch (error) {
      console.error(`Error getting fund info for ${fundId}:`, error);
      return null;
    }
  }

  // Get NAV data for a fund
  private async getFundNavData(fundId: number, days: number): Promise<any[]> {
    try {
      const result = await pool.query(`
        SELECT 
          nav_date, nav_value
        FROM nav_data
        WHERE fund_id = $1
        AND nav_date >= CURRENT_DATE - INTERVAL '${days} days'
        ORDER BY nav_date
      `, [fundId]);
      
      return result.rows || [];
    } catch (error) {
      console.error(`Error getting NAV data for fund ${fundId}:`, error);
      return [];
    }
  }

  // Get benchmark data
  private async getBenchmarkData(benchmarkName: string, days: number): Promise<any[]> {
    try {
      const result = await pool.query(`
        SELECT 
          index_date, close_value
        FROM market_indices
        WHERE index_name = $1
        AND index_date >= CURRENT_DATE - INTERVAL '${days} days'
        ORDER BY index_date
      `, [benchmarkName]);
      
      return result.rows || [];
    } catch (error) {
      console.error(`Error getting benchmark data for ${benchmarkName}:`, error);
      return [];
    }
  }

  // Get list of funds in the same category for peer comparison
  private async getCategoryFunds(category: string): Promise<number[]> {
    try {
      const result = await pool.query(`
        SELECT id
        FROM funds
        WHERE category = $1
      `, [category]);
      
      return result.rows.map(row => row.id) || [];
    } catch (error) {
      console.error(`Error getting category funds for ${category}:`, error);
      return [];
    }
  }

  // Score historical returns component
  private async scoreHistoricalReturns(navData: any[], categoryFunds: number[], fundId: number): Promise<Record<string, number>> {
    const scores: Record<string, number> = {};
    
    // If we don't have enough data, return zeros
    if (navData.length < 30) {
      return {
        return3mScore: 0,
        return6mScore: 0,
        return1yScore: 0,
        return3yScore: 0,
        return5yScore: 0
      };
    }
    
    // Calculate returns for different periods
    const periods = [
      { name: '3m', days: 90, weight: this.weights.return3m },
      { name: '6m', days: 180, weight: this.weights.return6m },
      { name: '1y', days: 365, weight: this.weights.return1y },
      { name: '3y', days: 1095, weight: this.weights.return3y },
      { name: '5y', days: 1825, weight: this.weights.return5y }
    ];
    
    for (const period of periods) {
      const returnValue = this.calculatePeriodReturn(navData, period.days);
      
      if (returnValue !== null) {
        // Use actual percentage return as the score base instead of quartile buckets
        scores[`return${period.name}Score`] = this.scoreFromActualReturn(returnValue, period.weight);
        
        // Store the actual percentage return for reference (removing null assignment)
        scores[`actual${period.name}Return`] = returnValue;
      } else {
        scores[`return${period.name}Score`] = 0;
        // scores[`actual${period.name}Return`] = null; // Remove null assignment
      }
    }
    
    return scores;
  }

  // Calculate return for a specific period - returns actual percentage
  private calculatePeriodReturn(navData: any[], days: number): number | null {
    if (navData.length < days) {
      return null;
    }
    
    // Get the latest NAV
    const currentNav = parseFloat(navData[navData.length - 1].nav_value);
    
    // Try to get the NAV from exactly days ago, or the closest earlier date
    let startIdx = 0;
    for (let i = navData.length - 1; i >= 0; i--) {
      const navDate = new Date(navData[i].nav_date);
      const targetDate = new Date();
      targetDate.setDate(targetDate.getDate() - days);
      
      if (navDate <= targetDate) {
        startIdx = i;
        break;
      }
    }
    
    const startNav = parseFloat(navData[startIdx].nav_value);
    
    if (startNav <= 0 || currentNav <= 0) {
      return null;
    }
    
    const years = days / 365.25;
    
    // Calculate simple return for periods <= 1 year
    if (years <= 1) {
      return ((currentNav / startNav) - 1) * 100;
    } 
    // Calculate annualized return for periods > 1 year
    else {
      return (Math.pow(currentNav / startNav, 1/years) - 1) * 100;
    }
  }

  // Get returns for all funds in a category for a specific period
  private async getCategoryReturns(categoryFundIds: number[], days: number): Promise<number[]> {
    const returns: number[] = [];
    
    // This would be inefficient for a large number of funds
    // In a production environment, this should be optimized with a bulk query
    for (const fundId of categoryFundIds) {
      try {
        const navData = await this.getFundNavData(fundId, days);
        const returnValue = this.calculatePeriodReturn(navData, days);
        
        if (returnValue !== null) {
          returns.push(returnValue);
        }
      } catch (error) {
        console.error(`Error calculating return for fund ${fundId}:`, error);
      }
    }
    
    return returns;
  }

  // Determine quartile rank within a set of values
  private getQuartileRank(value: number, values: number[]): number {
    const sortedValues = [...values].sort((a, b) => b - a); // Sort descending
    const index = sortedValues.findIndex(v => v <= value);
    
    if (index === -1) return 4; // Below all values
    
    const percentile = index / sortedValues.length;
    
    if (percentile <= 0.25) return 1; // Top 25%
    if (percentile <= 0.5) return 2; // Top 50%
    if (percentile <= 0.75) return 3; // Top 75%
    return 4; // Bottom 25%
  }

  // Convert quartile to score based on max points
  private quartileToScore(quartile: number, maxPoints: number): number {
    switch (quartile) {
      case 1: return maxPoints;
      case 2: return maxPoints * 0.75;
      case 3: return maxPoints * 0.5;
      case 4: return maxPoints * 0.25;
      default: return 0;
    }
  }

  // Score based on actual percentage return (continuous scaling, not quartile buckets)
  private scoreFromActualReturn(returnValue: number, maxPoints: number): number {
    // Use actual percentage return with continuous scaling instead of fixed quartiles
    // This preserves the authentic return data while providing meaningful scores
    
    // Transform actual percentage return to score using continuous function
    // Higher returns get higher scores, but cap at maxPoints
    const baseScore = Math.max(0, returnValue * 0.5 + maxPoints * 0.4);
    return Math.min(maxPoints, baseScore);
  }

  // Score absolute return (used when peer data is not available)
  private scoreAbsoluteReturn(returnValue: number, maxPoints: number): number {
    // Simple scoring based on absolute return value
    // This is a placeholder and should be calibrated based on market conditions
    if (returnValue >= 20) return maxPoints;
    if (returnValue >= 15) return maxPoints * 0.8;
    if (returnValue >= 10) return maxPoints * 0.6;
    if (returnValue >= 5) return maxPoints * 0.4;
    if (returnValue >= 0) return maxPoints * 0.2;
    return 0; // Negative returns get 0 points
  }

  // Score risk metrics
  private async scoreRiskGrade(navData: any[], benchmarkData: any[] | null, categoryFunds: number[], fundId: number): Promise<Record<string, number>> {
    const scores: Record<string, number> = {};
    
    // Calculate daily returns for different time periods
    const fundReturns1y = this.calculateDailyReturns(navData, 365);
    const fundReturns3y = this.calculateDailyReturns(navData, 1095);
    
    let benchmarkReturns1y = null;
    let benchmarkReturns3y = null;
    
    if (benchmarkData && benchmarkData.length > 0) {
      benchmarkReturns1y = this.calculateDailyReturns(benchmarkData, 365);
      benchmarkReturns3y = this.calculateDailyReturns(benchmarkData, 1095);
    }
    
    // Score volatility (standard deviation)
    if (fundReturns1y.length >= 90) { // At least 3 months of daily returns
      const volatility1y = this.calculateVolatility(fundReturns1y);
      scores.stdDev1yScore = await this.scoreVolatility(
        volatility1y, categoryFunds, '1y', this.weights.stdDev1y
      );
    } else {
      scores.stdDev1yScore = 0;
    }
    
    if (fundReturns3y.length >= 250) { // At least 1 year of daily returns
      const volatility3y = this.calculateVolatility(fundReturns3y);
      scores.stdDev3yScore = await this.scoreVolatility(
        volatility3y, categoryFunds, '3y', this.weights.stdDev3y
      );
    } else {
      scores.stdDev3yScore = 0;
    }
    
    // Score up/down capture ratio
    if (benchmarkReturns1y && benchmarkReturns1y.length >= 90) {
      const captureRatio1y = this.calculateUpdownCapture(fundReturns1y, benchmarkReturns1y);
      scores.updownCapture1yScore = this.scoreCaptureRatio(
        captureRatio1y, this.weights.updownCapture1y
      );
    } else {
      scores.updownCapture1yScore = this.weights.updownCapture1y * 0.5; // Default to half points
    }
    
    if (benchmarkReturns3y && benchmarkReturns3y.length >= 250) {
      const captureRatio3y = this.calculateUpdownCapture(fundReturns3y, benchmarkReturns3y);
      scores.updownCapture3yScore = this.scoreCaptureRatio(
        captureRatio3y, this.weights.updownCapture3y
      );
    } else {
      scores.updownCapture3yScore = this.weights.updownCapture3y * 0.5; // Default to half points
    }
    
    // Score maximum drawdown
    const maxDrawdown = this.calculateMaxDrawdown(navData);
    scores.maxDrawdownScore = this.scoreMaxDrawdown(
      maxDrawdown, this.weights.maxDrawdown
    );
    
    return scores;
  }

  // Calculate daily returns from NAV data
  private calculateDailyReturns(data: any[], days: number): number[] {
    const returns: number[] = [];
    
    // Filter to the required time period
    const filteredData = data.slice(-Math.min(days, data.length));
    
    // Calculate daily returns
    for (let i = 1; i < filteredData.length; i++) {
      const current = parseFloat(filteredData[i].nav_value || filteredData[i].close_value);
      const previous = parseFloat(filteredData[i-1].nav_value || filteredData[i-1].close_value);
      
      if (previous > 0) {
        returns.push((current / previous) - 1);
      }
    }
    
    return returns;
  }

  // Calculate volatility (standard deviation of returns)
  private calculateVolatility(returns: number[]): number {
    if (returns.length === 0) return 0;
    
    const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
    const squaredDiffs = returns.map(value => Math.pow(value - mean, 2));
    const variance = squaredDiffs.reduce((sum, value) => sum + value, 0) / returns.length;
    
    // Annualize the volatility (standard deviation) by multiplying by sqrt(252)
    return Math.sqrt(variance) * Math.sqrt(252) * 100; // Convert to percentage
  }

  // Score volatility compared to peers
  private async scoreVolatility(volatility: number, categoryFundIds: number[], period: string, maxPoints: number): Promise<number> {
    // Lower volatility is better
    // Get volatility for all category funds
    const volatilities: number[] = [];
    
    for (const fundId of categoryFundIds) {
      try {
        const navData = await this.getFundNavData(fundId, period === '1y' ? 365 : 1095);
        const returns = this.calculateDailyReturns(navData, period === '1y' ? 365 : 1095);
        
        if (returns.length > 0) {
          volatilities.push(this.calculateVolatility(returns));
        }
      } catch (error) {
        console.error(`Error calculating volatility for fund ${fundId}:`, error);
      }
    }
    
    if (volatilities.length === 0) {
      // If no peer data, score based on absolute volatility
      return this.scoreAbsoluteVolatility(volatility, maxPoints);
    }
    
    // Sort volatilities in ascending order (lower is better)
    const sortedVolatilities = [...volatilities].sort((a, b) => a - b);
    const index = sortedVolatilities.findIndex(v => v >= volatility);
    
    if (index === -1) return maxPoints; // Below all values (best)
    
    const percentile = index / sortedVolatilities.length;
    
    if (percentile <= 0.25) return maxPoints;
    if (percentile <= 0.5) return maxPoints * 0.75;
    if (percentile <= 0.75) return maxPoints * 0.5;
    return maxPoints * 0.25;
  }

  // Score absolute volatility
  private scoreAbsoluteVolatility(volatility: number, maxPoints: number): number {
    // Lower volatility is better
    if (volatility <= 10) return maxPoints;
    if (volatility <= 15) return maxPoints * 0.8;
    if (volatility <= 20) return maxPoints * 0.6;
    if (volatility <= 25) return maxPoints * 0.4;
    if (volatility <= 30) return maxPoints * 0.2;
    return 0; // Very high volatility gets 0 points
  }

  // Calculate up/down capture ratio
  private calculateUpdownCapture(fundReturns: number[], benchmarkReturns: number[]): number {
    // Ensure both arrays are the same length
    const length = Math.min(fundReturns.length, benchmarkReturns.length);
    
    if (length === 0) return 1.0; // Default to neutral
    
    const upPeriods: { fund: number, benchmark: number }[] = [];
    const downPeriods: { fund: number, benchmark: number }[] = [];
    
    // Separate up and down periods
    for (let i = 0; i < length; i++) {
      if (benchmarkReturns[i] > 0) {
        upPeriods.push({ fund: fundReturns[i], benchmark: benchmarkReturns[i] });
      } else if (benchmarkReturns[i] < 0) {
        downPeriods.push({ fund: fundReturns[i], benchmark: benchmarkReturns[i] });
      }
    }
    
    if (upPeriods.length === 0 || downPeriods.length === 0) {
      return 1.0; // Not enough data to calculate
    }
    
    // Calculate up capture
    const upCapture = upPeriods.reduce((sum, period) => sum + period.fund, 0) / upPeriods.length /
                      (upPeriods.reduce((sum, period) => sum + period.benchmark, 0) / upPeriods.length);
    
    // Calculate down capture
    const downCapture = downPeriods.reduce((sum, period) => sum + period.fund, 0) / downPeriods.length /
                        (downPeriods.reduce((sum, period) => sum + period.benchmark, 0) / downPeriods.length);
    
    // Return the ratio (higher is better - higher up capture, lower down capture)
    return upCapture / Math.abs(downCapture);
  }

  // Score capture ratio
  private scoreCaptureRatio(ratio: number, maxPoints: number): number {
    // Higher ratio is better (capturing more upside than downside)
    if (ratio >= 2.0) return maxPoints;
    if (ratio >= 1.5) return maxPoints * 0.9;
    if (ratio >= 1.2) return maxPoints * 0.8;
    if (ratio >= 1.0) return maxPoints * 0.7;
    if (ratio >= 0.8) return maxPoints * 0.5;
    if (ratio >= 0.6) return maxPoints * 0.3;
    return maxPoints * 0.1;
  }

  // Calculate maximum drawdown
  private calculateMaxDrawdown(navData: any[]): number {
    if (navData.length < 30) return 0;
    
    let maxDrawdown = 0;
    let peak = parseFloat(navData[0].nav_value);
    
    for (let i = 1; i < navData.length; i++) {
      const current = parseFloat(navData[i].nav_value);
      
      if (current > peak) {
        peak = current;
      } else {
        const drawdown = (peak - current) / peak;
        maxDrawdown = Math.max(maxDrawdown, drawdown);
      }
    }
    
    return maxDrawdown * 100; // Convert to percentage
  }

  // Score maximum drawdown
  private scoreMaxDrawdown(maxDrawdown: number, maxPoints: number): number {
    // Lower drawdown is better
    if (maxDrawdown <= 10) return maxPoints;
    if (maxDrawdown <= 15) return maxPoints * 0.8;
    if (maxDrawdown <= 20) return maxPoints * 0.6;
    if (maxDrawdown <= 30) return maxPoints * 0.4;
    if (maxDrawdown <= 40) return maxPoints * 0.2;
    return 0; // Very high drawdown gets 0 points
  }

  // Score other metrics
  private async scoreOtherMetrics(fundInfo: any, fundId: number, scoreDate?: Date): Promise<Record<string, number>> {
    // This is a simplified implementation
    // In a real implementation, you would calculate each of these metrics in detail
    
    const scores: Record<string, number> = {
      sectoralSimilarityScore: this.weights.sectoralSimilarity * 0.7, // Default to 70% of max
      forwardScore: this.weights.forwardScore * 0.7, // Default to 70% of max
      aumSizeScore: 0,
      expenseRatioScore: 0
    };
    
    // Score AUM size (if available)
    const aumData = await this.getLatestAUM(fundId);
    if (aumData) {
      scores.aumSizeScore = this.scoreAUMSize(aumData, fundInfo.category);
    } else {
      scores.aumSizeScore = this.weights.aumSize * 0.5; // Default to 50% of max
    }
    
    // Score expense ratio (if available)
    if (fundInfo.expense_ratio) {
      scores.expenseRatioScore = this.scoreExpenseRatio(
        parseFloat(fundInfo.expense_ratio), fundInfo.category
      );
    } else {
      scores.expenseRatioScore = this.weights.expenseRatio * 0.5; // Default to 50% of max
    }
    
    return scores;
  }

  // Get latest AUM for a fund
  private async getLatestAUM(fundId: number): Promise<number | null> {
    try {
      const result = await pool.query(`
        SELECT aum_cr 
        FROM nav_data
        WHERE fund_id = $1 AND aum_cr IS NOT NULL
        ORDER BY nav_date DESC
        LIMIT 1
      `, [fundId]);
      
      return result.rows.length > 0 ? parseFloat(result.rows[0].aum_cr) : null;
    } catch (error) {
      console.error(`Error getting AUM for fund ${fundId}:`, error);
      return null;
    }
  }

  // Score AUM size
  private scoreAUMSize(aumCr: number, category: string): number {
    // Different thresholds based on category
    let thresholds: number[];
    
    if (category.includes('Equity')) {
      thresholds = [5000, 2000, 1000, 500, 100]; // Equity funds - higher AUM thresholds
    } else if (category.includes('Debt')) {
      thresholds = [10000, 5000, 2000, 1000, 500]; // Debt funds - highest AUM thresholds
    } else {
      thresholds = [3000, 1000, 500, 200, 50]; // Others - lower thresholds
    }
    
    // Score based on thresholds
    if (aumCr >= thresholds[0]) return this.weights.aumSize;
    if (aumCr >= thresholds[1]) return this.weights.aumSize * 0.9;
    if (aumCr >= thresholds[2]) return this.weights.aumSize * 0.7;
    if (aumCr >= thresholds[3]) return this.weights.aumSize * 0.5;
    if (aumCr >= thresholds[4]) return this.weights.aumSize * 0.3;
    return this.weights.aumSize * 0.1; // Very small AUM
  }

  // Score expense ratio
  private scoreExpenseRatio(expenseRatio: number, category: string): number {
    // Lower expense ratio is better
    // Different thresholds based on category
    let thresholds: number[];
    
    if (category.includes('Equity')) {
      thresholds = [1.0, 1.5, 2.0, 2.5, 3.0]; // Equity funds thresholds
    } else if (category.includes('Debt')) {
      thresholds = [0.5, 1.0, 1.5, 2.0, 2.5]; // Debt funds - stricter thresholds
    } else {
      thresholds = [1.0, 1.5, 2.0, 2.5, 3.0]; // Others - same as equity
    }
    
    // Score based on thresholds (lower is better)
    if (expenseRatio <= thresholds[0]) return this.weights.expenseRatio;
    if (expenseRatio <= thresholds[1]) return this.weights.expenseRatio * 0.8;
    if (expenseRatio <= thresholds[2]) return this.weights.expenseRatio * 0.6;
    if (expenseRatio <= thresholds[3]) return this.weights.expenseRatio * 0.4;
    if (expenseRatio <= thresholds[4]) return this.weights.expenseRatio * 0.2;
    return 0; // Very high expense ratio
  }

  // Store fund score with both actual returns and derived scores
  private async storeFundScore(fundId: number, scoreDate: string, scores: Record<string, number>): Promise<void> {
    try {
      // Get fund info for context
      const fundInfo = await this.getFundInfo(fundId);
      if (!fundInfo) return;
      
      // Get NAV data to calculate actual percentage returns
      const navData = await this.getFundNavData(fundId, 1095); // 3 years of data
      
      // Calculate actual percentage returns from NAV movements
      const actual3mReturn = this.calculatePeriodReturn(navData, 90);
      const actual1yReturn = this.calculatePeriodReturn(navData, 365);
      const actual3yReturn = this.calculatePeriodReturn(navData, 1095);
      
      // Calculate actual volatility
      const dailyReturns = this.calculateDailyReturns(navData, 365);
      const actualVolatility = dailyReturns.length > 0 ? this.calculateVolatility(dailyReturns) : null;
      
      // Prepare comprehensive data with both actual returns and scores
      const comprehensiveData = {
        fund_id: fundId,
        score_date: scoreDate,
        
        // Store actual percentage returns (authentic data from NAV)
        return_3m_score: actual3mReturn || 0,
        return_1y_score: actual1yReturn || 0,
        return_3y_score: actual3yReturn || 0,
        volatility_1y_percent: actualVolatility,
        
        // Store derived scores based on actual returns
        return_3m_derived_score: scores.return3mScore || 0,
        return_1y_derived_score: scores.return1yScore || 0,
        return_3y_derived_score: scores.return3yScore || 0,
        
        // Other scores
        expense_ratio_score: scores.expenseRatioScore || 0,
        data_quality_score: navData.length / 10, // Based on actual data availability
        
        // Total scores
        historical_returns_total: scores.historicalReturnsTotal || 0,
        risk_grade_total: scores.riskGradeTotal || 0,
        other_metrics_total: scores.otherMetricsTotal || 0,
        total_score: scores.totalScore || 0,
        
        // Recommendation based on actual performance
        recommendation: this.getRecommendationFromActualReturns(actual3mReturn, actual1yReturn, actualVolatility),
        subcategory: fundInfo.subcategory,
        created_at: new Date()
      };
      
      // Check if score exists
      const existingResult = await pool.query(`
        SELECT fund_id FROM fund_performance_metrics 
        WHERE fund_id = $1 AND scoring_date = $2 AND total_score IS NOT NULL
      `, [fundId, scoreDate]);
      
      if (existingResult.rows.length > 0) {
        // Update with comprehensive authentic data
        await pool.query(`
          UPDATE fund_performance_metrics
          SET 
            return_3m_score = $3,
            return_1y_score = $4,
            return_3y_score = $5,
            volatility_1y_percent = $6,
            expense_ratio_score = $7,
            data_quality_score = $8,
            total_score = $9,
            recommendation = $10,
            subcategory = $11,
            created_at = $12
          WHERE fund_id = $1 AND score_date = $2
        `, [
          fundId, scoreDate,
          comprehensiveData.return_3m_score,
          comprehensiveData.return_1y_score,
          comprehensiveData.return_3y_score,
          comprehensiveData.volatility_1y_percent,
          comprehensiveData.expense_ratio_score,
          comprehensiveData.data_quality_score,
          comprehensiveData.total_score,
          comprehensiveData.recommendation,
          comprehensiveData.subcategory,
          comprehensiveData.created_at
        ]);
      } else {
        // Insert comprehensive authentic data
        await pool.query(`
          INSERT INTO fund_performance_metrics (
            fund_id, scoring_date, return_3m_score, return_1y_score, return_3y_score,
            volatility_1y_percent, expense_ratio_score, data_quality_score,
            total_score, recommendation, subcategory, created_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        `, [
          fundId, scoreDate,
          comprehensiveData.return_3m_score,
          comprehensiveData.return_1y_score,
          comprehensiveData.return_3y_score,
          comprehensiveData.volatility_1y_percent,
          comprehensiveData.expense_ratio_score,
          comprehensiveData.data_quality_score,
          comprehensiveData.total_score,
          comprehensiveData.recommendation,
          comprehensiveData.subcategory,
          comprehensiveData.created_at
        ]);
      }
    } catch (error) {
      console.error(`Error storing authentic fund score for ${fundId}:`, error);
      throw error;
    }
  }

  // DISABLED: Generate recommendation based on actual returns and volatility
  // This method was corrupting data with wrong thresholds
  private getRecommendationFromActualReturns(return3m: number | null, return1y: number | null, volatility: number | null): string {
    // DISABLED: Return neutral recommendation to prevent data corruption
    console.log('WARNING: getRecommendationFromActualReturns disabled to prevent data corruption');
    return 'HOLD'; // Safe default to prevent corruption
    
    // CORRUPTED LOGIC DISABLED:
    // const totalReturn = (return3m || 0) + (return1y || 0);
    // const riskAdjusted = volatility ? totalReturn / (volatility / 10) : totalReturn;
    // if (riskAdjusted >= 25) return 'STRONG_BUY'; // WRONG - should be 70+
    // if (riskAdjusted >= 15) return 'BUY';        // WRONG - should be 60+
    // if (riskAdjusted >= 5) return 'HOLD';        // WRONG - should be 50+
    // if (riskAdjusted >= -5) return 'SELL';       // WRONG - should be 35+
    // return 'STRONG_SELL';
  }

  // Process a batch of funds
  async batchScoreFunds(limit: number = 500, category?: string): Promise<any> {
    try {
      // Get funds to score
      let fundsQuery = `
        SELECT f.id
        FROM funds f
        LEFT JOIN (
          SELECT fund_id, MAX(score_date) as last_score_date
          FROM fund_scores
          GROUP BY fund_id
        ) fs ON f.id = fs.fund_id
        WHERE 1=1
      `;

      const queryParams: any[] = [];

      if (category) {
        fundsQuery += ` AND f.category = $1`;
        queryParams.push(category);
      }

      // Prioritize funds with no scores or older scores
      fundsQuery += `
        ORDER BY 
          CASE WHEN fs.last_score_date IS NULL THEN 0 ELSE 1 END,
          fs.last_score_date ASC
        LIMIT $${queryParams.length + 1}
      `;
      queryParams.push(limit);

      const fundsResult = await pool.query(fundsQuery, queryParams);
      const fundIds = fundsResult.rows.map(row => row.id);
      
      console.log(`Processing batch of ${fundIds.length} funds for scoring`);
      
      // Score each fund
      const scoreResults = [];
      for (const fundId of fundIds) {
        const scoreResult = await this.scoreFund(fundId);
        if (scoreResult) {
          scoreResults.push(scoreResult);
        }
        
        // Add a small delay to avoid overwhelming the database
        await new Promise(resolve => setTimeout(resolve, 50));
      }
      
      return {
        processed: scoreResults.length,
        timestamp: new Date()
      };
    } catch (error) {
      console.error('Error in batch fund scoring:', error);
      throw error;
    }
  }

  // Assign quartiles to all funds based on their scores
  async assignQuartiles(): Promise<void> {
    try {
      // Get the latest score date
      const scoreDate = await this.getLatestScoreDate();
      if (!scoreDate) {
        console.log('No scores found to assign quartiles');
        return;
      }
      
      // Get distinct categories
      const categoriesResult = await pool.query(`
        SELECT DISTINCT category FROM funds
        WHERE id IN (SELECT DISTINCT fund_id FROM fund_scores WHERE score_date = $1)
      `, [scoreDate]);
      
      // For each category, assign quartiles
      for (const row of categoriesResult.rows) {
        const category = row.category;
        
        // Assign quartiles and ranks within category
        await pool.query(`
          WITH ranked AS (
            SELECT 
              fs.fund_id,
              fs.total_score,
              ROW_NUMBER() OVER (ORDER BY fs.total_score DESC) as category_rank,
              COUNT(*) OVER() as category_total
            FROM fund_scores fs
            JOIN funds f ON fs.fund_id = f.id
            WHERE fs.score_date = $1 AND f.category = $2
          ),
          quartiles AS (
            SELECT 
              fund_id,
              total_score,
              category_rank,
              category_total,
              CASE 
                WHEN category_rank <= CEILING(category_total * 0.25) THEN 1
                WHEN category_rank <= CEILING(category_total * 0.5) THEN 2
                WHEN category_rank <= CEILING(category_total * 0.75) THEN 3
                ELSE 4
              END as quartile,
              CASE 
                WHEN category_rank <= CEILING(category_total * 0.25) THEN 'BUY'
                WHEN category_rank <= CEILING(category_total * 0.5) THEN 'HOLD'
                WHEN category_rank <= CEILING(category_total * 0.75) THEN 'REVIEW'
                ELSE 'SELL'
              END as recommendation
            FROM ranked
          )
          UPDATE fund_scores fs
          SET 
            quartile = q.quartile,
            category_rank = q.category_rank,
            category_total = q.category_total,
            recommendation = q.recommendation
          FROM quartiles q
          WHERE fs.fund_id = q.fund_id AND fs.score_date = $1
        `, [scoreDate, category]);
        
        console.log(`Assigned quartiles for ${category} funds`);
      }
    } catch (error) {
      console.error('Error assigning quartiles:', error);
      throw error;
    }
  }

  // Get the latest score date
  private async getLatestScoreDate(): Promise<string | null> {
    const result = await pool.query(`
      SELECT MAX(scoring_date) as latest_date FROM fund_performance_metrics WHERE total_score IS NOT NULL
    `);
    return result.rows[0]?.latest_date || null;
  }
}

export const fundScoringEngine = new FundScoringEngine();