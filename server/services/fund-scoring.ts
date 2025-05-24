import { storage } from '../storage';
import { elivateFramework } from './elivate-framework';
import type { 
  Fund, 
  NavData, 
  MarketIndex, 
  InsertFundScore,
  PortfolioHolding
} from '@shared/schema';

// Weights for fund scoring
interface ScoringWeights {
  // Main components
  historicalReturns: number;
  riskGrade: number;
  otherMetrics: number;
  
  // Historical returns sub-weights
  return3m: number;
  return6m: number;
  return1y: number;
  return3y: number;
  return5y: number;
  
  // Risk grade sub-weights
  stdDev1y: number;
  stdDev3y: number;
  updownCapture1y: number;
  updownCapture3y: number;
  maxDrawdown: number;
  
  // Other metrics sub-weights
  sectoralSimilarity: number;
  forward: number;
  aumSize: number;
  expenseRatio: number;
}

// Fund Scoring Engine
export class FundScoringEngine {
  private static instance: FundScoringEngine;
  private weights: ScoringWeights;
  
  private constructor() {
    // Initialize scoring weights
    this.weights = {
      // Main components (total: 100)
      historicalReturns: 40,
      riskGrade: 30,
      otherMetrics: 30,
      
      // Historical returns sub-weights (total: 40)
      return3m: 5,
      return6m: 10,
      return1y: 10,
      return3y: 8,
      return5y: 7,
      
      // Risk grade sub-weights (total: 30)
      stdDev1y: 5,
      stdDev3y: 5,
      updownCapture1y: 8,
      updownCapture3y: 8,
      maxDrawdown: 4,
      
      // Other metrics sub-weights (total: 30)
      sectoralSimilarity: 10,
      forward: 10,
      aumSize: 5,
      expenseRatio: 5
    };
  }
  
  public static getInstance(): FundScoringEngine {
    if (!FundScoringEngine.instance) {
      FundScoringEngine.instance = new FundScoringEngine();
    }
    return FundScoringEngine.instance;
  }
  
  // Score a single fund
  async scoreFund(fundId: number): Promise<{ 
    fundId: number; 
    totalScore: number; 
    quartile: number;
    recommendation: string;
    scoreComponents: any;
  }> {
    try {
      // Get fund information
      const fund = await storage.getFund(fundId);
      if (!fund) {
        throw new Error(`Fund with ID ${fundId} not found`);
      }
      
      // Get NAV data
      const navData = await storage.getNavData(fundId, undefined, undefined, 2000); // ~5 years of data
      if (navData.length < 60) { // Need at least 60 days of data
        throw new Error(`Insufficient NAV data for fund ${fundId}`);
      }
      
      // Get category peers for percentile ranking
      const categoryFunds = await storage.getFundsByCategory(fund.category);
      
      // Get benchmark for the fund (using Nifty 50 as default for now)
      const benchmarkData = await storage.getMarketIndex('NIFTY 50');
      
      // Get the latest ELIVATE score for market context
      const elivateScore = await storage.getLatestElivateScore();
      
      // Get portfolio holdings
      const holdings = await storage.getPortfolioHoldings(fundId);
      
      // Calculate scores for each component
      const historicalReturnsScore = await this.calculateHistoricalReturnsScore(fundId, navData, categoryFunds);
      const riskGradeScore = await this.calculateRiskGradeScore(fundId, navData, benchmarkData, categoryFunds);
      const otherMetricsScore = await this.calculateOtherMetricsScore(fundId, fund, holdings, elivateScore);
      
      // Calculate total score
      const totalHistoricalReturnsScore = Object.values(historicalReturnsScore).reduce((sum, score) => sum + score, 0);
      const totalRiskGradeScore = Object.values(riskGradeScore).reduce((sum, score) => sum + score, 0);
      const totalOtherMetricsScore = Object.values(otherMetricsScore).reduce((sum, score) => sum + score, 0);
      
      const totalScore = totalHistoricalReturnsScore + totalRiskGradeScore + totalOtherMetricsScore;
      
      // Determine quartile
      const quartile = await this.determineQuartile(totalScore, fund.category);
      
      // Generate recommendation
      const recommendation = this.generateRecommendation(totalScore, quartile);
      
      // Create score record with both raw metrics and derived scores
      const scoreDate = new Date();
      
      // Calculate raw metrics for transparency
      const rawReturns = {
        return1m: this.calculateReturn(navData, 30),  // 1 month
        return3m: this.calculateReturn(navData, 90),  // 3 months
        return6m: this.calculateReturn(navData, 180), // 6 months
        return1y: this.calculateReturn(navData, 365), // 1 year
        return3y: this.calculateAnnualizedReturn(navData, 1095), // 3 years
        return5y: this.calculateAnnualizedReturn(navData, 1825)  // 5 years
      };
      
      // Calculate risk metrics
      const dailyReturns = this.calculateDailyReturns(navData);
      const benchmarkDailyReturns = benchmarkData ? this.calculateBenchmarkDailyReturns(benchmarkData) : [];
      
      // Volatility calculations
      const volatility1y = dailyReturns.length >= 252 ? 
        this.calculateStandardDeviation(dailyReturns.slice(0, 252)) * Math.sqrt(252) * 100 : null;
      const volatility3y = dailyReturns.length >= 756 ? 
        this.calculateStandardDeviation(dailyReturns.slice(0, 756)) * Math.sqrt(252) * 100 : null;
      
      // Risk-free rate (using 10-year government bond as proxy - set to a reasonable default)
      const riskFreeRate = 4.5; // 4.5% annual
      
      // Sharpe ratio calculations
      const sharpeRatio1y = volatility1y && rawReturns.return1y ? 
        (rawReturns.return1y - riskFreeRate) / volatility1y : null;
      const sharpeRatio3y = volatility3y && rawReturns.return3y ? 
        (rawReturns.return3y - riskFreeRate) / volatility3y : null;
      
      // Sortino ratio calculations (downside deviation)
      const downsideReturns1y = dailyReturns.length >= 252 ? 
        dailyReturns.slice(0, 252).filter(r => r < 0) : [];
      const downsideReturns3y = dailyReturns.length >= 756 ? 
        dailyReturns.slice(0, 756).filter(r => r < 0) : [];
      
      const downsideDeviation1y = downsideReturns1y.length > 0 ? 
        this.calculateStandardDeviation(downsideReturns1y) * Math.sqrt(252) * 100 : null;
      const downsideDeviation3y = downsideReturns3y.length > 0 ? 
        this.calculateStandardDeviation(downsideReturns3y) * Math.sqrt(252) * 100 : null;
      
      const sortinoRatio1y = downsideDeviation1y && rawReturns.return1y ? 
        (rawReturns.return1y - riskFreeRate) / downsideDeviation1y : null;
      const sortinoRatio3y = downsideDeviation3y && rawReturns.return3y ? 
        (rawReturns.return3y - riskFreeRate) / downsideDeviation3y : null;
      
      // Maximum drawdown
      const maxDrawdown = this.calculateMaxDrawdown(navData) * 100; // Convert to percentage
      
      // Up/down capture ratios
      const upCaptureRatio = benchmarkDailyReturns.length >= 252 && dailyReturns.length >= 252 ? 
        this.calculateUpCaptureRatio(dailyReturns.slice(0, 252), benchmarkDailyReturns.slice(0, 252)) : null;
      
      const downCaptureRatio = benchmarkDailyReturns.length >= 252 && dailyReturns.length >= 252 ? 
        this.calculateDownCaptureRatio(dailyReturns.slice(0, 252), benchmarkDailyReturns.slice(0, 252)) : null;
      
      // Consistency score
      let aboveMedianMonthsCount = 0;
      let totalMonthsEvaluated = 0;
      
      // Get median returns for each month in the category
      const monthlyMedianReturns = await this.getMonthlyMedianReturns(fund.category, 36); // 3 years
      if (monthlyMedianReturns.length > 0) {
        const fundMonthlyReturns = this.calculateMonthlyReturns(navData, 36);
        totalMonthsEvaluated = Math.min(fundMonthlyReturns.length, monthlyMedianReturns.length);
        
        for (let i = 0; i < totalMonthsEvaluated; i++) {
          if (fundMonthlyReturns[i] > monthlyMedianReturns[i]) {
            aboveMedianMonthsCount++;
          }
        }
      }
      
      const consistencyScore = totalMonthsEvaluated > 0 ? 
        aboveMedianMonthsCount / totalMonthsEvaluated : null;
      
      // Category benchmarks for comparison
      const categoryMedianReturns1y = this.calculateMedian(
        categoryFunds.map(f => this.getFundReturn(f.id, 365)).filter(r => r !== null)
      );
      
      const categoryMedianReturns3y = this.calculateMedian(
        categoryFunds.map(f => this.getFundReturn(f.id, 1095)).filter(r => r !== null)
      );
      
      // Expense ratio analysis
      const expenseRatios = categoryFunds
        .map(f => f.expenseRatio ? parseFloat(f.expenseRatio.toString()) : null)
        .filter(r => r !== null);
      
      const categoryMedianExpenseRatio = this.calculateMedian(expenseRatios);
      const categoryStdDevExpenseRatio = this.calculateStandardDeviation(expenseRatios);
      
      const fundExpenseRatio = fund.expenseRatio ? parseFloat(fund.expenseRatio.toString()) : null;
      
      const expenseRatioRank = fundExpenseRatio && categoryMedianExpenseRatio && categoryStdDevExpenseRatio ? 
        (categoryMedianExpenseRatio - fundExpenseRatio) / categoryStdDevExpenseRatio : null;
      
      // Fund size analysis (AUM)
      // For now, using fund size relative to category median as a proxy for AUM
      // In a real implementation, we would use actual AUM values
      const fundAum = 10000; // Placeholder - would come from actual data
      const categoryMedianAum = 5000; // Placeholder - would be calculated from actual data
      
      // Fund size factor (normalized to 0-1 range)
      const fundSizeFactor = fundAum && categoryMedianAum ? 
        Math.min(1, Math.max(0, 1 - Math.abs(Math.log(fundAum) / Math.log(categoryMedianAum) - 1))) : null;
      
      const fundScore: InsertFundScore = {
        fundId,
        scoreDate,
        
        // Raw return data (actual percentages)
        return1m: rawReturns.return1m,
        return3m: rawReturns.return3m,
        return6m: rawReturns.return6m,
        return1y: rawReturns.return1y,
        return3y: rawReturns.return3y,
        return5y: rawReturns.return5y,
        
        // Risk metrics - raw values
        volatility1y,
        volatility3y,
        sharpeRatio1y,
        sharpeRatio3y,
        sortinoRatio1y,
        sortinoRatio3y,
        maxDrawdown,
        upCaptureRatio,
        downCaptureRatio,
        
        // Quality metrics - raw values
        consistencyScore,
        categoryMedianExpenseRatio,
        categoryStdDevExpenseRatio,
        expenseRatioRank,
        fundAum,
        categoryMedianAum,
        fundSizeFactor,
        
        // Context fields
        riskFreeRate,
        categoryBenchmarkReturn1y: categoryMedianReturns1y,
        categoryBenchmarkReturn3y: categoryMedianReturns3y,
        medianReturns1y: categoryMedianReturns1y,
        medianReturns3y: categoryMedianReturns3y,
        aboveMedianMonthsCount,
        totalMonthsEvaluated,
        
        // Historical returns scores
        return3mScore: historicalReturnsScore.return3m,
        return6mScore: historicalReturnsScore.return6m,
        return1yScore: historicalReturnsScore.return1y,
        return3yScore: historicalReturnsScore.return3y,
        return5yScore: historicalReturnsScore.return5y,
        historicalReturnsTotal: totalHistoricalReturnsScore,
        
        // Risk grade scores
        stdDev1yScore: riskGradeScore.stdDev1y,
        stdDev3yScore: riskGradeScore.stdDev3y,
        updownCapture1yScore: riskGradeScore.updownCapture1y,
        updownCapture3yScore: riskGradeScore.updownCapture3y,
        maxDrawdownScore: riskGradeScore.maxDrawdown,
        riskGradeTotal: totalRiskGradeScore,
        
        // Other metrics scores
        sectoralSimilarityScore: otherMetricsScore.sectoralSimilarity,
        forwardScore: otherMetricsScore.forward,
        aumSizeScore: otherMetricsScore.aumSize,
        expenseRatioScore: otherMetricsScore.expenseRatio,
        otherMetricsTotal: totalOtherMetricsScore,
        
        // Final scoring
        totalScore,
        quartile,
        categoryRank: 0, // Will be updated later after all funds are scored
        categoryTotal: categoryFunds.length,
        recommendation,
      };
      
      // Save the score to database
      await storage.createFundScore(fundScore);
      
      return {
        fundId,
        totalScore,
        quartile,
        recommendation,
        scoreComponents: {
          historicalReturns: historicalReturnsScore,
          riskGrade: riskGradeScore,
          otherMetrics: otherMetricsScore
        }
      };
    } catch (error) {
      console.error(`Error scoring fund ${fundId}:`, error);
      throw error;
    }
  }
  
  // Score all funds in a category
  async scoreAllFundsInCategory(category: string): Promise<any[]> {
    const funds = await storage.getFundsByCategory(category);
    
    const results = [];
    for (const fund of funds) {
      try {
        const result = await this.scoreFund(fund.id);
        results.push(result);
      } catch (error) {
        console.error(`Error scoring fund ${fund.id}:`, error);
      }
    }
    
    // Sort results by total score to determine category rank
    results.sort((a, b) => b.totalScore - a.totalScore);
    
    // Update category ranks
    for (let i = 0; i < results.length; i++) {
      const categoryRank = i + 1;
      await storage.updateFundScore(
        results[i].fundId,
        new Date(), // Today's date - should match the score date used in scoreFund
        { categoryRank }
      );
      results[i].categoryRank = categoryRank;
    }
    
    return results;
  }
  
  // Score all funds across all categories
  async scoreAllFunds(): Promise<any[]> {
    // Get all active funds
    const allFunds = await storage.getAllFunds();
    
    // Group funds by category
    const fundsByCategory: Record<string, Fund[]> = {};
    for (const fund of allFunds) {
      if (!fundsByCategory[fund.category]) {
        fundsByCategory[fund.category] = [];
      }
      fundsByCategory[fund.category].push(fund);
    }
    
    // Score each category
    const allResults = [];
    for (const category in fundsByCategory) {
      try {
        const categoryResults = await this.scoreAllFundsInCategory(category);
        allResults.push(...categoryResults);
      } catch (error) {
        console.error(`Error scoring category ${category}:`, error);
      }
    }
    
    return allResults;
  }
  
  // Calculate historical returns score (40 points)
  private async calculateHistoricalReturnsScore(
    fundId: number,
    navData: NavData[],
    categoryFunds: Fund[]
  ): Promise<Record<string, number>> {
    const scores: Record<string, number> = {
      return3m: 0,
      return6m: 0,
      return1y: 0,
      return3y: 0,
      return5y: 0
    };
    
    // Calculate returns for different periods
    const returns = {
      '3m': this.calculateReturn(navData, 90),  // 3 months
      '6m': this.calculateReturn(navData, 180), // 6 months
      '1y': this.calculateReturn(navData, 365), // 1 year
      '3y': this.calculateAnnualizedReturn(navData, 1095), // 3 years
      '5y': this.calculateAnnualizedReturn(navData, 1825)  // 5 years
    };
    
    // Collect category returns for percentile scoring
    const categoryReturns: Record<string, number[]> = {
      '3m': [],
      '6m': [],
      '1y': [],
      '3y': [],
      '5y': []
    };
    
    // Calculate returns for all funds in the category
    for (const peerFund of categoryFunds) {
      if (peerFund.id === fundId) continue; // Skip current fund
      
      try {
        const peerNavData = await storage.getNavData(peerFund.id, undefined, undefined, 2000);
        
        if (peerNavData.length >= 90) {
          categoryReturns['3m'].push(this.calculateReturn(peerNavData, 90));
        }
        
        if (peerNavData.length >= 180) {
          categoryReturns['6m'].push(this.calculateReturn(peerNavData, 180));
        }
        
        if (peerNavData.length >= 365) {
          categoryReturns['1y'].push(this.calculateReturn(peerNavData, 365));
        }
        
        if (peerNavData.length >= 1095) {
          categoryReturns['3y'].push(this.calculateAnnualizedReturn(peerNavData, 1095));
        }
        
        if (peerNavData.length >= 1825) {
          categoryReturns['5y'].push(this.calculateAnnualizedReturn(peerNavData, 1825));
        }
      } catch (error) {
        console.error(`Error calculating returns for peer fund ${peerFund.id}:`, error);
      }
    }
    
    // Score based on percentile rank
    if (returns['3m'] !== null && categoryReturns['3m'].length > 0) {
      scores.return3m = this.scoreReturnPercentile(returns['3m']!, categoryReturns['3m'], this.weights.return3m);
    }
    
    if (returns['6m'] !== null && categoryReturns['6m'].length > 0) {
      scores.return6m = this.scoreReturnPercentile(returns['6m']!, categoryReturns['6m'], this.weights.return6m);
    }
    
    if (returns['1y'] !== null && categoryReturns['1y'].length > 0) {
      scores.return1y = this.scoreReturnPercentile(returns['1y']!, categoryReturns['1y'], this.weights.return1y);
    }
    
    if (returns['3y'] !== null && categoryReturns['3y'].length > 0) {
      scores.return3y = this.scoreReturnPercentile(returns['3y']!, categoryReturns['3y'], this.weights.return3y);
    }
    
    if (returns['5y'] !== null && categoryReturns['5y'].length > 0) {
      scores.return5y = this.scoreReturnPercentile(returns['5y']!, categoryReturns['5y'], this.weights.return5y);
    }
    
    return scores;
  }
  
  // Calculate risk grade score (30 points)
  private async calculateRiskGradeScore(
    fundId: number,
    navData: NavData[],
    benchmarkData: MarketIndex[],
    categoryFunds: Fund[]
  ): Promise<Record<string, number>> {
    const scores: Record<string, number> = {
      stdDev1y: 0,
      stdDev3y: 0,
      updownCapture1y: 0,
      updownCapture3y: 0,
      maxDrawdown: 0
    };
    
    // Calculate daily returns
    const dailyReturns = this.calculateDailyReturns(navData);
    
    if (dailyReturns.length < 252) { // Need at least 1 year of daily returns
      return scores;
    }
    
    // Calculate benchmark daily returns
    const benchmarkDailyReturns = this.calculateBenchmarkDailyReturns(benchmarkData);
    
    // Calculate standard deviation (volatility)
    const stdDev1y = this.calculateStandardDeviation(dailyReturns.slice(0, 252)) * Math.sqrt(252) * 100;
    const stdDev3y = dailyReturns.length >= 756 ? 
      this.calculateStandardDeviation(dailyReturns.slice(0, 756)) * Math.sqrt(252) * 100 : null;
    
    // Calculate up/down capture ratio
    const updownCapture1y = benchmarkDailyReturns.length >= 252 ? 
      this.calculateUpDownCaptureRatio(dailyReturns.slice(0, 252), benchmarkDailyReturns.slice(0, 252)) : null;
    
    const updownCapture3y = benchmarkDailyReturns.length >= 756 && dailyReturns.length >= 756 ? 
      this.calculateUpDownCaptureRatio(dailyReturns.slice(0, 756), benchmarkDailyReturns.slice(0, 756)) : null;
    
    // Calculate maximum drawdown
    const maxDrawdown = this.calculateMaxDrawdown(navData) * 100; // Convert to percentage
    
    // Collect category risk metrics for percentile scoring
    const categoryRiskMetrics: Record<string, number[]> = {
      stdDev1y: [],
      stdDev3y: [],
      updownCapture1y: [],
      updownCapture3y: [],
      maxDrawdown: []
    };
    
    // Calculate risk metrics for all funds in the category
    for (const peerFund of categoryFunds) {
      if (peerFund.id === fundId) continue; // Skip current fund
      
      try {
        const peerNavData = await storage.getNavData(peerFund.id, undefined, undefined, 2000);
        if (peerNavData.length < 252) continue;
        
        const peerDailyReturns = this.calculateDailyReturns(peerNavData);
        
        categoryRiskMetrics.stdDev1y.push(
          this.calculateStandardDeviation(peerDailyReturns.slice(0, 252)) * Math.sqrt(252) * 100
        );
        
        if (peerDailyReturns.length >= 756) {
          categoryRiskMetrics.stdDev3y.push(
            this.calculateStandardDeviation(peerDailyReturns.slice(0, 756)) * Math.sqrt(252) * 100
          );
        }
        
        if (benchmarkDailyReturns.length >= 252) {
          categoryRiskMetrics.updownCapture1y.push(
            this.calculateUpDownCaptureRatio(peerDailyReturns.slice(0, 252), benchmarkDailyReturns.slice(0, 252))
          );
        }
        
        if (benchmarkDailyReturns.length >= 756 && peerDailyReturns.length >= 756) {
          categoryRiskMetrics.updownCapture3y.push(
            this.calculateUpDownCaptureRatio(peerDailyReturns.slice(0, 756), benchmarkDailyReturns.slice(0, 756))
          );
        }
        
        categoryRiskMetrics.maxDrawdown.push(this.calculateMaxDrawdown(peerNavData) * 100);
      } catch (error) {
        console.error(`Error calculating risk metrics for peer fund ${peerFund.id}:`, error);
      }
    }
    
    // Score based on percentile rank (lower std dev and drawdown is better, higher updown capture is better)
    if (stdDev1y !== null && categoryRiskMetrics.stdDev1y.length > 0) {
      scores.stdDev1y = this.scoreVolatilityPercentile(stdDev1y, categoryRiskMetrics.stdDev1y, this.weights.stdDev1y);
    }
    
    if (stdDev3y !== null && categoryRiskMetrics.stdDev3y.length > 0) {
      scores.stdDev3y = this.scoreVolatilityPercentile(stdDev3y, categoryRiskMetrics.stdDev3y, this.weights.stdDev3y);
    }
    
    if (updownCapture1y !== null && categoryRiskMetrics.updownCapture1y.length > 0) {
      scores.updownCapture1y = this.scoreUpDownCapturePercentile(updownCapture1y, categoryRiskMetrics.updownCapture1y, this.weights.updownCapture1y);
    }
    
    if (updownCapture3y !== null && categoryRiskMetrics.updownCapture3y.length > 0) {
      scores.updownCapture3y = this.scoreUpDownCapturePercentile(updownCapture3y, categoryRiskMetrics.updownCapture3y, this.weights.updownCapture3y);
    }
    
    if (maxDrawdown !== null && categoryRiskMetrics.maxDrawdown.length > 0) {
      scores.maxDrawdown = this.scoreDrawdownPercentile(maxDrawdown, categoryRiskMetrics.maxDrawdown, this.weights.maxDrawdown);
    }
    
    return scores;
  }
  
  // Calculate other metrics score (30 points)
  private async calculateOtherMetricsScore(
    fundId: number,
    fund: Fund,
    holdings: PortfolioHolding[],
    elivateScore: any
  ): Promise<Record<string, number>> {
    const scores: Record<string, number> = {
      sectoralSimilarity: 0,
      forward: 0,
      aumSize: 0,
      expenseRatio: 0
    };
    
    // Calculate sectoral similarity score
    if (elivateScore && holdings.length > 0) {
      const modelAllocation = this.getModelSectorAllocation(elivateScore.marketStance);
      const currentAllocation = this.getCurrentSectorAllocation(holdings);
      
      scores.sectoralSimilarity = this.calculateSectoralSimilarityScore(
        currentAllocation,
        modelAllocation,
        this.weights.sectoralSimilarity
      );
    } else {
      // Default middle score if no data available
      scores.sectoralSimilarity = this.weights.sectoralSimilarity * 0.5;
    }
    
    // Calculate forward score based on ELIVATE framework and fund category
    if (elivateScore) {
      scores.forward = this.calculateForwardScore(
        elivateScore.totalElivateScore,
        elivateScore.marketStance,
        fund.category,
        this.weights.forward
      );
    } else {
      scores.forward = this.weights.forward * 0.5;
    }
    
    // Calculate AUM size score
    const latestNav = await storage.getLatestNav(fundId);
    if (latestNav?.aumCr) {
      scores.aumSize = this.calculateAumSizeScore(latestNav.aumCr, fund.category, this.weights.aumSize);
    } else {
      scores.aumSize = this.weights.aumSize * 0.5;
    }
    
    // Calculate expense ratio score (lower is better)
    if (fund.expenseRatio) {
      scores.expenseRatio = this.calculateExpenseRatioScore(fund.expenseRatio, fund.category, this.weights.expenseRatio);
    } else {
      scores.expenseRatio = this.weights.expenseRatio * 0.5;
    }
    
    return scores;
  }
  
  // Determine quartile based on total score and category
  private async determineQuartile(totalScore: number, category: string): Promise<number> {
    // Get all fund scores for this category
    const funds = await storage.getFundsByCategory(category);
    const allScores: number[] = [];
    
    for (const fund of funds) {
      try {
        const latestScore = await storage.getFundScore(fund.id);
        if (latestScore) {
          allScores.push(latestScore.totalScore);
        }
      } catch (error) {
        console.error(`Error getting score for fund ${fund.id}:`, error);
      }
    }
    
    // Include current score
    allScores.push(totalScore);
    allScores.sort((a, b) => b - a); // Sort in descending order
    
    const totalFunds = allScores.length;
    const position = allScores.indexOf(totalScore) + 1;
    
    // Determine quartile
    if (position <= Math.ceil(totalFunds / 4)) {
      return 1;
    } else if (position <= Math.ceil(totalFunds / 2)) {
      return 2;
    } else if (position <= Math.ceil(3 * totalFunds / 4)) {
      return 3;
    } else {
      return 4;
    }
  }
  
  // Generate recommendation based on score and quartile
  private generateRecommendation(totalScore: number, quartile: number): string {
    if (quartile === 1) {
      return 'BUY';
    } else if (quartile === 2) {
      return totalScore >= 65 ? 'HOLD' : 'REVIEW';
    } else if (quartile === 3) {
      return totalScore >= 50 ? 'REVIEW' : 'SELL';
    } else {
      return 'SELL';
    }
  }
  
  // Helper: Calculate simple point-to-point return
  private calculateReturn(navData: NavData[], days: number): number | null {
    if (navData.length < days) return null;
    
    const currentNav = navData[0].navValue;
    const pastNav = navData[Math.min(days, navData.length - 1)].navValue;
    
    return ((currentNav / pastNav) - 1) * 100; // Return in percentage
  }
  
  // Helper: Calculate annualized return
  private calculateAnnualizedReturn(navData: NavData[], days: number): number | null {
    if (navData.length < days) return null;
    
    const currentNav = navData[0].navValue;
    const pastNav = navData[Math.min(days, navData.length - 1)].navValue;
    const years = days / 365;
    
    return (Math.pow(currentNav / pastNav, 1 / years) - 1) * 100; // Annualized return in percentage
  }
  
  // Helper: Calculate daily returns
  private calculateDailyReturns(navData: NavData[]): number[] {
    const returns = [];
    
    // Sort by date in descending order (most recent first)
    const sortedNavData = [...navData].sort((a, b) => 
      new Date(b.navDate).getTime() - new Date(a.navDate).getTime()
    );
    
    for (let i = 0; i < sortedNavData.length - 1; i++) {
      const todayNav = sortedNavData[i].navValue;
      const yesterdayNav = sortedNavData[i + 1].navValue;
      
      returns.push((todayNav / yesterdayNav) - 1);
    }
    
    return returns;
  }
  
  // Helper: Calculate benchmark daily returns
  private calculateBenchmarkDailyReturns(benchmarkData: MarketIndex[]): number[] {
    const returns = [];
    
    // Sort by date in descending order (most recent first)
    const sortedData = [...benchmarkData].sort((a, b) => 
      new Date(b.indexDate).getTime() - new Date(a.indexDate).getTime()
    );
    
    for (let i = 0; i < sortedData.length - 1; i++) {
      const todayValue = sortedData[i].closeValue;
      const yesterdayValue = sortedData[i + 1].closeValue;
      
      if (todayValue && yesterdayValue) {
        returns.push((todayValue / yesterdayValue) - 1);
      }
    }
    
    return returns;
  }
  
  // Helper: Calculate standard deviation
  /**
   * Calculate standard deviation (volatility) of returns
   * Enhanced implementation with improved accuracy
   */
  private calculateStandardDeviation(returns: number[]): number {
    if (returns.length === 0) return 0;
    
    const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
    const squaredDiffs = returns.map(value => Math.pow(value - mean, 2));
    const variance = squaredDiffs.reduce((sum, value) => sum + value, 0) / returns.length;
    
    return Math.sqrt(variance);
  }
  
  /**
   * Calculate Sharpe ratio - a measure of risk-adjusted return
   * Higher values indicate better risk-adjusted performance
   */
  private calculateSharpeRatio(returns: number[], riskFreeRate: number = 0.04): number {
    if (returns.length === 0) return 0;
    
    // Convert annual risk-free rate to daily
    const dailyRiskFreeRate = Math.pow(1 + riskFreeRate, 1/252) - 1;
    
    // Calculate excess returns
    const excessReturns = returns.map(r => r - dailyRiskFreeRate);
    
    // Calculate average excess return
    const avgExcessReturn = excessReturns.reduce((sum, r) => sum + r, 0) / excessReturns.length;
    
    // Calculate standard deviation of returns
    const stdDev = this.calculateStandardDeviation(returns);
    
    // Calculate annualized Sharpe ratio
    if (stdDev === 0) return 0;
    return (avgExcessReturn / stdDev) * Math.sqrt(252);
  }
  
  /**
   * Calculate maximum drawdown - maximum loss from peak to trough
   */
  private calculateMaxDrawdown(navData: NavData[]): number {
    if (navData.length < 2) return 0;
    
    // Sort by date in descending order
    const sortedNavData = [...navData].sort((a, b) => 
      new Date(b.navDate).getTime() - new Date(a.navDate).getTime()
    );
    
    let maxDrawdown = 0;
    let peakNav = sortedNavData[sortedNavData.length - 1].navValue;
    
    for (let i = sortedNavData.length - 2; i >= 0; i--) {
      const currentNav = sortedNavData[i].navValue;
      
      // If we found a new peak, update it
      if (currentNav > peakNav) {
        peakNav = currentNav;
      } else {
        // Calculate drawdown from peak
        const drawdown = (peakNav - currentNav) / peakNav;
        maxDrawdown = Math.max(maxDrawdown, drawdown);
      }
    }
    
    return maxDrawdown;
  }
  
  /**
   * Calculate up capture ratio - how fund performs in up markets
   */
  private calculateUpCaptureRatio(fundReturns: number[], benchmarkReturns: number[]): number {
    if (fundReturns.length === 0 || benchmarkReturns.length === 0) return 0;
    
    // Consider only days when benchmark was positive
    const upDays = [];
    for (let i = 0; i < Math.min(fundReturns.length, benchmarkReturns.length); i++) {
      if (benchmarkReturns[i] > 0) {
        upDays.push({
          fund: fundReturns[i],
          benchmark: benchmarkReturns[i]
        });
      }
    }
    
    if (upDays.length === 0) return 0;
    
    // Calculate average returns on up days
    const avgFundReturn = upDays.reduce((sum, day) => sum + day.fund, 0) / upDays.length;
    const avgBenchmarkReturn = upDays.reduce((sum, day) => sum + day.benchmark, 0) / upDays.length;
    
    // Up capture ratio
    return avgBenchmarkReturn === 0 ? 0 : (avgFundReturn / avgBenchmarkReturn) * 100;
  }
  
  /**
   * Calculate down capture ratio - how fund performs in down markets
   */
  private calculateDownCaptureRatio(fundReturns: number[], benchmarkReturns: number[]): number {
    if (fundReturns.length === 0 || benchmarkReturns.length === 0) return 0;
    
    // Consider only days when benchmark was negative
    const downDays = [];
    for (let i = 0; i < Math.min(fundReturns.length, benchmarkReturns.length); i++) {
      if (benchmarkReturns[i] < 0) {
        downDays.push({
          fund: fundReturns[i],
          benchmark: benchmarkReturns[i]
        });
      }
    }
    
    if (downDays.length === 0) return 0;
    
    // Calculate average returns on down days
    const avgFundReturn = downDays.reduce((sum, day) => sum + day.fund, 0) / downDays.length;
    const avgBenchmarkReturn = downDays.reduce((sum, day) => sum + day.benchmark, 0) / downDays.length;
    
    // Down capture ratio (lower is better)
    return avgBenchmarkReturn === 0 ? 0 : (avgFundReturn / avgBenchmarkReturn) * 100;
  }
  
  /**
   * Calculate monthly returns for consistency score
   */
  private calculateMonthlyReturns(navData: NavData[], months: number): number[] {
    if (navData.length < 30) return [];
    
    // Sort by date in descending order
    const sortedNavData = [...navData].sort((a, b) => 
      new Date(b.navDate).getTime() - new Date(a.navDate).getTime()
    );
    
    // Group NAVs by month
    const navByMonth: Record<string, NavData[]> = {};
    for (const nav of sortedNavData) {
      const monthKey = new Date(nav.navDate).toISOString().slice(0, 7); // YYYY-MM format
      if (!navByMonth[monthKey]) {
        navByMonth[monthKey] = [];
      }
      navByMonth[monthKey].push(nav);
    }
    
    // Sort months chronologically
    const sortedMonths = Object.keys(navByMonth).sort().reverse();
    
    // Calculate monthly returns
    const monthlyReturns: number[] = [];
    for (let i = 0; i < Math.min(sortedMonths.length - 1, months); i++) {
      const currentMonth = sortedMonths[i];
      const previousMonth = sortedMonths[i + 1];
      
      if (navByMonth[currentMonth].length > 0 && navByMonth[previousMonth].length > 0) {
        const currentMonthNav = navByMonth[currentMonth][0].navValue;
        const previousMonthNav = navByMonth[previousMonth][0].navValue;
        
        const monthlyReturn = (currentMonthNav / previousMonthNav - 1) * 100;
        monthlyReturns.push(monthlyReturn);
      }
    }
    
    return monthlyReturns;
  }
  
  /**
   * Get median returns for each month across a category
   */
  private async getMonthlyMedianReturns(category: string, months: number): Promise<number[]> {
    // Get all funds in category
    const funds = await storage.getFundsByCategory(category);
    
    // Track returns by month across all funds
    const monthlyReturns: Record<string, number[]> = {};
    
    // Process each fund
    for (const fund of funds) {
      try {
        const navData = await storage.getNavData(fund.id, undefined, undefined, months * 31);
        if (navData.length < 60) continue; // Skip funds with insufficient data
        
        const fundMonthlyReturns = this.calculateMonthlyReturns(navData, months);
        
        // Add fund's returns to the month's collection
        for (let i = 0; i < fundMonthlyReturns.length; i++) {
          const monthKey = `month_${i}`;
          if (!monthlyReturns[monthKey]) {
            monthlyReturns[monthKey] = [];
          }
          monthlyReturns[monthKey].push(fundMonthlyReturns[i]);
        }
      } catch (error) {
        console.error(`Error getting monthly returns for fund ${fund.id}:`, error);
      }
    }
    
    // Calculate median for each month
    const medianReturns: number[] = [];
    for (let i = 0; i < months; i++) {
      const monthKey = `month_${i}`;
      if (monthlyReturns[monthKey] && monthlyReturns[monthKey].length > 0) {
        const median = this.calculateMedian(monthlyReturns[monthKey]);
        medianReturns.push(median);
      }
    }
    
    return medianReturns;
  }
  
  /**
   * Calculate median of an array of numbers
   */
  private calculateMedian(values: number[]): number {
    if (values.length === 0) return 0;
    
    const sorted = [...values].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    
    if (sorted.length % 2 === 0) {
      return (sorted[middle - 1] + sorted[middle]) / 2;
    } else {
      return sorted[middle];
    }
  }
  
  /**
   * Sortino ratio - focuses on downside risk only
   * Higher values indicate better downside risk-adjusted performance
   */
  private calculateSortinoRatio(returns: number[], riskFreeRate: number = 0.04): number {
    if (returns.length === 0) return 0;
    
    // Convert annual risk-free rate to daily
    const dailyRiskFreeRate = Math.pow(1 + riskFreeRate, 1/252) - 1;
    
    // Calculate excess returns
    const excessReturns = returns.map(r => r - dailyRiskFreeRate);
    
    // Calculate average excess return
    const avgExcessReturn = excessReturns.reduce((sum, r) => sum + r, 0) / excessReturns.length;
    
    // Calculate downside deviation (only negative returns)
    const negativeReturns = returns.filter(r => r < dailyRiskFreeRate).map(r => Math.pow(dailyRiskFreeRate - r, 2));
    if (negativeReturns.length === 0) return avgExcessReturn > 0 ? 10 : 0; // Handle case with no negative returns
    
    const downsideDeviation = Math.sqrt(negativeReturns.reduce((sum, r) => sum + r, 0) / negativeReturns.length);
    
    // Calculate annualized Sortino ratio
    if (downsideDeviation === 0) return 0;
    return (avgExcessReturn / downsideDeviation) * Math.sqrt(252);
  }
  
  // Helper: Calculate up/down capture ratio
  private calculateUpDownCaptureRatio(fundReturns: number[], benchmarkReturns: number[]): number {
    // Ensure arrays are of same length
    const length = Math.min(fundReturns.length, benchmarkReturns.length);
    
    if (length === 0) return 1.0; // Default neutral ratio
    
    // Split into up and down market periods
    const upMarketFund = [];
    const upMarketBenchmark = [];
    const downMarketFund = [];
    const downMarketBenchmark = [];
    
    for (let i = 0; i < length; i++) {
      if (benchmarkReturns[i] > 0) {
        upMarketFund.push(fundReturns[i]);
        upMarketBenchmark.push(benchmarkReturns[i]);
      } else if (benchmarkReturns[i] < 0) {
        downMarketFund.push(fundReturns[i]);
        downMarketBenchmark.push(benchmarkReturns[i]);
      }
    }
    
    // Calculate up and down capture ratios
    const upCapture = upMarketFund.length > 0 ? 
      (upMarketFund.reduce((sum, val) => sum + val, 0) / upMarketFund.length) / 
      (upMarketBenchmark.reduce((sum, val) => sum + val, 0) / upMarketBenchmark.length) : 1.0;
    
    const downCapture = downMarketFund.length > 0 ? 
      (downMarketFund.reduce((sum, val) => sum + val, 0) / downMarketFund.length) / 
      (downMarketBenchmark.reduce((sum, val) => sum + val, 0) / downMarketBenchmark.length) : 1.0;
    
    // Return ratio of up capture to down capture (higher is better)
    // For down markets, we want less capture (lower is better), so we invert
    return upCapture / Math.abs(downCapture);
  }
  
  // Helper: Calculate maximum drawdown
  private calculateMaxDrawdown(navData: NavData[]): number {
    if (navData.length === 0) return 0;
    
    // Sort by date in ascending order
    const sortedNavData = [...navData].sort((a, b) => 
      new Date(a.navDate).getTime() - new Date(b.navDate).getTime()
    );
    
    let maxDrawdown = 0;
    let peak = sortedNavData[0].navValue;
    let peakIndex = 0;
    let valleyIndex = 0;
    
    for (let i = 0; i < sortedNavData.length; i++) {
      const nav = sortedNavData[i];
      
      if (nav.navValue > peak) {
        peak = nav.navValue;
        peakIndex = i;
      } else {
        const drawdown = (peak - nav.navValue) / peak;
        if (drawdown > maxDrawdown) {
          maxDrawdown = drawdown;
          valleyIndex = i;
        }
      }
    }
    
    // Find recovery date (when NAV returns to the peak level)
    let recoveryIndex = -1;
    for (let i = valleyIndex + 1; i < sortedNavData.length; i++) {
      if (sortedNavData[i].navValue >= peak) {
        recoveryIndex = i;
        break;
      }
    }
    
    // Store drawdown details for reporting
    this.drawdownInfo = {
      maxDrawdown,
      peakDate: sortedNavData[peakIndex].navDate,
      valleyDate: sortedNavData[valleyIndex].navDate,
      recoveryDate: recoveryIndex !== -1 ? sortedNavData[recoveryIndex].navDate : null,
      recoveryPeriod: recoveryIndex !== -1 ? 
        (new Date(sortedNavData[recoveryIndex].navDate).getTime() - 
         new Date(sortedNavData[valleyIndex].navDate).getTime()) / (1000 * 60 * 60 * 24) : null
    };
    
    return maxDrawdown;
  }
  
  // Store information about the latest drawdown analysis
  private drawdownInfo: {
    maxDrawdown: number;
    peakDate: Date;
    valleyDate: Date;
    recoveryDate: Date | null;
    recoveryPeriod: number | null;
  } | null = null;
  
  /**
   * Calculate Beta - measure of systematic risk relative to the market
   * Beta < 1: Lower volatility than market
   * Beta = 1: Same volatility as market
   * Beta > 1: Higher volatility than market
   */
  private calculateBeta(fundReturns: number[], benchmarkReturns: number[]): number {
    const length = Math.min(fundReturns.length, benchmarkReturns.length);
    if (length < 30) return 1; // Default beta value
    
    // Calculate average returns
    const fundAvg = fundReturns.reduce((sum, r) => sum + r, 0) / length;
    const benchmarkAvg = benchmarkReturns.reduce((sum, r) => sum + r, 0) / length;
    
    // Calculate covariance and benchmark variance
    let covariance = 0;
    let benchmarkVariance = 0;
    
    for (let i = 0; i < length; i++) {
      covariance += (fundReturns[i] - fundAvg) * (benchmarkReturns[i] - benchmarkAvg);
      benchmarkVariance += Math.pow(benchmarkReturns[i] - benchmarkAvg, 2);
    }
    
    covariance /= length;
    benchmarkVariance /= length;
    
    // Calculate beta
    if (benchmarkVariance === 0) return 1;
    return covariance / benchmarkVariance;
  }
  
  /**
   * Calculate Alpha - excess return over what would be predicted by CAPM
   * Higher alpha indicates better risk-adjusted performance relative to benchmark
   */
  private calculateAlpha(fundReturns: number[], benchmarkReturns: number[], riskFreeRate: number = 0.04): number {
    const length = Math.min(fundReturns.length, benchmarkReturns.length);
    if (length < 30) return 0;
    
    // Convert annual risk-free rate to daily
    const dailyRiskFreeRate = Math.pow(1 + riskFreeRate, 1/252) - 1;
    
    // Calculate average returns
    const avgFundReturn = fundReturns.reduce((sum, r) => sum + r, 0) / length;
    const avgBenchmarkReturn = benchmarkReturns.reduce((sum, r) => sum + r, 0) / length;
    
    // Calculate beta
    const beta = this.calculateBeta(fundReturns, benchmarkReturns);
    
    // Calculate alpha (daily)
    const dailyAlpha = avgFundReturn - (dailyRiskFreeRate + beta * (avgBenchmarkReturn - dailyRiskFreeRate));
    
    // Annualize alpha
    return dailyAlpha * 252;
  }
  
  // Helper: Score return percentile
  private scoreReturnPercentile(value: number, peerValues: number[], maxPoints: number): number {
    // Higher return is better
    const percentile = this.calculatePercentile(value, peerValues, true);
    
    // Map percentile to score
    if (percentile >= 90) return maxPoints;
    if (percentile >= 75) return maxPoints * 0.8;
    if (percentile >= 50) return maxPoints * 0.6;
    if (percentile >= 25) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  // Helper: Score volatility percentile
  private scoreVolatilityPercentile(value: number, peerValues: number[], maxPoints: number): number {
    // Lower volatility is better
    const percentile = this.calculatePercentile(value, peerValues, false);
    
    // Map percentile to score
    if (percentile >= 90) return maxPoints;
    if (percentile >= 75) return maxPoints * 0.8;
    if (percentile >= 50) return maxPoints * 0.6;
    if (percentile >= 25) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  // Helper: Score up/down capture percentile
  private scoreUpDownCapturePercentile(value: number, peerValues: number[], maxPoints: number): number {
    // Higher up/down capture ratio is better
    const percentile = this.calculatePercentile(value, peerValues, true);
    
    // Map percentile to score
    if (percentile >= 90) return maxPoints;
    if (percentile >= 75) return maxPoints * 0.8;
    if (percentile >= 50) return maxPoints * 0.6;
    if (percentile >= 25) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  // Helper: Score drawdown percentile
  private scoreDrawdownPercentile(value: number, peerValues: number[], maxPoints: number): number {
    // Lower drawdown is better
    const percentile = this.calculatePercentile(value, peerValues, false);
    
    // Map percentile to score
    if (percentile >= 90) return maxPoints;
    if (percentile >= 75) return maxPoints * 0.8;
    if (percentile >= 50) return maxPoints * 0.6;
    if (percentile >= 25) return maxPoints * 0.4;
    return maxPoints * 0.2;
  }
  
  // Helper: Calculate percentile
  private calculatePercentile(value: number, values: number[], higherIsBetter: boolean): number {
    const count = values.length;
    if (count === 0) return 50; // Default to middle percentile
    
    let belowCount = 0;
    
    for (const val of values) {
      if (higherIsBetter) {
        if (val < value) belowCount++;
      } else {
        if (val > value) belowCount++;
      }
    }
    
    return (belowCount / count) * 100;
  }
  
  // Helper: Get model sector allocation
  private getModelSectorAllocation(marketStance: string): Record<string, number> {
    if (marketStance === 'BULLISH') {
      return {
        'Banking & Finance': 0.35,
        'Technology': 0.25,
        'Consumer Discretionary': 0.15,
        'Healthcare': 0.10,
        'Industrials': 0.10,
        'Others': 0.05
      };
    } else if (marketStance === 'NEUTRAL') {
      return {
        'Banking & Finance': 0.30,
        'Technology': 0.20,
        'Healthcare': 0.15,
        'Consumer Staples': 0.15,
        'Industrials': 0.10,
        'Others': 0.10
      };
    } else { // BEARISH
      return {
        'Healthcare': 0.25,
        'Consumer Staples': 0.25,
        'Banking & Finance': 0.20,
        'Technology': 0.15,
        'Utilities': 0.10,
        'Others': 0.05
      };
    }
  }
  
  // Helper: Get current sector allocation
  private getCurrentSectorAllocation(holdings: PortfolioHolding[]): Record<string, number> {
    const sectorAllocation: Record<string, number> = {};
    
    // Group holdings by sector
    for (const holding of holdings) {
      const sector = holding.sector || 'Others';
      const holdingPercent = holding.holdingPercent || 0;
      
      if (!sectorAllocation[sector]) {
        sectorAllocation[sector] = 0;
      }
      
      sectorAllocation[sector] += holdingPercent / 100; // Convert percentage to decimal
    }
    
    return sectorAllocation;
  }
  
  // Helper: Calculate sectoral similarity score
  /**
   * Enhanced sectoral similarity scoring with weighted sector comparison
   * This measures how well the fund is positioned relative to ELIVATE recommendations
   */
  private calculateSectoralSimilarityScore(
    currentAllocation: Record<string, number>,
    modelAllocation: Record<string, number>,
    maxPoints: number
  ): number {
    // Get all unique sectors from both allocations
    const allSectors = [...new Set([
      ...Object.keys(currentAllocation),
      ...Object.keys(modelAllocation)
    ])];
    
    // Define sector importance weights
    const sectorImportance: Record<string, number> = {
      'Banking & Finance': 1.5,
      'Technology': 1.4,
      'Healthcare': 1.3,
      'Consumer Discretionary': 1.2,
      'Industrials': 1.2,
      'Consumer Staples': 1.1,
      'Utilities': 1.0,
      'Pharma': 1.1,
      'Infrastructure': 1.0,
      'Auto': 0.9,
      'FMCG': 0.9,
      'Others': 0.7
    };
    
    let totalWeightedDifference = 0;
    let totalWeight = 0;
    
    // Calculate weighted differences for each sector
    for (const sector of allSectors) {
      const currentWeight = currentAllocation[sector] || 0;
      const modelWeight = modelAllocation[sector] || 0;
      
      // Get importance weight (default to 1.0 if not specified)
      const importance = sectorImportance[sector] || 1.0;
      
      // Calculate absolute difference
      const difference = Math.abs(currentWeight - modelWeight);
      
      // Apply additional penalty for overallocation vs underallocation
      // Overallocating to a sector is generally worse than underallocating
      const adjustedDifference = modelWeight > currentWeight 
        ? difference * 0.9  // Underallocation penalty
        : difference * 1.1; // Overallocation penalty
      
      totalWeightedDifference += adjustedDifference * importance;
      totalWeight += importance;
    }
    
    // Calculate normalized weighted difference (0-1)
    const avgWeightedDifference = totalWeightedDifference / totalWeight;
    
    // Convert to similarity score (0-1, higher is better)
    // Normalize to avoid extreme penalties
    let similarityScore = 1 - Math.min(1, avgWeightedDifference);
    similarityScore = Math.max(0.2, similarityScore); // Minimum score floor
    
    // Apply non-linear scoring for better differentiation
    const finalScore = Math.pow(similarityScore, 0.7); // Favor higher similarity
    
    // Store detailed similarity analysis for reporting
    this.lastSimilarityAnalysis = {
      overallSimilarity: similarityScore,
      adjustedSimilarity: finalScore,
      sectorDifferences: allSectors.map(sector => ({
        sector,
        currentAllocation: currentAllocation[sector] || 0,
        modelAllocation: modelAllocation[sector] || 0,
        difference: Math.abs((currentAllocation[sector] || 0) - (modelAllocation[sector] || 0))
      }))
    };
    
    // Scale to max points with more granular scoring
    return finalScore * maxPoints;
  }
  
  // Store detailed similarity analysis for advanced reporting
  private lastSimilarityAnalysis: {
    overallSimilarity: number;
    adjustedSimilarity: number;
    sectorDifferences: Array<{
      sector: string;
      currentAllocation: number;
      modelAllocation: number;
      difference: number;
    }>;
  } | null = null;
  
  // Helper: Calculate forward score
  /**
   * Enhanced forward score calculation that predicts future fund performance
   * This integrates the ELIVATE framework with fund category analysis
   */
  private calculateForwardScore(
    elivateScore: number,
    marketStance: string,
    category: string,
    maxPoints: number
  ): number {
    // Enhanced category classifications with more detailed mapping
    const categoryMapping: Record<string, {
      strongFit: string[];
      moderateFit: string[];
      weakFit: string[];
    }> = {
      BULLISH: {
        strongFit: ['Equity: Large Cap Growth', 'Equity: Flexi Cap', 'Equity: Mid Cap', 'Equity: Sectoral: Technology', 'Equity: Sectoral: Banking'],
        moderateFit: ['Equity: Multi Cap', 'Equity: Large Cap', 'Equity: ELSS', 'Equity: Small Cap', 'Equity: Focused'],
        weakFit: ['Hybrid: Balanced', 'Hybrid: Dynamic Asset Allocation', 'Equity: Value', 'Equity: Dividend Yield']
      },
      NEUTRAL: {
        strongFit: ['Equity: Multi Cap', 'Hybrid: Balanced', 'Hybrid: Dynamic Asset Allocation', 'Equity: Value', 'Equity: Large Cap'],
        moderateFit: ['Equity: Dividend Yield', 'Debt: Medium Duration', 'Hybrid: Aggressive', 'Equity: Contra'],
        weakFit: ['Equity: Small Cap', 'Debt: Short Duration', 'Debt: Corporate Bond', 'Debt: Banking & PSU']
      },
      BEARISH: {
        strongFit: ['Debt: Short Duration', 'Debt: Medium Duration', 'Hybrid: Conservative', 'Equity: Dividend Yield', 'Debt: Banking & PSU'],
        moderateFit: ['Debt: Corporate Bond', 'Hybrid: Balanced', 'Equity: Value', 'Debt: Gilt'],
        weakFit: ['Hybrid: Dynamic Asset Allocation', 'Equity: Large Cap', 'Equity: Multi Cap', 'Equity: Contra']
      }
    };
    
    // Original category lists for fallback
    const bullishFavoredCategories = ['Equity: Large Cap', 'Equity: Flexi Cap', 'Equity: Mid Cap', 'Equity: Sectoral'];
    const neutralFavoredCategories = ['Equity: Multi Cap', 'Hybrid: Balanced', 'Hybrid: Dynamic Asset Allocation'];
    const bearishFavoredCategories = ['Debt: Short Duration', 'Debt: Medium Duration', 'Hybrid: Conservative', 'Equity: Dividend Yield'];
    
    // Determine market regime strength based on ELIVATE score
    let regimeStrength = 0;
    
    if (marketStance === 'BULLISH') {
      regimeStrength = Math.min(1, Math.max(0, (elivateScore - 50) / 30));
    } else if (marketStance === 'BEARISH') {
      regimeStrength = Math.min(1, Math.max(0, (50 - elivateScore) / 30));
    } else { // NEUTRAL
      regimeStrength = Math.min(1, Math.max(0, 1 - Math.abs(elivateScore - 50) / 15));
    }
    
    // Calculate category alignment score
    let alignmentScore = 0;
    
    // Check if category matches any of the detailed mappings
    const mapping = categoryMapping[marketStance] || {
      strongFit: [],
      moderateFit: [],
      weakFit: []
    };
    
    const isStrongFit = mapping.strongFit.some(c => category.includes(c));
    const isModerateFit = mapping.moderateFit.some(c => category.includes(c));
    const isWeakFit = mapping.weakFit.some(c => category.includes(c));
    
    if (isStrongFit) {
      alignmentScore = 1.0;
    } else if (isModerateFit) {
      alignmentScore = 0.7;
    } else if (isWeakFit) {
      alignmentScore = 0.4;
    } else {
      // Use original categorization as fallback
      if (marketStance === 'BULLISH') {
        alignmentScore = bullishFavoredCategories.some(c => category.includes(c)) ? 0.8 :
                       neutralFavoredCategories.some(c => category.includes(c)) ? 0.5 : 0.2;
      } else if (marketStance === 'NEUTRAL') {
        alignmentScore = neutralFavoredCategories.some(c => category.includes(c)) ? 0.8 :
                       (bullishFavoredCategories.some(c => category.includes(c)) || bearishFavoredCategories.some(c => category.includes(c))) ? 0.5 : 0.2;
      } else { // BEARISH
        alignmentScore = bearishFavoredCategories.some(c => category.includes(c)) ? 0.8 :
                       neutralFavoredCategories.some(c => category.includes(c)) ? 0.5 : 0.2;
      }
    }
    
    // Calculate conviction based on ELIVATE score
    let conviction = 0;
    if (elivateScore >= 80) conviction = 1.0;      // Very strong conviction
    else if (elivateScore >= 65) conviction = 0.8; // Strong conviction
    else if (elivateScore >= 50) conviction = 0.6; // Moderate conviction
    else if (elivateScore >= 35) conviction = 0.4; // Weak conviction
    else conviction = 0.2;                         // Very weak conviction
    
    // Factor in regime strength
    alignmentScore = alignmentScore * 0.8 + regimeStrength * 0.2;
    
    // Calculate momentum factor (in a full implementation, this would use historical ELIVATE data)
    const momentumFactor = Math.min(1, Math.max(0, Math.abs(elivateScore - 50) / 30));
    
    // Store forward score analysis
    this.lastForwardScoreAnalysis = {
      elivateScore,
      marketStance,
      category,
      regimeStrength,
      categoryAlignment: isStrongFit ? 'Strong' : isModerateFit ? 'Moderate' : isWeakFit ? 'Weak' : 'Poor',
      alignmentScore,
      conviction,
      momentumFactor,
      forwardOutlook: alignmentScore > 0.7 ? 'Highly Favorable' :
                     alignmentScore > 0.5 ? 'Favorable' :
                     alignmentScore > 0.3 ? 'Neutral' : 'Unfavorable'
    };
    
    // Calculate final forward score with weighted components
    const forwardScore = (alignmentScore * 0.6 + conviction * 0.3 + momentumFactor * 0.1) * maxPoints;
    
    return forwardScore;
  }
  
  // Store forward score analysis for reporting
  // Type for storing the forward score analysis
  private lastForwardScoreAnalysis: {
    elivateScore: number;
    marketStance: string;
    category: string;
    regimeStrength: number;
    categoryAlignment: string;
    alignmentScore: number;
    conviction: number;
    momentumFactor: number;
    forwardOutlook: string;
  } | null = null;
  
  // Helper: Calculate AUM size score
  private calculateAumSizeScore(aumCr: number, category: string, maxPoints: number): number {
    // Different optimal AUM ranges for different categories
    let optimalAum = 0;
    let score = 0;
    
    if (category.includes('Small Cap')) {
      // For small cap, we prefer smaller AUM to maintain nimbleness
      if (aumCr <= 2000) score = 1.0;
      else if (aumCr <= 5000) score = 0.8;
      else if (aumCr <= 10000) score = 0.6;
      else if (aumCr <= 15000) score = 0.4;
      else score = 0.2;
    } else if (category.includes('Mid Cap')) {
      // For mid cap, moderate AUM is optimal
      if (aumCr >= 5000 && aumCr <= 15000) score = 1.0;
      else if (aumCr >= 2000 && aumCr <= 20000) score = 0.8;
      else if (aumCr >= 1000 && aumCr <= 25000) score = 0.6;
      else if (aumCr >= 500 && aumCr <= 30000) score = 0.4;
      else score = 0.2;
    } else if (category.includes('Large Cap') || category.includes('Flexi Cap')) {
      // For large cap, higher AUM is acceptable
      if (aumCr >= 10000) score = 1.0;
      else if (aumCr >= 5000) score = 0.8;
      else if (aumCr >= 2000) score = 0.6;
      else if (aumCr >= 1000) score = 0.4;
      else score = 0.2;
    } else {
      // Default scoring for other categories
      if (aumCr >= 5000 && aumCr <= 20000) score = 1.0;
      else if (aumCr >= 2000 && aumCr <= 30000) score = 0.8;
      else if (aumCr >= 1000 && aumCr <= 40000) score = 0.6;
      else if (aumCr >= 500) score = 0.4;
      else score = 0.2;
    }
    
    return score * maxPoints;
  }
  
  // Helper: Calculate expense ratio score
  private calculateExpenseRatioScore(expenseRatio: number, category: string, maxPoints: number): number {
    // Lower expense ratio is better, but benchmarked against category averages
    let score = 0;
    
    if (category.includes('Index') || category.includes('ETF')) {
      // Index funds should have very low expense ratios
      if (expenseRatio <= 0.2) score = 1.0;
      else if (expenseRatio <= 0.5) score = 0.8;
      else if (expenseRatio <= 0.75) score = 0.6;
      else if (expenseRatio <= 1.0) score = 0.4;
      else score = 0.2;
    } else if (category.includes('Equity')) {
      // Active equity funds typically have higher expense ratios
      if (expenseRatio <= 1.0) score = 1.0;
      else if (expenseRatio <= 1.5) score = 0.8;
      else if (expenseRatio <= 2.0) score = 0.6;
      else if (expenseRatio <= 2.25) score = 0.4;
      else score = 0.2;
    } else if (category.includes('Debt')) {
      // Debt funds should have lower expense ratios
      if (expenseRatio <= 0.5) score = 1.0;
      else if (expenseRatio <= 0.8) score = 0.8;
      else if (expenseRatio <= 1.0) score = 0.6;
      else if (expenseRatio <= 1.2) score = 0.4;
      else score = 0.2;
    } else {
      // Default scoring for other categories
      if (expenseRatio <= 1.0) score = 1.0;
      else if (expenseRatio <= 1.5) score = 0.8;
      else if (expenseRatio <= 2.0) score = 0.6;
      else if (expenseRatio <= 2.5) score = 0.4;
      else score = 0.2;
    }
    
    return score * maxPoints;
  }
}

// Export singleton instance
export const fundScoringEngine = FundScoringEngine.getInstance();
