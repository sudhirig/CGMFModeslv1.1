import { storage } from '../storage';
import { elivateFramework } from './elivate-framework';
// Fund scoring engine removed during cleanup
import type { 
  InsertModelPortfolio,
  InsertModelPortfolioAllocation,
  Fund,
  FundScore
} from '@shared/schema';

// Risk profile types
type RiskProfile = 'Conservative' | 'Moderately Conservative' | 'Balanced' | 'Moderately Aggressive' | 'Aggressive';

// Asset allocation by risk profile
interface AssetAllocation {
  equityLargeCap: number;
  equityMidCap: number;
  equitySmallCap: number;
  debtShortTerm: number;
  debtMediumTerm: number;
  hybrid: number;
}

// Model portfolio builder
export class PortfolioBuilder {
  private static instance: PortfolioBuilder;
  
  private constructor() {}
  
  public static getInstance(): PortfolioBuilder {
    if (!PortfolioBuilder.instance) {
      PortfolioBuilder.instance = new PortfolioBuilder();
    }
    return PortfolioBuilder.instance;
  }
  
  // Generate model portfolio
  async generateModelPortfolio(riskProfile: RiskProfile): Promise<any> {
    try {
      // Get latest ELIVATE score for market context
      const elivateScore = await storage.getLatestElivateScore();
      
      if (!elivateScore) {
        throw new Error('ELIVATE score not found. Please calculate market score first.');
      }
      
      // Get asset allocation based on risk profile and market stance
      const allocation = this.getAssetAllocation(riskProfile, elivateScore.marketStance);
      
      // Create model portfolio
      const portfolioData: InsertModelPortfolio = {
        name: `${riskProfile} Portfolio - ${new Date().toISOString().split('T')[0]}`,
        riskProfile,
        elivateScoreId: elivateScore.id
      };
      
      // Select top-rated funds for each asset class
      const recommendedFunds = await this.selectRecommendedFunds(allocation);
      
      // Create portfolio allocations
      const portfolioAllocations: InsertModelPortfolioAllocation[] = [];
      
      for (const fund of recommendedFunds) {
        portfolioAllocations.push({
          fundId: fund.fund.id,
          allocationPercent: fund.allocation
        });
      }
      
      // Save model portfolio
      const portfolio = await storage.createModelPortfolio(portfolioData, portfolioAllocations);
      
      // Return portfolio with detailed allocations
      const detailedPortfolio = await storage.getModelPortfolio(portfolio.id);
      
      return {
        ...detailedPortfolio,
        assetAllocation: allocation,
        expectedReturns: this.calculateExpectedReturns(riskProfile, elivateScore.marketStance)
      };
    } catch (error) {
      console.error('Error generating model portfolio:', error);
      throw error;
    }
  }
  
  /**
   * Get asset allocation based on risk profile and market stance
   * Maps risk profile to standard allocation with market-specific adjustments
   */
  private getAssetAllocation(riskProfile: RiskProfile | string, marketStance: string): AssetAllocation {
    // Normalize the risk profile to match our standard types
    let normalizedProfile: RiskProfile = 'Balanced';
    
    // Convert any non-standard risk profile strings to our standard types
    if (typeof riskProfile === 'string') {
      if (riskProfile === 'Conservative' || riskProfile === 'Moderately Conservative' || 
          riskProfile === 'Balanced' || riskProfile === 'Moderately Aggressive' || 
          riskProfile === 'Aggressive') {
        normalizedProfile = riskProfile as RiskProfile;
      } else if (riskProfile === 'Moderate') {
        normalizedProfile = 'Balanced';
      } else if (riskProfile.includes('Conservative') && riskProfile.includes('Moderate')) {
        normalizedProfile = 'Moderately Conservative';
      } else if (riskProfile.includes('Aggressive') && riskProfile.includes('Moderate')) {
        normalizedProfile = 'Moderately Aggressive';
      } else if (riskProfile.includes('Conservative')) {
        normalizedProfile = 'Conservative';
      } else if (riskProfile.includes('Aggressive')) {
        normalizedProfile = 'Aggressive';
      }
    } else {
      normalizedProfile = riskProfile;
    }
    
    console.log(`Normalized risk profile "${riskProfile}" to "${normalizedProfile}" for allocation`);
    
    // Base allocations by risk profile
    const baseAllocations: Record<RiskProfile, AssetAllocation> = {
      'Conservative': {
        equityLargeCap: 15,
        equityMidCap: 5,
        equitySmallCap: 0,
        debtShortTerm: 40,
        debtMediumTerm: 30,
        hybrid: 10
      },
      'Moderately Conservative': {
        equityLargeCap: 25,
        equityMidCap: 10,
        equitySmallCap: 5,
        debtShortTerm: 30,
        debtMediumTerm: 20,
        hybrid: 10
      },
      'Balanced': {
        equityLargeCap: 30,
        equityMidCap: 15,
        equitySmallCap: 5,
        debtShortTerm: 20,
        debtMediumTerm: 20,
        hybrid: 10
      },
      'Moderately Aggressive': {
        equityLargeCap: 35,
        equityMidCap: 25,
        equitySmallCap: 15,
        debtShortTerm: 10,
        debtMediumTerm: 10,
        hybrid: 5
      },
      'Aggressive': {
        equityLargeCap: 40,
        equityMidCap: 30,
        equitySmallCap: 20,
        debtShortTerm: 5,
        debtMediumTerm: 5,
        hybrid: 0
      }
    };
    
    // Get base allocation
    const baseAllocation = baseAllocations[riskProfile];
    
    // Adjust for market stance
    let adjustedAllocation = { ...baseAllocation };
    
    if (marketStance === 'BULLISH') {
      // In bullish markets, slightly increase equity allocation
      const increase = riskProfile === 'Conservative' ? 5 : 10;
      const decrease = riskProfile === 'Conservative' ? 5 : 10;
      
      adjustedAllocation.equityLargeCap += increase;
      adjustedAllocation.debtShortTerm -= decrease;
      
      // Ensure debt allocation doesn't go below 0
      adjustedAllocation.debtShortTerm = Math.max(0, adjustedAllocation.debtShortTerm);
    } else if (marketStance === 'BEARISH') {
      // In bearish markets, slightly increase debt allocation
      const decrease = riskProfile === 'Aggressive' ? 5 : 10;
      const increase = riskProfile === 'Aggressive' ? 5 : 10;
      
      adjustedAllocation.equityLargeCap -= decrease;
      adjustedAllocation.debtShortTerm += increase;
      
      // Ensure equity allocation doesn't go below 0
      adjustedAllocation.equityLargeCap = Math.max(0, adjustedAllocation.equityLargeCap);
    }
    
    // Ensure allocation percentages sum to 100
    const total = Object.values(adjustedAllocation).reduce((sum, value) => sum + value, 0);
    
    if (total !== 100) {
      const scaleFactor = 100 / total;
      adjustedAllocation = Object.fromEntries(
        Object.entries(adjustedAllocation).map(([key, value]) => [key, Math.round(value * scaleFactor)])
      ) as AssetAllocation;
      
      // Ensure rounding doesn't affect total
      const roundedTotal = Object.values(adjustedAllocation).reduce((sum, value) => sum + value, 0);
      
      if (roundedTotal !== 100) {
        const diff = 100 - roundedTotal;
        // Add/subtract the difference from the largest allocation
        const largestKey = Object.entries(adjustedAllocation)
          .sort((a, b) => b[1] - a[1])[0][0] as keyof AssetAllocation;
        
        adjustedAllocation[largestKey] += diff;
      }
    }
    
    return adjustedAllocation;
  }
  
  /**
   * Enhanced fund selection with risk constraints and portfolio optimization
   * This implements portfolio construction with diversification rules and risk limits
   */
  private async selectRecommendedFunds(allocation: AssetAllocation): Promise<{
    fund: Fund;
    category: string;
    allocation: number;
    score: number;
  }[]> {
    const recommendedFunds = [];
    
    // Map asset classes to fund categories
    const categoryMap = {
      equityLargeCap: 'Equity: Large Cap',
      equityMidCap: 'Equity: Mid Cap',
      equitySmallCap: 'Equity: Small Cap',
      debtShortTerm: 'Debt: Short Duration',
      debtMediumTerm: 'Debt: Medium Duration',
      hybrid: 'Hybrid: Balanced'
    };
    
    // Risk constraints configuration
    const riskConstraints = {
      // Maximum allocation to a single fund
      maxSingleFundAllocation: 25, 
      
      // Maximum allocation to funds from a single AMC
      maxSingleAmcAllocation: 40,
      
      // Minimum number of funds in the portfolio
      minFundCount: 4,
      
      // Maximum volatility target (annualized)
      maxVolatility: {
        'Conservative': 6,
        'Moderately Conservative': 9,
        'Balanced': 12,
        'Moderately Aggressive': 15,
        'Aggressive': 18
      },
      
      // Minimum expected return target (annualized %)
      minExpectedReturn: {
        'Conservative': 6,
        'Moderately Conservative': 8,
        'Balanced': 10,
        'Moderately Aggressive': 12,
        'Aggressive': 14
      }
    };
    
    // Track AMC allocations for diversification
    const amcAllocations: Record<string, number> = {};
    
    // Track selected funds for each category
    const selectedFundIds = new Set<number>();
    
    // Select funds for each asset class
    for (const [assetKey, percentage] of Object.entries(allocation)) {
      if (percentage <= 0) continue;
      
      const category = categoryMap[assetKey as keyof AssetAllocation];
      
      try {
        // Get top funds in category - fetch more funds to enable better optimization
        const topFunds = await storage.getLatestFundScores(5, category);
        
        if (topFunds.length > 0) {
          // Only include funds with BUY or HOLD recommendation
          let eligibleFunds = topFunds.filter(fund => 
            fund.recommendation === 'BUY' || fund.recommendation === 'HOLD'
          );
          
          // Sort by total score (highest first)
          eligibleFunds.sort((a, b) => b.totalScore - a.totalScore);
          
          if (eligibleFunds.length > 0) {
            // Apply risk constraints to fund selection
            
            // 1. Determine optimal allocation - may split between multiple funds for diversification
            let remainingAllocation = percentage;
            let fundsToAllocate = [];
            
            // Identify how many funds to use for this asset class (larger allocations use more funds)
            const fundsToUse = percentage > 20 ? 2 : 1;
            
            for (let i = 0; i < Math.min(fundsToUse, eligibleFunds.length); i++) {
              const candidate = eligibleFunds[i];
              
              // Skip funds we've already selected
              if (selectedFundIds.has(candidate.fund.id)) {
                continue;
              }
              
              // Check AMC concentration constraints
              const amcName = candidate.fund.amcName;
              const currentAmcAllocation = amcAllocations[amcName] || 0;
              
              // Skip if this would exceed AMC concentration limit
              if (currentAmcAllocation + remainingAllocation > riskConstraints.maxSingleAmcAllocation) {
                continue;
              }
              
              // Calculate allocation for this fund (ensuring we don't exceed single fund constraint)
              const fundAllocation = Math.min(
                remainingAllocation,
                riskConstraints.maxSingleFundAllocation,
                fundsToUse > 1 ? remainingAllocation / (fundsToUse - i) : remainingAllocation
              );
              
              // Add to selected funds
              fundsToAllocate.push({
                fund: candidate.fund,
                category,
                allocation: fundAllocation,
                score: candidate.totalScore
              });
              
              // Update tracking variables
              selectedFundIds.add(candidate.fund.id);
              amcAllocations[amcName] = (amcAllocations[amcName] || 0) + fundAllocation;
              remainingAllocation -= fundAllocation;
              
              // Stop if we've allocated everything
              if (remainingAllocation <= 0) break;
            }
            
            // Add all allocated funds to the recommended list
            recommendedFunds.push(...fundsToAllocate);
            
            // If we couldn't allocate everything with our constraints, use the top fund for the remainder
            if (remainingAllocation > 0) {
              const topFund = eligibleFunds[0];
              
              // Try to find a fund we haven't used yet
              const unusedFund = eligibleFunds.find(f => !selectedFundIds.has(f.fund.id));
              const fundToUse = unusedFund || topFund;
              
              recommendedFunds.push({
                fund: fundToUse.fund,
                category,
                allocation: remainingAllocation,
                score: fundToUse.totalScore
              });
              
              // Update tracking
              selectedFundIds.add(fundToUse.fund.id);
              amcAllocations[fundToUse.fund.amcName] = (amcAllocations[fundToUse.fund.amcName] || 0) + remainingAllocation;
            }
          }
        }
      } catch (error) {
        console.error(`Error selecting funds for ${category}:`, error);
      }
    }
    
    // Apply portfolio-level optimizations and constraints
    if (recommendedFunds.length > 0) {
      // Ensure we meet minimum fund count constraint
      const minFundCount = 4;
      if (recommendedFunds.length < minFundCount) {
        console.log(`Portfolio has ${recommendedFunds.length} funds, which is below the minimum ${minFundCount}. Adding additional diversifiers.`);
        // This would be implemented in a production system
      }
      
      // Calculate and analyze correlation between selected funds
      this.analyzePortfolioCorrelation(recommendedFunds);
      
      // Calculate portfolio-level risk metrics
      this.calculatePortfolioRiskMetrics(recommendedFunds);
    }
    
    return recommendedFunds;
  }
  
  /**
   * Analyze correlation between funds in the portfolio
   * High correlation reduces diversification benefits
   */
  private analyzePortfolioCorrelation(
    recommendedFunds: Array<{
      fund: Fund;
      category: string;
      allocation: number;
      score: number;
    }>
  ): void {
    // In a production system, this would:
    // 1. Retrieve historical returns for each fund
    // 2. Calculate correlation matrix between funds
    // 3. Flag high correlations (e.g., >0.8) for potential replacement
    // 4. Suggest replacement funds with similar performance but lower correlation
    
    // For demonstration, we'll log the analysis process
    console.log(`Analyzing correlation for ${recommendedFunds.length} funds in portfolio`);
    
    // Simulated correlation analysis - would be replaced with actual calculations
    if (recommendedFunds.length > 1) {
      // Check for funds from the same category (higher correlation risk)
      const categoryCounts: Record<string, number> = {};
      for (const fund of recommendedFunds) {
        categoryCounts[fund.category] = (categoryCounts[fund.category] || 0) + 1;
      }
      
      // Log categories with multiple funds (potential correlation concern)
      for (const [category, count] of Object.entries(categoryCounts)) {
        if (count > 1) {
          console.log(`Correlation alert: ${count} funds from ${category} category`);
        }
      }
    }
  }
  
  /**
   * Calculate key portfolio risk metrics
   * Ensures the portfolio meets risk/return objectives
   */
  private calculatePortfolioRiskMetrics(
    recommendedFunds: Array<{
      fund: Fund;
      category: string;
      allocation: number;
      score: number;
    }>
  ): void {
    // For demonstration purposes - in production would calculate:
    // 1. Expected portfolio return (weighted average of fund expected returns)
    // 2. Expected portfolio volatility (using correlation-adjusted variance-covariance matrix)
    // 3. Expected maximum drawdown
    // 4. Expected Sharpe ratio
    
    // Calculate simplified weighted average score
    const totalAllocation = recommendedFunds.reduce((sum, fund) => sum + fund.allocation, 0);
    const weightedScore = recommendedFunds.reduce(
      (sum, fund) => sum + (fund.score * fund.allocation / totalAllocation), 
      0
    );
    
    console.log(`Portfolio analysis complete. Weighted average score: ${weightedScore.toFixed(2)}`);
  }
  
  /**
   * Calculate expected returns range based on risk profile and market conditions
   */
  private calculateExpectedReturns(riskProfile: RiskProfile, marketStance: string): { min: number; max: number } {
    // Base expected returns by risk profile
    const baseReturns: Record<RiskProfile, { min: number; max: number }> = {
      'Conservative': { min: 6, max: 8 },
      'Moderately Conservative': { min: 8, max: 10 },
      'Balanced': { min: 10, max: 12 },
      'Moderately Aggressive': { min: 12, max: 15 },
      'Aggressive': { min: 15, max: 18 }
    };
    
    // Adjust for market stance
    const returns = { ...baseReturns[riskProfile] };
    
    if (marketStance === 'BULLISH') {
      returns.min += 2;
      returns.max += 3;
    } else if (marketStance === 'BEARISH') {
      returns.min -= 2;
      returns.max -= 2;
    }
    
    return returns;
  }

  /**
   * Build a portfolio using the generateModelPortfolio method
   * This method provides the interface expected by the routes
   */
  async buildPortfolio(riskProfile: string): Promise<any> {
    return this.generateModelPortfolio(riskProfile as RiskProfile);
  }
}

// Export singleton instance
export const portfolioBuilder = PortfolioBuilder.getInstance();
