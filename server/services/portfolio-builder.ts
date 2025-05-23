import { storage } from '../storage';
import { elivateFramework } from './elivate-framework';
import { fundScoringEngine } from './fund-scoring';
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
  
  // Get asset allocation based on risk profile and market stance
  private getAssetAllocation(riskProfile: RiskProfile, marketStance: string): AssetAllocation {
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
  
  // Select top-rated funds for each asset class
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
    
    // Select funds for each asset class
    for (const [assetKey, percentage] of Object.entries(allocation)) {
      if (percentage <= 0) continue;
      
      const category = categoryMap[assetKey as keyof AssetAllocation];
      
      try {
        // Get top funds in category
        const topFunds = await storage.getLatestFundScores(3, category);
        
        if (topFunds.length > 0) {
          // Only include funds with BUY or HOLD recommendation
          const eligibleFunds = topFunds.filter(fund => 
            fund.recommendation === 'BUY' || fund.recommendation === 'HOLD'
          );
          
          if (eligibleFunds.length > 0) {
            const preferredFund = eligibleFunds[0]; // Take the highest-rated fund
            
            recommendedFunds.push({
              fund: preferredFund.fund,
              category,
              allocation: percentage,
              score: preferredFund.totalScore
            });
          }
        }
      } catch (error) {
        console.error(`Error selecting funds for ${category}:`, error);
      }
    }
    
    return recommendedFunds;
  }
  
  // Calculate expected returns range
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
}

// Export singleton instance
export const portfolioBuilder = PortfolioBuilder.getInstance();
