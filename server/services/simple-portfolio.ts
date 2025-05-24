import { db, pool } from '../db';
import { eq, and, or, like } from 'drizzle-orm';
import { funds } from '../../shared/schema';

/**
 * A simple service to generate portfolios with real fund allocations
 */
export class SimplePortfolioService {
  /**
   * Generate a model portfolio with real fund allocations
   */
  async generatePortfolio(riskProfile: string) {
    try {
      console.log(`Generating ${riskProfile} portfolio with real fund allocations`);
      
      // Get asset allocation based on risk profile
      const assetAllocation = this.getAllocationForRiskProfile(riskProfile);
      
      // Define expected returns
      const expectedReturns = this.getExpectedReturnsForRiskProfile(riskProfile);
      
      // Fetch real fund data from different categories
      const equityLargeCapFunds = await db.select().from(funds)
        .where(like(funds.category, '%Large%'))
        .limit(2);
        
      const equityMidCapFunds = await db.select().from(funds)
        .where(like(funds.category, '%Mid%'))
        .limit(2);
        
      const equitySmallCapFunds = await db.select().from(funds)
        .where(like(funds.category, '%Small%'))
        .limit(2);
        
      const debtShortTermFunds = await db.select().from(funds)
        .where(like(funds.category, '%Short%'))
        .limit(2);
        
      const debtMediumTermFunds = await db.select().from(funds)
        .where(like(funds.category, '%Medium%'))
        .limit(2);
        
      const hybridFunds = await db.select().from(funds)
        .where(like(funds.category, '%Hybrid%'))
        .limit(2);
      
      // If we didn't find enough categorized funds, get some more general ones
      const generalFunds = await db.select().from(funds)
        .limit(12);
      
      // Combine all funds into categories and ensure we have enough funds
      const largeCaps = equityLargeCapFunds.length > 0 ? equityLargeCapFunds : generalFunds.slice(0, 2);
      const midCaps = equityMidCapFunds.length > 0 ? equityMidCapFunds : generalFunds.slice(2, 4);
      const smallCaps = equitySmallCapFunds.length > 0 ? equitySmallCapFunds : generalFunds.slice(4, 6);
      const shortTerms = debtShortTermFunds.length > 0 ? debtShortTermFunds : generalFunds.slice(6, 8);
      const mediumTerms = debtMediumTermFunds.length > 0 ? debtMediumTermFunds : generalFunds.slice(8, 10);
      const hybrids = hybridFunds.length > 0 ? hybridFunds : generalFunds.slice(10, 12);
      
      // Create allocations from real fund data
      const allocations = [];
      
      // Add large cap funds
      if (assetAllocation.equityLargeCap > 0 && largeCaps.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equityLargeCap / largeCaps.length);
        for (const fund of largeCaps) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Add mid cap funds
      if (assetAllocation.equityMidCap > 0 && midCaps.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equityMidCap / midCaps.length);
        for (const fund of midCaps) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Add small cap funds
      if (assetAllocation.equitySmallCap > 0 && smallCaps.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equitySmallCap / smallCaps.length);
        for (const fund of smallCaps) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Add debt short term funds
      if (assetAllocation.debtShortTerm > 0 && shortTerms.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.debtShortTerm / shortTerms.length);
        for (const fund of shortTerms) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Add debt medium term funds
      if (assetAllocation.debtMediumTerm > 0 && mediumTerms.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.debtMediumTerm / mediumTerms.length);
        for (const fund of mediumTerms) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Add hybrid funds
      if (assetAllocation.hybrid > 0 && hybrids.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.hybrid / hybrids.length);
        for (const fund of hybrids) {
          allocations.push({
            fund,
            allocationPercent: perFundAllocation
          });
        }
      }
      
      // Create portfolio object with allocations
      const portfolio = {
        id: Date.now(), // Use timestamp as simple ID
        name: `${riskProfile} Portfolio`,
        riskProfile: riskProfile,
        elivateScoreId: 2,
        assetAllocation,
        expectedReturns,
        allocations: allocations.map(allocation => ({
          portfolioId: Date.now(),
          fundId: allocation.fund.id,
          allocationPercent: allocation.allocationPercent,
          fund: {
            id: allocation.fund.id,
            fundName: allocation.fund.fund_name,
            amcName: allocation.fund.amc_name,
            category: allocation.fund.category || 'Mixed Asset',
            subcategory: allocation.fund.subcategory
          }
        })),
        createdAt: new Date().toISOString()
      };
      
      return portfolio;
    } catch (error) {
      console.error('Error generating simple portfolio:', error);
      throw error;
    }
  }
  
  /**
   * Get the asset allocation breakdown for a given risk profile
   */
  private getAllocationForRiskProfile(riskProfile: string) {
    switch (riskProfile) {
      case 'Conservative':
        return {
          equityLargeCap: 15,
          equityMidCap: 5,
          equitySmallCap: 0,
          debtShortTerm: 35,
          debtMediumTerm: 40,
          hybrid: 5
        };
        
      case 'Moderately Conservative':
        return {
          equityLargeCap: 25,
          equityMidCap: 10,
          equitySmallCap: 5,
          debtShortTerm: 25,
          debtMediumTerm: 30,
          hybrid: 5
        };
        
      case 'Balanced':
        return {
          equityLargeCap: 30,
          equityMidCap: 15,
          equitySmallCap: 5,
          debtShortTerm: 20,
          debtMediumTerm: 20,
          hybrid: 10
        };
        
      case 'Moderately Aggressive':
        return {
          equityLargeCap: 40,
          equityMidCap: 20,
          equitySmallCap: 10,
          debtShortTerm: 10,
          debtMediumTerm: 15,
          hybrid: 5
        };
        
      case 'Aggressive':
        return {
          equityLargeCap: 45,
          equityMidCap: 25,
          equitySmallCap: 15,
          debtShortTerm: 5,
          debtMediumTerm: 5,
          hybrid: 5
        };
        
      default:
        return {
          equityLargeCap: 30,
          equityMidCap: 15,
          equitySmallCap: 5,
          debtShortTerm: 20,
          debtMediumTerm: 20,
          hybrid: 10
        };
    }
  }
  
  /**
   * Get expected returns range for a risk profile
   */
  private getExpectedReturnsForRiskProfile(riskProfile: string) {
    switch (riskProfile) {
      case 'Conservative':
        return { min: 6, max: 8 };
      case 'Moderately Conservative':
        return { min: 8, max: 10 };
      case 'Balanced':
        return { min: 10, max: 12 };
      case 'Moderately Aggressive':
        return { min: 12, max: 14 };
      case 'Aggressive':
        return { min: 14, max: 16 };
      default:
        return { min: 10, max: 12 };
    }
  }
}

export const simplePortfolioService = new SimplePortfolioService();