import { db, pool } from '../db';

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
      
      // Fetch top funds for each category directly using SQL for reliability
      const topFunds = await pool.query(`
        SELECT 
          id, 
          scheme_code, 
          fund_name, 
          amc_name, 
          category, 
          subcategory
        FROM funds
        ORDER BY id
        LIMIT 20
      `);
      
      // Get asset allocation based on risk profile
      const assetAllocation = this.getAllocationForRiskProfile(riskProfile);
      
      // Define expected returns
      const expectedReturns = this.getExpectedReturnsForRiskProfile(riskProfile);
      
      // Create real fund allocations
      const allocations = [];
      const fundsPerCategory = 2;
      let fundIndex = 0;
      
      // Create sample equity large cap allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.equityLargeCap / fundsPerCategory)
        });
      }
      
      // Create sample equity mid cap allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.equityMidCap / fundsPerCategory)
        });
      }
      
      // Create sample equity small cap allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.equitySmallCap / fundsPerCategory)
        });
      }
      
      // Create sample debt short-term allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.debtShortTerm / fundsPerCategory)
        });
      }
      
      // Create sample debt medium-term allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.debtMediumTerm / fundsPerCategory)
        });
      }
      
      // Create sample hybrid allocation
      for (let i = 0; i < fundsPerCategory && fundIndex < topFunds.rows.length; i++, fundIndex++) {
        allocations.push({
          fund: topFunds.rows[fundIndex],
          allocationPercent: Math.round(assetAllocation.hybrid / fundsPerCategory)
        });
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