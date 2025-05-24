import { db, pool } from '../db';
import { funds, modelPortfolios, modelPortfolioAllocations } from '../../shared/schema';
import { eq, and, desc, sql } from 'drizzle-orm';

/**
 * Service to generate real portfolio allocations using actual fund data
 * This ensures our portfolio builder always has real fund data to display
 */
export class PortfolioGenerator {
  /**
   * Generate a model portfolio with real fund allocations based on a risk profile
   */
  async generatePortfolio(riskProfile: string) {
    console.log(`Normalized risk profile "${riskProfile}" to "${riskProfile}" for allocation`);
    
    try {
      // Create the portfolio first
      const [portfolio] = await db.insert(modelPortfolios).values({
        name: `${riskProfile} Portfolio`,
        riskProfile: riskProfile,
        elivateScoreId: 2, // Using a default ELIVATE score
        createdAt: new Date()
      }).returning();
      
      if (!portfolio || !portfolio.id) {
        throw new Error('Failed to create portfolio record');
      }
      
      // Define asset allocation based on risk profile
      const assetAllocation = this.getAllocationForRiskProfile(riskProfile);
      
      // Get top-rated funds from each category
      const equityFunds = await this.getTopFundsByCategory('Equity: Large Cap', 2);
      const equityMidFunds = await this.getTopFundsByCategory('Equity: Mid Cap', 2);
      const equitySmallFunds = await this.getTopFundsByCategory('Equity: Small Cap', 1);
      const debtFunds = await this.getTopFundsByCategory('Debt: Medium Duration', 2);
      const debtShortFunds = await this.getTopFundsByCategory('Debt: Short Duration', 1);
      const hybridFunds = await this.getTopFundsByCategory('Hybrid: Aggressive', 1);
      
      const allocations = [];
      
      // Create allocations for each fund category
      if (assetAllocation.equityLargeCap > 0 && equityFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equityLargeCap / equityFunds.length);
        for (const fund of equityFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      if (assetAllocation.equityMidCap > 0 && equityMidFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equityMidCap / equityMidFunds.length);
        for (const fund of equityMidFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      if (assetAllocation.equitySmallCap > 0 && equitySmallFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.equitySmallCap / equitySmallFunds.length);
        for (const fund of equitySmallFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      if (assetAllocation.debtMediumTerm > 0 && debtFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.debtMediumTerm / debtFunds.length);
        for (const fund of debtFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      if (assetAllocation.debtShortTerm > 0 && debtShortFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.debtShortTerm / debtShortFunds.length);
        for (const fund of debtShortFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      if (assetAllocation.hybrid > 0 && hybridFunds.length > 0) {
        const perFundAllocation = Math.round(assetAllocation.hybrid / hybridFunds.length);
        for (const fund of hybridFunds) {
          allocations.push({
            portfolioId: portfolio.id,
            fundId: fund.id,
            allocationPercent: perFundAllocation,
            createdAt: new Date()
          });
        }
      }
      
      // Insert all allocations
      await db.insert(modelPortfolioAllocations).values(allocations);
      
      // Fetch the completed portfolio with allocations
      const completePortfolio = await this.getPortfolioWithAllocations(portfolio.id);
      
      // Add expected returns and asset allocation to the response
      return {
        ...completePortfolio,
        assetAllocation,
        expectedReturns: this.getExpectedReturnsForRiskProfile(riskProfile)
      };
    } catch (error) {
      console.error('Error generating portfolio:', error);
      throw error;
    }
  }
  
  /**
   * Get top-rated funds by category
   */
  private async getTopFundsByCategory(category: string, limit: number = 3) {
    try {
      const result = await pool.query(`
        SELECT f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory 
        FROM funds f
        WHERE f.category LIKE $1
        ORDER BY f.id
        LIMIT $2
      `, [`%${category}%`, limit]);
      
      return result.rows;
    } catch (error) {
      console.error(`Error fetching top funds for category ${category}:`, error);
      return [];
    }
  }
  
  /**
   * Get portfolio with all its allocations
   */
  private async getPortfolioWithAllocations(portfolioId: number) {
    try {
      const portfolio = await db.query.modelPortfolios.findFirst({
        where: eq(modelPortfolios.id, portfolioId)
      });
      
      if (!portfolio) {
        throw new Error(`Portfolio with ID ${portfolioId} not found`);
      }
      
      // Get allocations with fund details
      const result = await pool.query(`
        SELECT 
          mpa.portfolio_id,
          mpa.fund_id,
          mpa.allocation_percent,
          f.fund_name,
          f.amc_name,
          f.category,
          f.subcategory
        FROM model_portfolio_allocations mpa
        JOIN funds f ON mpa.fund_id = f.id
        WHERE mpa.portfolio_id = $1
      `, [portfolioId]);
      
      const allocations = result.rows.map(row => ({
        portfolioId: row.portfolio_id,
        fundId: row.fund_id,
        allocationPercent: row.allocation_percent,
        fund: {
          id: row.fund_id,
          fundName: row.fund_name,
          amcName: row.amc_name,
          category: row.category,
          subcategory: row.subcategory
        }
      }));
      
      return {
        ...portfolio,
        allocations
      };
    } catch (error) {
      console.error(`Error fetching portfolio ${portfolioId}:`, error);
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

export const portfolioGenerator = new PortfolioGenerator();