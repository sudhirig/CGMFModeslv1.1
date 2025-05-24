import { db, pool } from '../db';
import { eq, and, or, like } from 'drizzle-orm';
import { funds } from '../../shared/schema';

/**
 * A completely revised portfolio service that guarantees no duplicate funds
 */
export class RevisedPortfolioService {
  /**
   * Generate a model portfolio with real fund allocations and no duplicates
   */
  async generatePortfolio(riskProfile: string) {
    try {
      console.log(`Generating ${riskProfile} portfolio with real fund allocations (REVISED)`);
      
      // Get asset allocation based on risk profile
      const assetAllocation = this.getAllocationForRiskProfile(riskProfile);
      
      // Get expected returns for risk profile
      const expectedReturns = this.getExpectedReturnsForRiskProfile(riskProfile);
      
      // Get latest date for fund scores
      const scoreDate = await pool.query(`
        SELECT MAX(score_date) as latest_date FROM fund_scores
      `);
      
      const latestScoreDate = scoreDate.rows[0]?.latest_date || new Date().toISOString().split('T')[0];
      
      // ======= NEW IMPLEMENTATION: COMPLETELY DIFFERENT APPROACH TO FUND SELECTION =======
      // Instead of selecting funds by category in separate queries, we'll use a single query
      // with partitioning to ensure we get unique funds across all categories
      
      console.log("Using new fund selection approach to prevent duplicates");
      
      // Single query to get ALL funds with quartile ratings, ensuring uniqueness by fund name
      // IMPORTANT: Exclude Q4 funds entirely as they are SELL rated
      const allFundsQuery = await pool.query(`
        WITH ranked_funds AS (
          SELECT 
            f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
            fs.quartile, fs.total_score,
            CASE 
              WHEN fs.quartile = 1 THEN 'BUY'
              WHEN fs.quartile = 2 THEN 'HOLD'
              WHEN fs.quartile = 3 THEN 'REVIEW'
              WHEN fs.quartile = 4 THEN 'SELL'
              ELSE NULL
            END as recommendation,
            -- Add a category marker to help with allocation
            CASE
              WHEN f.category LIKE '%Equity%' OR f.category LIKE '%Growth%' OR f.category = 'ELSS' THEN 'Equity'
              WHEN f.category LIKE '%Debt%' OR f.category LIKE '%Income%' OR f.category LIKE '%Liquid%' THEN 'Debt'
              WHEN f.category LIKE '%Hybrid%' OR f.category LIKE '%Balanced%' THEN 'Hybrid'
              ELSE 'Other'
            END as asset_class,
            -- Add sub-category markers for allocation within equity
            CASE
              WHEN f.subcategory LIKE '%Large%' OR f.fund_name LIKE '%Large%' THEN 'Large Cap'
              WHEN f.subcategory LIKE '%Mid%' OR f.fund_name LIKE '%Mid%' THEN 'Mid Cap'
              WHEN f.subcategory LIKE '%Small%' OR f.fund_name LIKE '%Small%' THEN 'Small Cap'
              ELSE 'Other'
            END as equity_subcategory,
            -- Add row number partitioned by fund_name to ensure no duplicates
            ROW_NUMBER() OVER (PARTITION BY f.fund_name ORDER BY 
              CASE 
                WHEN fs.quartile = 1 THEN 1  -- Prioritize Q1 funds
                WHEN fs.quartile = 2 THEN 2  -- Then Q2 funds
                WHEN fs.quartile = 3 THEN 3  -- Then Q3 funds
                ELSE 4                      -- Unrated funds last
              END,
              fs.total_score DESC NULLS LAST  -- Higher scores first within quartile
            ) as unique_rank
          FROM funds f
          LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
          WHERE 
            f.fund_name IS NOT NULL AND 
            f.amc_name IS NOT NULL AND
            fs.quartile IS NOT NULL AND  -- Only include rated funds
            fs.quartile != 4             -- Exclude Q4 (SELL rated) funds entirely
        )
        SELECT * FROM ranked_funds
        WHERE unique_rank = 1  -- This ensures each fund name appears only once
        ORDER BY 
          quartile ASC NULLS LAST,  -- First prioritize by quartile (Q1 first)
          total_score DESC NULLS LAST  -- Then by score within quartile
      `, [latestScoreDate]);
      
      console.log(`Found ${allFundsQuery.rows.length} funds with quartile ratings for portfolio`);
      
      // Organize funds by asset class and quartile for selection
      const fundsByCategory = {
        Equity: {
          'Large Cap': { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] },
          'Mid Cap': { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] },
          'Small Cap': { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] },
          'Other': { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] }
        },
        Debt: { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] },
        Hybrid: { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] },
        Other: { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] }
      };
      
      // Categorize all funds
      allFundsQuery.rows.forEach(fund => {
        const assetClass = fund.asset_class || 'Other';
        let quartile = 'Unrated';
        
        if (fund.quartile === 1) quartile = 'Q1';
        else if (fund.quartile === 2) quartile = 'Q2';
        else if (fund.quartile === 3) quartile = 'Q3';
        else if (fund.quartile === 4) quartile = 'Q4';
        
        // Handle Equity funds with subcategories
        if (assetClass === 'Equity') {
          const subCategory = fund.equity_subcategory || 'Other';
          if (!fundsByCategory.Equity[subCategory]) {
            fundsByCategory.Equity[subCategory] = { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] };
          }
          fundsByCategory.Equity[subCategory][quartile].push(fund);
        } else {
          // Handle other asset classes
          if (!fundsByCategory[assetClass]) {
            fundsByCategory[assetClass] = { Q1: [], Q2: [], Q3: [], Q4: [], Unrated: [] };
          }
          fundsByCategory[assetClass][quartile].push(fund);
        }
      });
      
      // Generate portfolio allocations based on risk profile and fund quartiles
      const allocations = [];
      const selectedFundNames = new Set(); // Additional check to prevent any duplicates
      
      // Function to select funds with priority to higher quartiles
      const selectFundsFromCategory = (category, count, allocPerFund) => {
        const selected = [];
        // Prioritize Q1 and Q2 funds first, then Q3, and only use Q4 and Unrated as last resort
        const quartileOrder = ['Q1', 'Q2', 'Q3', 'Q4', 'Unrated'];
        
        // First try to get funds ONLY from Q1 and Q2
        for (const quartile of ['Q1', 'Q2']) {
          if (selected.length >= count) break;
          
          const availableFunds = category[quartile].filter(fund => 
            !selectedFundNames.has(fund.fund_name)
          );
          
          // Take as many as needed from this quartile
          const neededFromQuartile = Math.min(
            count - selected.length, 
            availableFunds.length
          );
          
          for (let i = 0; i < neededFromQuartile; i++) {
            const fund = availableFunds[i];
            selected.push(fund);
            selectedFundNames.add(fund.fund_name);
          }
        }
        
        // If we still need more funds, then try Q3
        if (selected.length < count) {
          const availableQ3Funds = category['Q3'].filter(fund => 
            !selectedFundNames.has(fund.fund_name)
          );
          
          const neededFromQ3 = Math.min(
            count - selected.length, 
            availableQ3Funds.length
          );
          
          for (let i = 0; i < neededFromQ3; i++) {
            const fund = availableQ3Funds[i];
            selected.push(fund);
            selectedFundNames.add(fund.fund_name);
          }
        }
        
        // NEVER use Q4 funds in recommendations - they're "SELL" rated
        // NEVER use unrated funds in recommendations - we only want funds with proper ratings
        // If we don't have enough rated funds, we'll just return fewer funds rather than including unrated ones
        
        // If we still don't have enough funds, we should find similar funds from other categories
        // but never use Q4 rated funds
        
        // Convert to allocation objects - using formatted allocation percentages
        return selected.map(fund => ({
          fund,
          allocationPercent: this.formatAllocationPercentage(allocPerFund),
          expectedReturn: this.getExpectedReturnByQuartile(fund.quartile || 'Unrated')
        }));
      };
      
      // Allocate LARGE CAP EQUITY
      if (assetAllocation.largeCapEquity > 0) {
        const largeCapAlloc = selectFundsFromCategory(
          fundsByCategory.Equity['Large Cap'], 
          2,  // Number of funds to select
          assetAllocation.largeCapEquity / 2  // Split allocation equally
        );
        allocations.push(...largeCapAlloc);
      }
      
      // Allocate MID CAP EQUITY
      if (assetAllocation.midCapEquity > 0) {
        const midCapAlloc = selectFundsFromCategory(
          fundsByCategory.Equity['Mid Cap'], 
          1,  // Number of funds to select
          assetAllocation.midCapEquity  // Full allocation to one fund
        );
        allocations.push(...midCapAlloc);
      }
      
      // Allocate SMALL CAP EQUITY
      if (assetAllocation.smallCapEquity > 0) {
        const smallCapAlloc = selectFundsFromCategory(
          fundsByCategory.Equity['Small Cap'], 
          2,  // Number of funds to select
          assetAllocation.smallCapEquity / 2  // Split allocation equally
        );
        allocations.push(...smallCapAlloc);
      }
      
      // Allocate DEBT
      if (assetAllocation.debt > 0) {
        const debtAlloc = selectFundsFromCategory(
          fundsByCategory.Debt, 
          3,  // Number of funds to select
          assetAllocation.debt / 3  // Split allocation equally
        );
        allocations.push(...debtAlloc);
      }
      
      // Allocate HYBRID
      if (assetAllocation.hybrid > 0) {
        const hybridAlloc = selectFundsFromCategory(
          fundsByCategory.Hybrid, 
          2,  // Number of funds to select
          assetAllocation.hybrid / 2  // Split allocation equally
        );
        allocations.push(...hybridAlloc);
      }
      
      // Calculate overall expected return based on actual allocations
      let portfolioExpectedReturn = 0;
      allocations.forEach(allocation => {
        portfolioExpectedReturn += (allocation.allocationPercent / 100) * allocation.expectedReturn;
      });
      
      // Create the portfolio object
      const portfolio = {
        id: Date.now(), // Using timestamp as simple ID
        name: `${riskProfile} Portfolio`,
        riskProfile,
        description: `A ${riskProfile.toLowerCase()} risk portfolio with real funds and no duplicates`,
        expectedReturn: portfolioExpectedReturn.toFixed(2),
        allocations,
        createdAt: new Date()
      };
      
      // Final validation to ensure no duplicate funds
      const fundNames = new Set();
      const uniqueAllocations = portfolio.allocations.filter(allocation => {
        if (!allocation.fund || !allocation.fund.fund_name) return true;
        
        if (fundNames.has(allocation.fund.fund_name)) {
          console.log(`Removing duplicate fund in final check: ${allocation.fund.fund_name}`);
          return false;
        }
        
        fundNames.add(allocation.fund.fund_name);
        return true;
      });
      
      // Log the deduplicated result
      console.log(`Final portfolio has ${uniqueAllocations.length} unique funds`);
      
      // Set the deduplicated allocations
      portfolio.allocations = uniqueAllocations;
      
      return portfolio;
    } catch (error) {
      console.error('Error generating portfolio:', error);
      throw error;
    }
  }
  
  /**
   * Get asset allocation based on risk profile
   */
  getAllocationForRiskProfile(riskProfile: string) {
    // Allocations as percentages (0-100)
    switch (riskProfile.toLowerCase()) {
      case 'aggressive':
        return {
          largeCapEquity: 30,
          midCapEquity: 15,
          smallCapEquity: 10,
          debt: 30,
          hybrid: 15
        };
      case 'balanced':
        return {
          largeCapEquity: 30,
          midCapEquity: 8,
          smallCapEquity: 6,
          debt: 40,
          hybrid: 16
        };
      case 'conservative':
        return {
          largeCapEquity: 15,
          midCapEquity: 5,
          smallCapEquity: 0,
          debt: 70,
          hybrid: 10
        };
      default:
        return {
          largeCapEquity: 25,
          midCapEquity: 10,
          smallCapEquity: 5,
          debt: 45,
          hybrid: 15
        };
    }
  }
  
  /**
   * Get expected returns for risk profile
   */
  getExpectedReturnsForRiskProfile(riskProfile: string) {
    switch (riskProfile.toLowerCase()) {
      case 'aggressive':
        return 13.5;
      case 'balanced':
        return 11.0;
      case 'conservative':
        return 8.5;
      default:
        return 10.0;
    }
  }
  
  /**
   * Get expected return by quartile
   */
  getExpectedReturnByQuartile(quartile: number | string) {
    if (quartile === 'Unrated' || quartile === null || quartile === undefined) {
      return 10.0; // Default for unrated funds
    }
    
    switch (Number(quartile)) {
      case 1: return 14.0;  // Q1 funds expected to outperform
      case 2: return 12.0;  // Q2 funds slight outperformance
      case 3: return 10.0;  // Q3 funds average performance
      case 4: return 8.0;   // Q4 funds underperformance
      default: return 10.0; // Default for unrated funds
    }
  }
  
  /**
   * Format allocation percentage to clean display value
   * Returns a number instead of a string to match the expected format in the UI
   */
  formatAllocationPercentage(percentage: number): number {
    return Math.round(percentage);
  }
}

export const revisedPortfolioService = new RevisedPortfolioService();