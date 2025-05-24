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
      
      // We'll initialize with a baseline, but will update this
      // after fund allocation with actual calculations
      let expectedReturns = this.getExpectedReturnsForRiskProfile(riskProfile);
      
      // Get latest date for fund scores
      const scoreDate = await pool.query(`
        SELECT MAX(score_date) as latest_date FROM fund_scores
      `);
      
      const latestScoreDate = scoreDate.rows[0]?.latest_date || new Date().toISOString().split('T')[0];
      
      // Fetch ALL funds with their quartile ratings using a MUCH more selective approach
      // This implements the Spark Capital methodology for fund selection
      // Q1: Top 25% - BUY recommendation
      // Q2: 26-50% - HOLD recommendation
      // Q3: 51-75% - REVIEW recommendation
      // Q4: Bottom 25% - SELL recommendation
      
      // STEP 1: Create a completely fresh approach to fund selection
      // Fetch funds by category with quartile ratings and proper deduplication
      
      // First, initialize an array to track all selected fund names for 100% duplicate prevention
      const allSelectedFundNames = new Set<string>();
      
      // First, get LARGE CAP EQUITY funds
      const largeCapQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          (f.category LIKE '%Equity%' AND (f.subcategory LIKE '%Large Cap%' OR f.fund_name LIKE '%Large Cap%' OR f.fund_name LIKE '%Bluechip%'))
          OR f.category LIKE '%Large Cap%'
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1  -- First prioritize Q1 funds (top 25%)
            WHEN fs.quartile = 2 THEN 2  -- Then Q2 funds (26-50%)
            WHEN fs.quartile = 3 THEN 3  -- Then Q3 funds (51-75%)
            WHEN fs.quartile = 4 THEN 4  -- Then Q4 funds (bottom 25%)
            ELSE 5                       -- Unrated funds last
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // Next, get MID CAP EQUITY funds
      const midCapQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          (f.category LIKE '%Equity%' AND (f.subcategory LIKE '%Mid Cap%' OR f.fund_name LIKE '%Mid Cap%'))
          OR f.category LIKE '%Mid Cap%'
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1
            WHEN fs.quartile = 2 THEN 2
            WHEN fs.quartile = 3 THEN 3
            WHEN fs.quartile = 4 THEN 4
            ELSE 5
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // Get SMALL CAP EQUITY funds
      const smallCapQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          (f.category LIKE '%Equity%' AND (f.subcategory LIKE '%Small Cap%' OR f.fund_name LIKE '%Small Cap%'))
          OR f.category LIKE '%Small Cap%'
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1
            WHEN fs.quartile = 2 THEN 2
            WHEN fs.quartile = 3 THEN 3
            WHEN fs.quartile = 4 THEN 4
            ELSE 5
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // Get SHORT TERM DEBT funds
      const shortTermDebtQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          (f.category LIKE '%Debt%' AND (f.subcategory LIKE '%Short%' OR f.fund_name LIKE '%Short%'))
          OR (f.fund_name LIKE '%Liquid%' OR f.subcategory LIKE '%Liquid%')
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1
            WHEN fs.quartile = 2 THEN 2
            WHEN fs.quartile = 3 THEN 3
            WHEN fs.quartile = 4 THEN 4
            ELSE 5
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // Get MEDIUM TERM DEBT funds
      const mediumTermDebtQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          (f.category LIKE '%Debt%' AND (
            f.subcategory LIKE '%Medium%' OR 
            f.fund_name LIKE '%Medium%' OR
            f.fund_name LIKE '%Corporate Bond%' OR
            f.subcategory LIKE '%Corporate Bond%'
          ))
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1
            WHEN fs.quartile = 2 THEN 2
            WHEN fs.quartile = 3 THEN 3
            WHEN fs.quartile = 4 THEN 4
            ELSE 5
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // Get HYBRID funds
      const hybridQuery = await pool.query(`
        SELECT DISTINCT ON (f.fund_name, f.amc_name) 
          f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
          fs.quartile, fs.total_score, 
          CASE 
            WHEN fs.quartile = 1 THEN 'BUY'
            WHEN fs.quartile = 2 THEN 'HOLD'
            WHEN fs.quartile = 3 THEN 'REVIEW'
            WHEN fs.quartile = 4 THEN 'SELL'
            ELSE NULL
          END as recommendation
        FROM funds f
        LEFT JOIN fund_scores fs ON f.id = fs.fund_id AND fs.score_date = $1
        WHERE f.fund_name IS NOT NULL AND f.amc_name IS NOT NULL
        AND (
          f.category LIKE '%Hybrid%' OR 
          f.category LIKE '%Balanced%' OR
          f.fund_name LIKE '%Balanced%' OR
          f.fund_name LIKE '%Hybrid%'
        )
        ORDER BY f.fund_name, f.amc_name,
          CASE 
            WHEN fs.quartile = 1 THEN 1
            WHEN fs.quartile = 2 THEN 2
            WHEN fs.quartile = 3 THEN 3
            WHEN fs.quartile = 4 THEN 4
            ELSE 5
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 10
      `, [latestScoreDate]);
      
      // STEP 2: Track a master set of selected fund IDs to absolutely ensure no duplicates
      const selectedFundIds = new Set<number>();
      
      // Use an additional Set to track specific fund name + AMC combinations
      // This adds an extra layer of deduplication beyond just IDs
      const selectedFundNameAMCPairs = new Set<string>();
      
      // STEP 3: Create a completely redesigned function for selecting the best funds
      // This version is designed with absolute deduplication as the top priority
      const getBestFundsFromCategory = (funds: any[], count: number) => {
        // Create a local copy to avoid modifying the original array
        const availableFunds = [...funds].filter(fund => {
          // Only include funds that haven't been selected before
          // This is the key change - check the fund name directly
          if (!fund.fund_name) return false; // Skip if no name
          return !allSelectedFundNames.has(fund.fund_name);
        });
        
        console.log(`Found ${availableFunds.length} unique funds after filtering duplicates`);
        
        const result = [];
        const quartiles = [1, 2, 3, 4, null]; // Process in order of quartile ranking
        
        // Try to fill our selection with the best quartile funds first
        for (const quartile of quartiles) {
          if (result.length >= count) break;
          
          // Find funds of the current quartile that haven't been selected yet
          const quartiledFunds = availableFunds.filter(fund => 
            fund.quartile === quartile && !allSelectedFundNames.has(fund.fund_name)
          );
          
          // Sort by total score (if available) descending
          quartiledFunds.sort((a, b) => {
            if (!a.total_score) return 1;
            if (!b.total_score) return -1;
            return b.total_score - a.total_score;
          });
          
          // Add funds until we reach the count
          for (const fund of quartiledFunds) {
            if (result.length >= count) break;
            
            // Double-check to make sure we're not adding a duplicate
            if (!allSelectedFundNames.has(fund.fund_name)) {
              // Add to tracking structures
              allSelectedFundNames.add(fund.fund_name);
              selectedFundIds.add(fund.id);
              selectedFundNameAMCPairs.add(`${fund.fund_name}|${fund.amc_name}`);
              
              // Add to result
              result.push(fund);
              console.log(`Selected fund: ${fund.fund_name}`);
            }
          }
        }
        
        return result;
      };
      
      // STEP 4: Select the best funds for each category, being absolutely sure 
      // there's no overlap of fund names across categories
      
      // First, create a filtering function that removes any funds already selected
      // across ALL categories using our global tracking Set
      const filterDuplicateFunds = (funds: any[]) => {
        return funds.filter(fund => {
          const fundName = fund.fund_name;
          return !allSelectedFundNames.has(fundName);
        });
      };
      
      // Process large cap funds first
      const filteredLargeCaps = filterDuplicateFunds(largeCapQuery.rows);
      const largeCaps = getBestFundsFromCategory(filteredLargeCaps, 2);
      // Add selected fund names to global tracking set
      largeCaps.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // Process mid cap funds next
      const filteredMidCaps = filterDuplicateFunds(midCapQuery.rows);
      const midCaps = getBestFundsFromCategory(filteredMidCaps, 2);
      midCaps.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // Process small cap funds
      const filteredSmallCaps = filterDuplicateFunds(smallCapQuery.rows);
      const smallCaps = getBestFundsFromCategory(filteredSmallCaps, 2);
      smallCaps.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // Process short term debt funds
      const filteredShortTerms = filterDuplicateFunds(shortTermDebtQuery.rows);
      const shortTerms = getBestFundsFromCategory(filteredShortTerms, 2);
      shortTerms.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // Process medium term debt funds
      const filteredMediumTerms = filterDuplicateFunds(mediumTermDebtQuery.rows);
      const mediumTerms = getBestFundsFromCategory(filteredMediumTerms, 2);
      mediumTerms.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // Process hybrid funds
      const filteredHybrids = filterDuplicateFunds(hybridQuery.rows);
      const hybrids = getBestFundsFromCategory(filteredHybrids, 2);
      hybrids.forEach(fund => allSelectedFundNames.add(fund.fund_name));
      
      // STEP 5: Create allocations from selected funds
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
      
      // Update expected returns based on actual fund allocation and quartile data
      expectedReturns = this.calculateExpectedReturns(allocations, riskProfile);
      
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
            fundName: allocation.fund.fund_name, // Use the database field name
            amcName: allocation.fund.amc_name,   // Use the database field name
            category: allocation.fund.category || 'Mixed Asset',
            subcategory: allocation.fund.subcategory,
            quartile: allocation.fund.quartile,
            recommendation: allocation.fund.recommendation,
            totalScore: allocation.fund.total_score
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
   * Calculate expected returns based on actual fund quartile ratings and allocation
   */
  private calculateExpectedReturns(allocations: any[], riskProfile: string) {
    // Start with baseline expected returns
    let baselineReturns = {
      Conservative: { min: 6, max: 8 },
      'Moderately Conservative': { min: 8, max: 10 },
      Balanced: { min: 10, max: 12 },
      'Moderately Aggressive': { min: 12, max: 14 },
      Aggressive: { min: 14, max: 16 }
    };
    
    // Get baseline based on risk profile
    const baseline = baselineReturns[riskProfile as keyof typeof baselineReturns] || baselineReturns.Balanced;
    
    // Calculate weighted expected returns based on quartiles
    // Q1 (BUY) funds typically have higher expected returns, Q4 (SELL) funds lower
    let weightedSum = 0;
    let totalWeight = 0;
    
    // Count funds by quartile to adjust expected returns appropriately
    let q1Count = 0, q2Count = 0, q3Count = 0, q4Count = 0, unratedCount = 0;
    let totalAllocationPercentage = 0;
    
    // First, count the number of funds in each quartile category
    for (const allocation of allocations) {
      if (!allocation.fund) continue;
      
      totalAllocationPercentage += allocation.allocationPercent;
      
      if (allocation.fund.quartile === 1) {
        q1Count++;
      } else if (allocation.fund.quartile === 2) {
        q2Count++;
      } else if (allocation.fund.quartile === 3) {
        q3Count++;
      } else if (allocation.fund.quartile === 4) {
        q4Count++;
      } else {
        unratedCount++;
      }
    }
    
    // Calculate weighted contributions from each allocation
    for (const allocation of allocations) {
      if (!allocation.fund) continue;
      
      // Default to moderate performance if no quartile
      let expectedReturn = (baseline.min + baseline.max) / 2;
      
      // Adjust expected return based on quartile
      if (allocation.fund.quartile === 1) {
        // Q1 funds perform significantly above baseline (BUY recommendation)
        expectedReturn = baseline.max + 3;
      } else if (allocation.fund.quartile === 2) {
        // Q2 funds perform at high end of baseline (HOLD recommendation)
        expectedReturn = baseline.max + 1;
      } else if (allocation.fund.quartile === 3) {
        // Q3 funds perform at low end of baseline (REVIEW recommendation)
        expectedReturn = baseline.min;
      } else if (allocation.fund.quartile === 4) {
        // Q4 funds perform below baseline (SELL recommendation)
        expectedReturn = baseline.min - 2;
      }
      
      // Add weighted contribution
      weightedSum += expectedReturn * allocation.allocationPercent;
      totalWeight += allocation.allocationPercent;
    }
    
    // Calculate weighted average expected return
    const avgExpectedReturn = totalWeight > 0 ? weightedSum / totalWeight : (baseline.min + baseline.max) / 2;
    
    // Calculate the quality of the portfolio based on quartile distribution
    const totalRatedFunds = q1Count + q2Count + q3Count + q4Count;
    const highQualityRatio = totalRatedFunds > 0 ? (q1Count + q2Count) / totalRatedFunds : 0.5;
    
    // Adjust the range based on portfolio quality
    let minAdjustment = 0;
    let maxAdjustment = 0;
    
    if (highQualityRatio >= 0.8) {
      // Excellent portfolio (mostly Q1 and Q2 funds)
      minAdjustment = 1;
      maxAdjustment = 2;
    } else if (highQualityRatio >= 0.6) {
      // Good portfolio
      minAdjustment = 0.5;
      maxAdjustment = 1.5;
    } else if (highQualityRatio >= 0.4) {
      // Average portfolio
      minAdjustment = 0;
      maxAdjustment = 1;
    } else if (highQualityRatio >= 0.2) {
      // Below average portfolio
      minAdjustment = -0.5;
      maxAdjustment = 0.5;
    } else {
      // Poor quality portfolio (mostly Q3 and Q4 funds)
      minAdjustment = -1;
      maxAdjustment = 0;
    }
    
    // Create a range around the weighted average with quality adjustments
    return {
      min: Math.max(Math.round(avgExpectedReturn + minAdjustment), 1),
      max: Math.round(avgExpectedReturn + maxAdjustment)
    };
  }
  
  /**
   * Get expected returns range for a risk profile (legacy fallback)
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