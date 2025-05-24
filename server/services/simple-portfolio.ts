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
      
      // Fetch funds with their quartile ratings, prioritizing Q1 and Q2 funds (top performers)
      // This implements the Spark Capital methodology for fund selection
      // Q1: Top 25% - BUY recommendation
      // Q2: 26-50% - HOLD recommendation
      // Q3: 51-75% - REVIEW recommendation
      // Q4: Bottom 25% - SELL recommendation
      const scoredFunds = await pool.query(`
        SELECT f.id, f.scheme_code, f.fund_name, f.amc_name, f.category, f.subcategory,
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
        ORDER BY 
          CASE 
            WHEN fs.quartile = 1 THEN 1  -- First prioritize Q1 funds (top 25%)
            WHEN fs.quartile = 2 THEN 2  -- Then Q2 funds (26-50%)
            WHEN fs.quartile = 3 THEN 3  -- Then Q3 funds (51-75%)
            WHEN fs.quartile = 4 THEN 4  -- Then Q4 funds (bottom 25%)
            ELSE 5                       -- Unrated funds last
          END,
          fs.total_score DESC NULLS LAST
        LIMIT 50
      `, [latestScoreDate]);
      
      console.log(`Found ${scoredFunds.rows.length} funds with quartile ratings for portfolio`);
      
      // If we have less than 10 scored funds, get additional funds to ensure we have enough
      let topFunds = scoredFunds.rows;
      if (topFunds.length < 10) {
        const additionalFunds = await pool.query(`
          SELECT id, scheme_code, fund_name, amc_name, category, subcategory 
          FROM funds 
          WHERE fund_name IS NOT NULL AND amc_name IS NOT NULL
          AND id NOT IN (SELECT fund_id FROM fund_scores WHERE score_date = $1)
          ORDER BY id
          LIMIT $2
        `, [latestScoreDate, 20 - topFunds.length]);
        
        topFunds = [...topFunds, ...additionalFunds.rows];
      }
      
      // Group funds by category AND quartile for better selection
      // For each category, we'll prioritize Q1 and Q2 funds
      
      // Create categorized fund lists with quartile prioritization
      const equityLargeCapFunds = topFunds
        .filter(fund => (fund.category?.includes('Large') || (fund.category?.includes('Equity') && fund.subcategory?.includes('Large'))))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      const equityMidCapFunds = topFunds
        .filter(fund => (fund.category?.includes('Mid') || fund.subcategory?.includes('Mid')))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      const equitySmallCapFunds = topFunds
        .filter(fund => (fund.category?.includes('Small') || fund.subcategory?.includes('Small')))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      const debtShortTermFunds = topFunds
        .filter(fund => (fund.category?.includes('Debt') && (fund.subcategory?.includes('Short') || fund.fund_name?.includes('Short'))))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      const debtMediumTermFunds = topFunds
        .filter(fund => (fund.category?.includes('Debt') && (fund.subcategory?.includes('Medium') || fund.fund_name?.includes('Medium') || fund.fund_name?.includes('Corporate'))))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      const hybridFunds = topFunds
        .filter(fund => (fund.category?.includes('Hybrid') || fund.category?.includes('Balanced')))
        .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
        .slice(0, 2);
      
      // General funds backup in case we didn't find specific categories
      // Sort by quartile to ensure we're using the best-rated funds
      const generalFunds = topFunds.sort((a, b) => (a.quartile || 5) - (b.quartile || 5));
      
      // Filter out duplicates and prioritize funds with quartile ratings
      const getUniqueFunds = (funds: any[], count: number) => {
        // First get funds with quartile ratings (Q1 and Q2 preferred)
        const ratedFunds = funds
          .filter(fund => fund.quartile && fund.quartile <= 2) // Only Q1 and Q2 funds
          .sort((a, b) => (a.quartile || 5) - (b.quartile || 5))
          .slice(0, count);
          
        // If we still need more funds, try Q3 funds
        let remainingCount = count - ratedFunds.length;
        let result = [...ratedFunds];
        
        if (remainingCount > 0) {
          const q3Funds = funds
            .filter(fund => fund.quartile === 3)
            .slice(0, remainingCount);
          
          result = [...result, ...q3Funds];
          remainingCount -= q3Funds.length;
        }
        
        // Only if we absolutely need more, include Q4 funds
        if (remainingCount > 0) {
          const q4Funds = funds
            .filter(fund => fund.quartile === 4)
            .slice(0, remainingCount);
          
          result = [...result, ...q4Funds];
          remainingCount -= q4Funds.length;
        }
        
        // As a last resort, get unrated funds
        if (remainingCount > 0) {
          const unratedFunds = funds
            .filter(fund => !fund.quartile)
            .slice(0, remainingCount);
          
          result = [...result, ...unratedFunds];
        }
        
        return result;
      };
      
      // Create unique ID sets to track all selected funds and avoid duplication
      const selectedFundIds = new Set<number>();
      
      // Get unique funds for each category, avoiding duplicates across categories
      // And strictly enforcing quartile-based recommendations
      const getFundsForCategory = (funds: any[], count: number) => {
        const result = [];
        
        // First pass: Get only Q1 and Q2 funds (BUY/HOLD recommendations)
        for (const fund of funds) {
          if (result.length >= count) break;
          
          // Only use a fund if it hasn't been selected before
          if (!selectedFundIds.has(fund.id) && (fund.quartile === 1 || fund.quartile === 2)) {
            selectedFundIds.add(fund.id);
            result.push(fund);
          }
        }
        
        // If we still need more funds and can't find enough Q1/Q2, only then use Q3 (REVIEW)
        if (result.length < count) {
          for (const fund of funds) {
            if (result.length >= count) break;
            
            if (!selectedFundIds.has(fund.id) && fund.quartile === 3) {
              selectedFundIds.add(fund.id);
              result.push(fund);
            }
          }
        }
        
        // As a last resort, include Q4 (SELL) or unrated funds if we absolutely must
        if (result.length < count) {
          for (const fund of funds) {
            if (result.length >= count) break;
            
            if (!selectedFundIds.has(fund.id) && (fund.quartile === 4 || !fund.quartile)) {
              selectedFundIds.add(fund.id);
              result.push(fund);
            }
          }
        }
        
        return result;
      };
      
      // Combine all funds into categories and ensure we have enough funds
      const largeCaps = getFundsForCategory(
        equityLargeCapFunds.length > 0 ? equityLargeCapFunds : generalFunds.filter(f => f.category?.includes('Equity')),
        2
      );
      
      const midCaps = getFundsForCategory(
        equityMidCapFunds.length > 0 ? equityMidCapFunds : generalFunds.filter(f => f.category?.includes('Equity')),
        2
      );
      
      const smallCaps = getFundsForCategory(
        equitySmallCapFunds.length > 0 ? equitySmallCapFunds : generalFunds.filter(f => f.category?.includes('Equity')),
        2
      );
      
      const shortTerms = getFundsForCategory(
        debtShortTermFunds.length > 0 ? debtShortTermFunds : generalFunds.filter(f => f.category?.includes('Debt')),
        2
      );
      
      const mediumTerms = getFundsForCategory(
        debtMediumTermFunds.length > 0 ? debtMediumTermFunds : generalFunds.filter(f => f.category?.includes('Debt')),
        2
      );
      
      const hybrids = getFundsForCategory(
        hybridFunds.length > 0 ? hybridFunds : generalFunds.filter(f => f.category?.includes('Hybrid')),
        2
      );
      
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