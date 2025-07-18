import { db, pool } from '../db';
import { storage } from '../storage';

export class BacktestingEngine {
  private static instance: BacktestingEngine;
  
  private constructor() {}
  
  public static getInstance(): BacktestingEngine {
    if (!BacktestingEngine.instance) {
      BacktestingEngine.instance = new BacktestingEngine();
    }
    return BacktestingEngine.instance;
  }

  /**
   * Get appropriate benchmark for portfolio based on allocations
   * Returns null until we have authentic fund-benchmark mappings
   */
  private getPortfolioBenchmark(allocations: any[]): string {
    // Return NIFTY 50 as default until we have authentic benchmark mappings
    // In the future, this should analyze fund allocations and return
    // the most appropriate benchmark based on fund categories
    return 'NIFTY 50';
  }
  
  /**
   * Run a backtest on a model portfolio for a given time period
   * Using real historical NAV data to accurately calculate returns
   */
  async runBacktest(params: {
    portfolioId?: number;
    riskProfile?: string;
    startDate: Date;
    endDate: Date;
    initialAmount: number;
    rebalancePeriod?: 'monthly' | 'quarterly' | 'annually';
  }) {
    try {
      console.log(`Running backtest with params: ${JSON.stringify({
        portfolioId: params.portfolioId,
        riskProfile: params.riskProfile,
        startDate: params.startDate,
        endDate: params.endDate,
        initialAmount: params.initialAmount,
        rebalancePeriod: params.rebalancePeriod || 'quarterly'
      })}`);
      
      const { 
        portfolioId, 
        riskProfile, 
        startDate, 
        endDate, 
        initialAmount,
        rebalancePeriod = 'quarterly'
      } = params;
      
      // Validate input dates
      if (startDate > endDate) {
        throw new Error("Start date must be before end date");
      }
      
      // Get portfolio - either by ID or by risk profile
      let portfolio;
      // We'll use these allocations for backtesting if we can't find a specific portfolio
      const defaultAllocations = await this.getDefaultPortfolioAllocations();
      
      // Try to get the requested portfolio
      if (portfolioId) {
        console.log(`Attempting to fetch portfolio with ID: ${portfolioId}`);
        try {
          portfolio = await storage.getModelPortfolio(portfolioId);
        } catch (error) {
          console.log(`Error fetching portfolio with ID ${portfolioId}: ${error.message}`);
        }
      } else if (riskProfile) {
        console.log(`Looking for portfolio with risk profile: ${riskProfile}`);
        
        // Get latest portfolio for risk profile
        try {
          const portfolios = await pool.query(`
            SELECT * FROM model_portfolios 
            WHERE risk_profile = $1 
            ORDER BY created_at DESC 
            LIMIT 1
          `, [riskProfile]);
          
          if (portfolios.rows.length > 0) {
            const latestPortfolioId = parseInt(portfolios.rows[0].id);
            console.log(`Found portfolio ID ${latestPortfolioId} for risk profile ${riskProfile}`);
            
            if (!isNaN(latestPortfolioId)) {
              portfolio = await storage.getModelPortfolio(latestPortfolioId);
            }
          } else {
            console.log(`No existing portfolio found for risk profile: ${riskProfile}`);
          }
        } catch (error) {
          console.log(`Error fetching portfolio for risk profile ${riskProfile}: ${error.message}`);
        }
      } else {
        console.log("No portfolio ID or risk profile provided");
      }
      
      // Portfolio not found, create a fallback portfolio
      if (!portfolio) {
        console.log("Portfolio not found, creating a fallback portfolio");
        
        // Check if risk profile is one of the valid options
        const validRiskProfiles = ['Conservative', 'Moderately Conservative', 'Balanced', 'Moderately Aggressive', 'Aggressive'];
        
        // Map any variations to standard risk profiles
        let standardRiskProfile = "Balanced";
        if (riskProfile) {
          if (riskProfile === "Moderate") {
            standardRiskProfile = "Balanced";
          } else if (validRiskProfiles.includes(riskProfile)) {
            standardRiskProfile = riskProfile;
          } else if (riskProfile.includes("Conservative") && riskProfile.includes("Moderate")) {
            standardRiskProfile = "Moderately Conservative";
          } else if (riskProfile.includes("Aggressive") && riskProfile.includes("Moderate")) {
            standardRiskProfile = "Moderately Aggressive";
          } else if (riskProfile.includes("Conservative")) {
            standardRiskProfile = "Conservative";
          } else if (riskProfile.includes("Aggressive")) {
            standardRiskProfile = "Aggressive";
          }
        }
        
        console.log(`Mapped risk profile "${riskProfile}" to standard risk profile "${standardRiskProfile}"`);
        
        // Create a basic fallback portfolio
        portfolio = {
          id: 0,
          name: `${standardRiskProfile} Portfolio (Fallback)`,
          riskProfile: standardRiskProfile,
          allocations: defaultAllocations[standardRiskProfile] || []
        };
        
        console.log(`Created fallback portfolio with ${portfolio.allocations.length} allocations`);
      }
      
      // Get portfolio allocations
      let allocations = portfolio.allocations;
      
      // Final check: if no allocations, use defaults for this risk profile
      if (!allocations || allocations.length === 0) {
        console.log(`Portfolio ${portfolio.id} has no allocations, using default allocations`);
        
        const riskProfileToUse = portfolio.riskProfile || riskProfile || "Balanced";
        allocations = defaultAllocations[riskProfileToUse] || defaultAllocations["Balanced"];
        
        // Update the portfolio object to include these allocations
        portfolio.allocations = allocations;
        
        console.log(`Applied ${allocations.length} default allocations to portfolio`);
      }
      
      // Calculate rebalance dates
      const rebalanceDates = this.getRebalanceDates(startDate, endDate, rebalancePeriod);
      
      // Initialize metrics
      let currentValue = initialAmount;
      let highWaterMark = initialAmount;
      let maxDrawdown = 0;
      let returns: { date: Date; value: number }[] = [{ date: startDate, value: initialAmount }];
      
      // If we're using a fallback/default portfolio, log that information
      if (portfolio.id === 0) {
        console.log(`Using fallback portfolio for backtesting with ${allocations.length} fund allocations`);
      }
      
      const fundIds = allocations.map(allocation => allocation.fund.id);
      const navData = await this.getHistoricalNavData(fundIds, startDate, endDate);
      
      // Calculate weights
      const weights = allocations.reduce((acc, allocation) => {
        // Handle both allocation types (percentage or raw allocation)
        if (allocation.allocationPercent) {
          acc[allocation.fund.id] = parseFloat(allocation.allocationPercent) / 100;
        } else if (allocation.allocation) {
          acc[allocation.fund.id] = allocation.allocation / 100;
        } else {
          // Default to equal weights if no allocation specified
          acc[allocation.fund.id] = 1 / allocations.length;
        }
        return acc;
      }, {} as Record<number, number>);
      
      console.log(`Calculated weights for ${Object.keys(weights).length} funds`);
      
      // Get benchmark index performance - using category-based benchmark
      // Note: In the future, this should use fund-specific benchmarks
      const benchmarkName = this.getPortfolioBenchmark(allocations);
      const benchmark = await this.getBenchmarkPerformance(benchmarkName, startDate, endDate);
      
      // Get fund units after initial allocation
      let fundUnits: Record<number, number> = {};
      
      for (const allocation of allocations) {
        const fundId = allocation.fund.id;
        const weight = weights[fundId];
        const initialFundAmount = initialAmount * weight;
        
        // Get NAV at start date
        const initialNav = await this.getNavValue(navData, fundId, startDate);
        
        if (!initialNav) {
          throw new Error(`No NAV data for fund ${fundId} at start date`);
        }
        
        // Calculate units
        fundUnits[fundId] = initialFundAmount / initialNav;
      }
      
      // Optimized: Process only key dates instead of every day
      const keyDates = this.generateKeyDates(startDate, endDate, rebalancePeriod);
      console.log(`Processing ${keyDates.length} key dates instead of daily processing`);
      
      for (let i = 0; i < keyDates.length; i++) {
        const currentDate = keyDates[i];
        let portfolioValue = 0;
        
        // Calculate portfolio value at this key date
        for (const fundId in fundUnits) {
          const units = fundUnits[fundId];
          const nav = await this.getNavValue(navData, parseInt(fundId), currentDate);
          
          if (nav) {
            portfolioValue += units * nav;
          }
        }
        
        if (portfolioValue > 0) {
          returns.push({ date: new Date(currentDate), value: portfolioValue });
          
          // Update high water mark and max drawdown
          if (portfolioValue > highWaterMark) {
            highWaterMark = portfolioValue;
          }
          
          const currentDrawdown = (highWaterMark - portfolioValue) / highWaterMark;
          if (currentDrawdown > maxDrawdown) {
            maxDrawdown = currentDrawdown;
          }
        }
        
        // Rebalance if this is a rebalance date
        if (this.isRebalanceDate(currentDate, rebalanceDates)) {
          currentValue = portfolioValue;
          
          for (const allocation of allocations) {
            const fundId = allocation.fund.id;
            const weight = weights[fundId];
            const targetFundAmount = currentValue * weight;
            
            const rebalanceNav = await this.getNavValue(navData, fundId, currentDate);
            
            if (rebalanceNav) {
              fundUnits[fundId] = targetFundAmount / rebalanceNav;
            }
          }
        }
      }
      
      // Calculate performance metrics
      const startValue = returns[0]?.value || initialAmount;
      const endValue = returns[returns.length - 1]?.value || initialAmount;
      
      const totalReturn = ((endValue / startValue) - 1) * 100;
      const annualizedReturn = this.calculateAnnualizedReturn(startValue, endValue, startDate, endDate);
      
      const returnData = this.calculateReturns(returns);
      const volatility = this.calculateVolatility(returnData.dailyReturns);
      const sharpeRatio = this.calculateSharpeRatio(annualizedReturn, volatility);
      
      const benchmarkReturn = ((benchmark[benchmark.length - 1]?.value || 0) / (benchmark[0]?.value || 1) - 1) * 100;
      
      return {
        portfolioId: portfolio.id,
        riskProfile: portfolio.riskProfile,
        startDate,
        endDate,
        initialAmount,
        finalAmount: endValue,
        totalReturn,
        annualizedReturn,
        maxDrawdown: maxDrawdown * 100,
        volatility: volatility * 100,
        sharpeRatio,
        benchmarkReturn,
        returns,
        benchmark
      };
    } catch (error) {
      console.error('Error running backtest:', error);
      throw error;
    }
  }
  
  /**
   * Generate rebalance dates based on the period
   */
  private getRebalanceDates(startDate: Date, endDate: Date, period: 'monthly' | 'quarterly' | 'annually'): Date[] {
    const dates: Date[] = [new Date(startDate)];
    const currentDate = new Date(startDate);
    
    while (currentDate < endDate) {
      if (period === 'monthly') {
        currentDate.setMonth(currentDate.getMonth() + 1);
      } else if (period === 'quarterly') {
        currentDate.setMonth(currentDate.getMonth() + 3);
      } else {
        currentDate.setFullYear(currentDate.getFullYear() + 1);
      }
      
      if (currentDate < endDate) {
        dates.push(new Date(currentDate));
      }
    }
    
    return dates;
  }

  /**
   * Generate key dates for efficient processing (weekly intervals + rebalance dates)
   */
  private generateKeyDates(startDate: Date, endDate: Date, period: 'monthly' | 'quarterly' | 'annually'): Date[] {
    const keyDates: Date[] = [];
    const rebalanceDates = this.getRebalanceDates(startDate, endDate, period);
    
    // Add weekly intervals for performance tracking
    const currentDate = new Date(startDate);
    while (currentDate <= endDate) {
      keyDates.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 7); // Weekly intervals
    }
    
    // Add all rebalance dates
    rebalanceDates.forEach(date => {
      if (!keyDates.some(existing => existing.getTime() === date.getTime())) {
        keyDates.push(new Date(date));
      }
    });
    
    // Add end date
    if (!keyDates.some(date => date.getTime() === endDate.getTime())) {
      keyDates.push(new Date(endDate));
    }
    
    return keyDates.sort((a, b) => a.getTime() - b.getTime());
  }

  /**
   * Check if a date is a rebalance date
   */
  private isRebalanceDate(date: Date, rebalanceDates: Date[]): boolean {
    return rebalanceDates.some(rebalanceDate => 
      Math.abs(date.getTime() - rebalanceDate.getTime()) < 24 * 60 * 60 * 1000
    );
  }
  
  /**
   * Get historical NAV data for funds
   */
  /**
   * Get historical NAV data for funds - Optimized Version
   * Retrieves only authentic NAV data with efficient database queries
   */
  private async getHistoricalNavData(fundIds: number[], startDate: Date, endDate: Date): Promise<any[]> {
    try {
      console.log(`Fetching NAV data for ${fundIds.length} funds from ${startDate.toISOString()} to ${endDate.toISOString()}`);
      
      // Single optimized query with proper indexing
      const navData = await pool.query(`
        SELECT fund_id, nav_date, nav_value 
        FROM nav_data
        WHERE fund_id = ANY($1)
        AND nav_date BETWEEN $2 AND $3
        ORDER BY fund_id, nav_date
      `, [fundIds, startDate, endDate]);
      
      console.log(`Retrieved ${navData.rows.length} authentic NAV data points`);
      
      // Process and return only authentic data
      return navData.rows.map(row => ({
        fund_id: typeof row.fund_id === 'string' ? parseInt(row.fund_id) : row.fund_id,
        nav_date: row.nav_date instanceof Date ? row.nav_date : new Date(row.nav_date),
        nav_value: typeof row.nav_value === 'string' ? parseFloat(row.nav_value) : row.nav_value
      }));
    } catch (error) {
      console.error('Error getting historical NAV data:', error);
      return [];
    }
  }
  
  /**
   * Get benchmark performance from real market data
   * This method retrieves actual market index data to ensure accurate benchmark comparisons
   */
  private async getBenchmarkPerformance(indexName: string, startDate: Date, endDate: Date): Promise<{ date: Date; value: number }[]> {
    try {
      console.log(`Retrieving benchmark data for "${indexName}" from ${startDate.toISOString()} to ${endDate.toISOString()}`);
      
      const indexData = await pool.query(`
        SELECT index_date, close_value
        FROM market_indices
        WHERE index_name = $1
        AND index_date BETWEEN $2 AND $3
        ORDER BY index_date
      `, [indexName, startDate, endDate]);
      
      const resultCount = indexData.rowCount || 0;
      console.log(`Retrieved ${resultCount} benchmark data points`);
      
      // Check if we have enough benchmark data
      if (resultCount === 0) {
        console.warn(`No benchmark data available for "${indexName}" in the selected date range`);
        console.log("Using fallback benchmark calculation based on market trends");
        
        // If no benchmark data is available, we will return only start and end points
        // based on known typical market returns to provide some comparison
        // This is better than returning no data while still making it clear it's estimated
        return this.generateBasicBenchmarkTrend(indexName, startDate, endDate);
      }
      
      // Process data to ensure consistent date and numeric formats
      return indexData.rows.map((row: any) => ({
        date: row.index_date instanceof Date ? row.index_date : new Date(row.index_date),
        value: typeof row.close_value === 'string' ? parseFloat(row.close_value) : row.close_value
      }));
    } catch (error) {
      console.error('Error getting benchmark performance:', error);
      return [];
    }
  }
  
  /**
   * No synthetic benchmark generation allowed - throw error when real data unavailable
   * This maintains data integrity by refusing to generate fake benchmark data
   */
  private generateBasicBenchmarkTrend(indexName: string, startDate: Date, endDate: Date): { date: Date; value: number }[] {
    console.error(`No authentic benchmark data available for ${indexName} between ${startDate.toISOString()} and ${endDate.toISOString()}`);
    console.error('Synthetic benchmark generation is disabled to maintain data integrity');
    
    // Return empty array instead of synthetic data
    // The calling function should handle this gracefully
    return [];
  }
  
  /**
   * Get NAV value for a specific date
   */
  /**
   * Get NAV value for a specific fund on a specific date - Optimized Version
   * Uses authentic NAV data with efficient lookup and proper fallback
   */
  private async getNavValue(navData: any[], fundId: number, date: Date): Promise<number | null> {
    if (!navData || navData.length === 0) {
      console.warn(`No NAV data available for fund ID ${fundId}`);
      return null;
    }
    
    try {
      const targetDate = date instanceof Date ? date : new Date(date);
      const targetDateStr = targetDate.toISOString().split('T')[0];
      
      // Find fund-specific data
      const fundNavData = navData.filter(nav => nav.fund_id === fundId);
      
      if (fundNavData.length === 0) {
        console.log(`No NAV data found for fund ID ${fundId}`);
        return null;
      }
      
      // Try exact date match first
      const exactMatch = fundNavData.find(nav => {
        const navDateStr = nav.nav_date.toISOString().split('T')[0];
        return navDateStr === targetDateStr;
      });
      
      if (exactMatch && exactMatch.nav_value > 0) {
        return exactMatch.nav_value;
      }
      
      // Find closest date before target
      const beforeDates = fundNavData
        .filter(nav => nav.nav_date < targetDate)
        .sort((a, b) => b.nav_date.getTime() - a.nav_date.getTime());
      
      if (beforeDates.length > 0 && beforeDates[0].nav_value > 0) {
        return beforeDates[0].nav_value;
      }
      
      // Find closest date after target as last resort
      const afterDates = fundNavData
        .filter(nav => nav.nav_date > targetDate)
        .sort((a, b) => a.nav_date.getTime() - b.nav_date.getTime());
      
      if (afterDates.length > 0 && afterDates[0].nav_value > 0) {
        return afterDates[0].nav_value;
      }
      
      console.warn(`No valid NAV data found for fund ID ${fundId} around date ${targetDateStr}`);
      return null;
    } catch (error) {
      console.error(`Error getting NAV value for fund ${fundId}:`, error);
      return null;
    }
  }
  
  /**
   * Calculate annualized return
   */
  private calculateAnnualizedReturn(startValue: number, endValue: number, startDate: Date, endDate: Date): number {
    const years = (endDate.getTime() - startDate.getTime()) / (1000 * 60 * 60 * 24 * 365);
    if (years <= 0) return 0;
    
    return (Math.pow(endValue / startValue, 1 / years) - 1) * 100;
  }
  
  /**
   * Calculate returns data
   */
  private calculateReturns(returns: { date: Date; value: number }[]): {
    dailyReturns: number[];
    cumulativeReturns: number[];
  } {
    const dailyReturns: number[] = [];
    const cumulativeReturns: number[] = [];
    
    for (let i = 1; i < returns.length; i++) {
      const previousValue = returns[i-1].value;
      const currentValue = returns[i].value;
      
      if (previousValue > 0) {
        const dailyReturn = (currentValue / previousValue) - 1;
        dailyReturns.push(dailyReturn);
        
        const cumulativeReturn = (currentValue / returns[0].value) - 1;
        cumulativeReturns.push(cumulativeReturn);
      }
    }
    
    return { dailyReturns, cumulativeReturns };
  }
  
  /**
   * Calculate volatility (standard deviation of returns)
   */
  private calculateVolatility(returns: number[]): number {
    if (returns.length <= 1) return 0;
    
    const mean = returns.reduce((sum, r) => sum + r, 0) / returns.length;
    const variance = returns.reduce((sum, r) => sum + Math.pow(r - mean, 2), 0) / (returns.length - 1);
    
    return Math.sqrt(variance) * Math.sqrt(252); // Annualized
  }
  
  /**
   * Calculate Sharpe ratio
   */
  private calculateSharpeRatio(annualizedReturn: number, volatility: number): number {
    const riskFreeRate = 5.0; // Assume 5% risk-free rate
    if (volatility === 0) return 0;
    
    return (annualizedReturn - riskFreeRate) / volatility;
  }
  
  /**
   * Get portfolio allocations from the database for backtesting
   * If no allocations are available, it will provide well-designed default allocations
   * that follow standard allocation patterns for different risk profiles using the best available funds
   */
  private async getDefaultPortfolioAllocations(): Promise<Record<string, any[]>> {
    console.log("Fetching default portfolio allocations for backtesting");
    
    try {
      // First attempt to fetch real recommended model portfolios from the database
      const models = await pool.query(`
        SELECT id, name, risk_profile FROM model_portfolios 
        ORDER BY created_at DESC
      `);
      
      if (models.rows && models.rows.length > 0) {
        const result: Record<string, any[]> = {};
        
        // For each risk profile, get the most recent model portfolio
        const processedProfiles = new Set<string>();
        
        for (const model of models.rows) {
          const profile = model.risk_profile;
          
          // Skip if we already have this profile
          if (processedProfiles.has(profile)) continue;
          
          // Get allocations for this model
          const allocations = await pool.query(`
            SELECT mpa.allocation_percent, mpa.fund_id, f.category, f.fund_name
            FROM model_portfolio_allocations mpa
            JOIN funds f ON mpa.fund_id = f.id
            WHERE mpa.portfolio_id = $1
          `, [model.id]);
          
          if (allocations.rows && allocations.rows.length > 0) {
            // Map the allocations to our format
            result[profile] = await Promise.all(allocations.rows.map(async row => {
              // Get the fund details to include in the result
              const fundDetails = await pool.query(`
                SELECT * FROM funds WHERE id = $1
              `, [row.fund_id]);
              
              const fund = fundDetails.rows[0];
              
              // Ensure allocation is a number
              const allocation = typeof row.allocation_percent === 'string' 
                ? parseFloat(row.allocation_percent) 
                : row.allocation_percent;
              
              return {
                fund: fund,
                category: row.category,
                allocation: allocation,
                score: 90 // Default high score for selected funds
              };
            }));
            
            processedProfiles.add(profile);
            console.log(`Found real portfolio allocations for ${profile} risk profile with ${result[profile].length} fund categories`);
          }
        }
        
        // If we found allocations for at least some profiles, return them
        if (Object.keys(result).length > 0) {
          return result;
        }
      }
      
      console.log("No existing portfolio allocations found in database, fetching top-rated funds for each category");
      
      // For each risk profile, we'll fetch top funds for appropriate categories
      const defaultAllocations: Record<string, any[]> = {
        'Conservative': [],
        'Moderately Conservative': [],
        'Balanced': [],
        'Moderately Aggressive': [],
        'Aggressive': []
      };
      
      // Get top-rated funds for different categories using authentic fund_scores_corrected
      const topLargeCap = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Equity' AND f.subcategory LIKE '%Large Cap%'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      const topMidCap = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Equity' AND f.subcategory LIKE '%Mid Cap%'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      const topSmallCap = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Equity' AND f.subcategory LIKE '%Small Cap%'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      const topDebtShort = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Debt' AND f.subcategory LIKE '%Short%'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      const topDebtMedium = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Debt' AND f.subcategory LIKE '%Medium%'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      const topHybrid = await pool.query(`
        SELECT fsc.*, f.* FROM fund_scores_corrected fsc
        JOIN funds f ON fsc.fund_id = f.id
        WHERE f.category = 'Hybrid'
        AND fsc.score_date = '2025-06-05'
        ORDER BY fsc.total_score DESC
        LIMIT 2
      `);
      
      // Conservative: 15% Large Cap, 5% Mid Cap, 0% Small Cap, 40% Short Debt, 30% Medium Debt, 10% Hybrid
      if (topLargeCap.rows?.length > 0 && topDebtShort.rows?.length > 0 && 
          topDebtMedium.rows?.length > 0 && topHybrid.rows?.length > 0) {
            
        // Use the first fund from each category with appropriate allocations
        defaultAllocations['Conservative'] = [
          { 
            fund: topLargeCap.rows[0], 
            category: 'Equity-Large Cap', 
            allocation: 15, 
            score: parseFloat(topLargeCap.rows[0].total_score) 
          },
          { 
            fund: topMidCap.rows.length > 0 ? topMidCap.rows[0] : topLargeCap.rows[0],
            category: 'Equity-Mid Cap', 
            allocation: 5, 
            score: topMidCap.rows.length > 0 ? parseFloat(topMidCap.rows[0].total_score) : 80
          },
          { 
            fund: topDebtShort.rows[0], 
            category: 'Debt-Short Duration', 
            allocation: 40, 
            score: parseFloat(topDebtShort.rows[0].total_score) 
          },
          { 
            fund: topDebtMedium.rows[0], 
            category: 'Debt-Medium Duration', 
            allocation: 30, 
            score: parseFloat(topDebtMedium.rows[0].total_score)
          },
          { 
            fund: topHybrid.rows[0], 
            category: 'Hybrid-Aggressive', 
            allocation: 10, 
            score: parseFloat(topHybrid.rows[0].total_score)
          }
        ];
        
        console.log(`Created Conservative portfolio with ${defaultAllocations['Conservative'].length} funds`);
      }
      
      // Moderately Conservative: 25% Large Cap, 10% Mid Cap, 5% Small Cap, 30% Short Debt, 20% Medium Debt, 10% Hybrid
      if (topLargeCap.rows?.length > 0 && topMidCap.rows?.length > 0 && 
          topDebtShort.rows?.length > 0 && topDebtMedium.rows?.length > 0) {
            
        defaultAllocations['Moderately Conservative'] = [
          { 
            fund: topLargeCap.rows[0], 
            category: 'Equity-Large Cap', 
            allocation: 25, 
            score: parseFloat(topLargeCap.rows[0].total_score) 
          },
          { 
            fund: topMidCap.rows[0], 
            category: 'Equity-Mid Cap', 
            allocation: 10, 
            score: parseFloat(topMidCap.rows[0].total_score) 
          },
          { 
            fund: topSmallCap.rows?.length > 0 ? topSmallCap.rows[0] : topMidCap.rows[0], 
            category: 'Equity-Small Cap', 
            allocation: 5, 
            score: topSmallCap.rows?.length > 0 ? parseFloat(topSmallCap.rows[0].total_score) : 85
          },
          { 
            fund: topDebtShort.rows[0], 
            category: 'Debt-Short Duration', 
            allocation: 30, 
            score: parseFloat(topDebtShort.rows[0].total_score) 
          },
          { 
            fund: topDebtMedium.rows[0], 
            category: 'Debt-Medium Duration', 
            allocation: 20, 
            score: parseFloat(topDebtMedium.rows[0].total_score) 
          },
          { 
            fund: topHybrid.rows[0], 
            category: 'Hybrid-Aggressive', 
            allocation: 10, 
            score: parseFloat(topHybrid.rows[0].total_score) 
          }
        ];
        
        console.log(`Created Moderately Conservative portfolio with ${defaultAllocations['Moderately Conservative'].length} funds`);
      }
      
      // Balanced: 30% Large Cap, 15% Mid Cap, 10% Small Cap, 20% Short Debt, 15% Medium Debt, 10% Hybrid
      if (topLargeCap.rows?.length > 0 && topMidCap.rows?.length > 0 && 
          topSmallCap.rows?.length > 0 && topDebtShort.rows?.length > 0) {
            
        defaultAllocations['Balanced'] = [
          { 
            fund: topLargeCap.rows[0], 
            category: 'Equity-Large Cap', 
            allocation: 30, 
            score: parseFloat(topLargeCap.rows[0].total_score) 
          },
          { 
            fund: topMidCap.rows[0], 
            category: 'Equity-Mid Cap', 
            allocation: 15, 
            score: parseFloat(topMidCap.rows[0].total_score) 
          },
          { 
            fund: topSmallCap.rows[0], 
            category: 'Equity-Small Cap', 
            allocation: 10, 
            score: parseFloat(topSmallCap.rows[0].total_score) 
          },
          { 
            fund: topDebtShort.rows[0], 
            category: 'Debt-Short Duration', 
            allocation: 20, 
            score: parseFloat(topDebtShort.rows[0].total_score) 
          },
          { 
            fund: topDebtMedium.rows[0], 
            category: 'Debt-Medium Duration', 
            allocation: 15, 
            score: parseFloat(topDebtMedium.rows[0].total_score) 
          },
          { 
            fund: topHybrid.rows[0], 
            category: 'Hybrid-Aggressive', 
            allocation: 10, 
            score: parseFloat(topHybrid.rows[0].total_score) 
          }
        ];
        
        console.log(`Created Balanced portfolio with ${defaultAllocations['Balanced'].length} funds`);
      }
      
      // Moderately Aggressive: 35% Large Cap, 25% Mid Cap, 15% Small Cap, 10% Short Debt, 5% Medium Debt, 10% Hybrid
      if (topLargeCap.rows?.length > 0 && topMidCap.rows?.length > 0 && topSmallCap.rows?.length > 0) {
        defaultAllocations['Moderately Aggressive'] = [
          { 
            fund: topLargeCap.rows[0], 
            category: 'Equity-Large Cap', 
            allocation: 35, 
            score: parseFloat(topLargeCap.rows[0].total_score) 
          },
          { 
            fund: topMidCap.rows[0], 
            category: 'Equity-Mid Cap', 
            allocation: 25, 
            score: parseFloat(topMidCap.rows[0].total_score) 
          },
          { 
            fund: topSmallCap.rows[0], 
            category: 'Equity-Small Cap', 
            allocation: 15, 
            score: parseFloat(topSmallCap.rows[0].total_score) 
          },
          { 
            fund: topDebtShort.rows?.length > 0 ? topDebtShort.rows[0] : topLargeCap.rows[0],
            category: 'Debt-Short Duration', 
            allocation: 10, 
            score: topDebtShort.rows?.length > 0 ? parseFloat(topDebtShort.rows[0].total_score) : 85
          },
          { 
            fund: topDebtMedium.rows?.length > 0 ? topDebtMedium.rows[0] : topLargeCap.rows[0],
            category: 'Debt-Medium Duration', 
            allocation: 5, 
            score: topDebtMedium.rows?.length > 0 ? parseFloat(topDebtMedium.rows[0].total_score) : 85
          },
          { 
            fund: topHybrid.rows?.length > 0 ? topHybrid.rows[0] : topLargeCap.rows[0],
            category: 'Hybrid-Aggressive', 
            allocation: 10, 
            score: topHybrid.rows?.length > 0 ? parseFloat(topHybrid.rows[0].total_score) : 85
          }
        ];
        
        console.log(`Created Moderately Aggressive portfolio with ${defaultAllocations['Moderately Aggressive'].length} funds`);
      }
      
      // Aggressive: 40% Large Cap, 30% Mid Cap, 20% Small Cap, 5% Short Debt, 0% Medium Debt, 5% Hybrid
      if (topLargeCap.rows?.length > 0 && topMidCap.rows?.length > 0 && topSmallCap.rows?.length > 0) {
        defaultAllocations['Aggressive'] = [
          { 
            fund: topLargeCap.rows[0], 
            category: 'Equity-Large Cap', 
            allocation: 40, 
            score: parseFloat(topLargeCap.rows[0].total_score) 
          },
          { 
            fund: topMidCap.rows[0], 
            category: 'Equity-Mid Cap', 
            allocation: 30, 
            score: parseFloat(topMidCap.rows[0].total_score) 
          },
          { 
            fund: topSmallCap.rows[0], 
            category: 'Equity-Small Cap', 
            allocation: 20, 
            score: parseFloat(topSmallCap.rows[0].total_score) 
          },
          { 
            fund: topDebtShort.rows?.length > 0 ? topDebtShort.rows[0] : topLargeCap.rows[0], 
            category: 'Debt-Short Duration', 
            allocation: 5, 
            score: topDebtShort.rows?.length > 0 ? parseFloat(topDebtShort.rows[0].total_score) : 85
          },
          { 
            fund: topHybrid.rows?.length > 0 ? topHybrid.rows[0] : topLargeCap.rows[0], 
            category: 'Hybrid-Aggressive', 
            allocation: 5, 
            score: topHybrid.rows?.length > 0 ? parseFloat(topHybrid.rows[0].total_score) : 85
          }
        ];
        
        console.log(`Created Aggressive portfolio with ${defaultAllocations['Aggressive'].length} funds`);
      }
      
      // Fallback if we couldn't get proper allocations
      if (Object.values(defaultAllocations).every(allocations => allocations.length === 0)) {
        // Find any valid funds in the database
        const allFunds = await storage.getAllFunds(10);
        
        if (allFunds.length > 0) {
          const fallbackFund = allFunds[0];
          
          // Create a basic allocation for all risk profiles
          for (const riskProfile in defaultAllocations) {
            defaultAllocations[riskProfile] = [
              { 
                fund: fallbackFund, 
                category: fallbackFund.category || 'Equity: Large Cap', 
                allocation: 100, 
                score: 75 
              }
            ];
          }
        }
      }
      
      return defaultAllocations;
    } catch (error) {
      console.error('Error getting default allocations:', error);
      
      // Return empty allocations
      return {
        'Conservative': [],
        'Moderately Conservative': [],
        'Balanced': [],
        'Moderately Aggressive': [],
        'Aggressive': []
      };
    }
  }
  
  /**
   * Run stress test scenarios on the portfolio
   * This simulates portfolio performance under various market conditions
   */
  async runStressTest(params: {
    portfolioId: number;
    scenarios: Array<{
      name: string;
      indexShock: number;
      duration: number;
      recoveryPeriod: number;
    }>;
  }) {
    try {
      const { portfolioId, scenarios } = params;
      
      // Get portfolio
      const portfolio = await storage.getModelPortfolio(portfolioId);
      
      if (!portfolio) {
        throw new Error('Portfolio not found');
      }
      
      // Get portfolio allocations
      const allocations = portfolio.allocations;
      
      if (!allocations || allocations.length === 0) {
        throw new Error('Portfolio has no allocations');
      }
      
      const results = [];
      
      // Run each stress scenario
      for (const scenario of scenarios) {
        // Set up initial parameters
        const initialAmount = 100000; // Standard 1 lakh for comparison
        
        const today = new Date();
        const startDate = new Date(today);
        startDate.setFullYear(today.getFullYear() - 3); // Use 3-year historical data
        
        // Get historical NAV data
        const fundIds = allocations.map(allocation => allocation.fund.id);
        const navData = await this.getHistoricalNavData(fundIds, startDate, today);
        
        // Calculate weights
        const weights = allocations.reduce((acc, allocation) => {
          acc[allocation.fund.id] = parseFloat(allocation.allocationPercent) / 100;
          return acc;
        }, {} as Record<number, number>);
        
        // Map historical data with shock applied
        const stressedReturns = this.simulateMarketShock(
          navData,
          fundIds,
          weights,
          initialAmount,
          scenario.indexShock,
          scenario.duration,
          scenario.recoveryPeriod
        );
        
        // Calculate performance metrics
        const startValue = stressedReturns[0]?.value || initialAmount;
        const endValue = stressedReturns[stressedReturns.length - 1]?.value || initialAmount;
        
        const totalReturn = ((endValue / startValue) - 1) * 100;
        
        // Calculate maximum drawdown during stress period
        let highWaterMark = startValue;
        let maxDrawdown = 0;
        
        for (const point of stressedReturns) {
          if (point.value > highWaterMark) {
            highWaterMark = point.value;
          }
          
          const currentDrawdown = (highWaterMark - point.value) / highWaterMark;
          if (currentDrawdown > maxDrawdown) {
            maxDrawdown = currentDrawdown;
          }
        }
        
        results.push({
          scenarioName: scenario.name,
          indexShock: scenario.indexShock,
          duration: scenario.duration,
          recoveryPeriod: scenario.recoveryPeriod,
          initialValue: startValue,
          finalValue: endValue,
          totalReturn,
          maxDrawdown: maxDrawdown * 100,
          returns: stressedReturns
        });
      }
      
      return {
        portfolioId: portfolio.id,
        riskProfile: portfolio.riskProfile,
        stressTestResults: results
      };
    } catch (error) {
      console.error('Error running stress test:', error);
      throw error;
    }
  }
  
  /**
   * Simulate market shock on historical data
   */
  private simulateMarketShock(
    navData: any[],
    fundIds: number[],
    weights: Record<number, number>,
    initialAmount: number,
    indexShock: number,
    shockDuration: number,
    recoveryPeriod: number
  ): { date: Date; value: number }[] {
    // Set up simulation timeline (90 days by default)
    const simulationDays = 90;
    
    // Create empty returns array
    const returns: { date: Date; value: number }[] = [];
    
    // Calculate initial fund units
    const fundUnits: Record<number, number> = {};
    
    for (const fundId of fundIds) {
      // Find latest NAV for this fund
      const fundNavs = navData.filter(nav => nav.fund_id === fundId)
        .sort((a, b) => new Date(b.nav_date).getTime() - new Date(a.nav_date).getTime());
      
      if (fundNavs.length > 0) {
        const latestNav = fundNavs[0].nav_value;
        const weight = weights[fundId];
        const initialFundAmount = initialAmount * weight;
        
        // Calculate units
        fundUnits[fundId] = initialFundAmount / latestNav;
      }
    }
    
    // Set up timeline
    const startDate = new Date();
    
    // Day 0 - initial value
    returns.push({ date: new Date(startDate), value: initialAmount });
    
    // Apply shock and recovery
    for (let day = 1; day <= simulationDays; day++) {
      let portfolioValue = 0;
      const currentDate = new Date(startDate);
      currentDate.setDate(currentDate.getDate() + day);
      
      // Calculate shock impact
      let shockFactor = 1.0;
      
      // Shock period
      if (day <= shockDuration) {
        // Linear decline to target shock level
        shockFactor = 1.0 - (indexShock / 100) * (day / shockDuration);
      } 
      // Recovery period
      else if (day <= shockDuration + recoveryPeriod) {
        // Calculate minimum value at the end of shock period
        const minFactor = 1.0 - (indexShock / 100);
        
        // Calculate recovery progress (0 to 1)
        const recoveryProgress = (day - shockDuration) / recoveryPeriod;
        
        // Linear recovery from shock
        shockFactor = minFactor + (1.0 - minFactor) * recoveryProgress;
      }
      
      // Apply shock factor to each fund based on beta
      for (const fundId of fundIds) {
        // Find latest NAV for this fund
        const fundNavs = navData.filter(nav => nav.fund_id === fundId)
          .sort((a, b) => new Date(b.nav_date).getTime() - new Date(a.nav_date).getTime());
        
        if (fundNavs.length > 0) {
          const units = fundUnits[fundId];
          const latestNav = fundNavs[0].nav_value;
          
          // Apply fund-specific beta (here we're using a simplified assumption - could be enhanced with actual betas)
          let fundBeta = 1.0; // Assume market beta by default
          
          // In a real implementation, we would calculate or fetch the fund's beta
          // We could also apply sector-specific shocks based on the fund's holdings
          
          // Apply shock with beta adjustment
          const adjustedShockFactor = 1.0 - ((1.0 - shockFactor) * fundBeta);
          
          // Calculate fund value
          portfolioValue += units * latestNav * adjustedShockFactor;
        }
      }
      
      // Add value to returns array
      returns.push({ date: new Date(currentDate), value: portfolioValue });
    }
    
    return returns;
  }
  
  /**
   * Analyze sector performance contribution
   * This breaks down how different sectors contributed to performance
   */
  async analyzeSectorContribution(params: {
    portfolioId: number;
    startDate: Date;
    endDate: Date;
  }) {
    try {
      const { portfolioId, startDate, endDate } = params;
      
      // Get portfolio
      const portfolio = await storage.getModelPortfolio(portfolioId);
      
      if (!portfolio) {
        throw new Error('Portfolio not found');
      }
      
      // Get portfolio allocations
      const allocations = portfolio.allocations;
      
      if (!allocations || allocations.length === 0) {
        throw new Error('Portfolio has no allocations');
      }
      
      const fundIds = allocations.map(allocation => allocation.fund.id);
      
      // Get sector allocations for each fund
      const sectorAllocations: Record<string, number> = {};
      const sectorPerformance: Record<string, { 
        allocation: number; 
        contribution: number;
        return: number;
      }> = {};
      
      // Get holdings data
      const holdingsPromises = fundIds.map(fundId => storage.getPortfolioHoldings(fundId));
      const allHoldings = await Promise.all(holdingsPromises);
      
      // Calculate portfolio performance
      const navData = await this.getHistoricalNavData(fundIds, startDate, endDate);
      
      // Calculate returns for each fund
      const fundReturns: Record<number, number> = {};
      
      for (const fundId of fundIds) {
        const fundStartNav = this.getNavValue(navData, fundId, startDate);
        const fundEndNav = this.getNavValue(navData, fundId, endDate);
        
        if (fundStartNav && fundEndNav) {
          fundReturns[fundId] = (fundEndNav / fundStartNav) - 1;
        }
      }
      
      // Aggregate by sector
      for (let i = 0; i < allocations.length; i++) {
        const allocation = allocations[i];
        const fundId = allocation.fund.id;
        const weight = parseFloat(allocation.allocationPercent) / 100;
        const fundReturn = fundReturns[fundId] || 0;
        
        // Get fund holdings
        const holdings = allHoldings[i];
        
        // Group by sector
        const holdingsBySector: Record<string, number> = {};
        
        for (const holding of holdings) {
          const sector = holding.sector || 'Unknown';
          holdingsBySector[sector] = (holdingsBySector[sector] || 0) + parseFloat(holding.weightPercentage);
        }
        
        // Calculate sector contribution
        for (const [sector, sectorWeight] of Object.entries(holdingsBySector)) {
          const sectorContribution = (sectorWeight / 100) * weight * fundReturn;
          
          if (!sectorPerformance[sector]) {
            sectorPerformance[sector] = {
              allocation: 0,
              contribution: 0,
              return: 0
            };
          }
          
          sectorPerformance[sector].allocation += (sectorWeight / 100) * weight;
          sectorPerformance[sector].contribution += sectorContribution;
        }
      }
      
      // Calculate sector returns
      for (const sector in sectorPerformance) {
        if (sectorPerformance[sector].allocation > 0) {
          sectorPerformance[sector].return = 
            (sectorPerformance[sector].contribution / sectorPerformance[sector].allocation) * 100;
        }
      }
      
      return {
        portfolioId: portfolio.id,
        riskProfile: portfolio.riskProfile,
        startDate,
        endDate,
        sectorContribution: Object.entries(sectorPerformance).map(([sector, data]) => ({
          sector,
          allocation: data.allocation * 100,
          contribution: data.contribution * 100,
          return: data.return
        }))
      };
    } catch (error) {
      console.error('Error analyzing sector contribution:', error);
      throw error;
    }
  }
  
  /**
   * Optimize rebalancing thresholds
   * This determines optimal rebalancing strategy based on historical performance
   */
  async optimizeRebalancingStrategy(params: {
    portfolioId: number;
    startDate: Date;
    endDate: Date;
    initialAmount: number;
    thresholdOptions: number[];
  }) {
    try {
      const { portfolioId, startDate, endDate, initialAmount, thresholdOptions } = params;
      
      // Get portfolio
      const portfolio = await storage.getModelPortfolio(portfolioId);
      
      if (!portfolio) {
        throw new Error('Portfolio not found');
      }
      
      // Get portfolio allocations
      const allocations = portfolio.allocations;
      
      if (!allocations || allocations.length === 0) {
        throw new Error('Portfolio has no allocations');
      }
      
      const fundIds = allocations.map(allocation => allocation.fund.id);
      const navData = await this.getHistoricalNavData(fundIds, startDate, endDate);
      
      // Calculate target weights
      const targetWeights = allocations.reduce((acc, allocation) => {
        acc[allocation.fund.id] = parseFloat(allocation.allocationPercent) / 100;
        return acc;
      }, {} as Record<number, number>);
      
      const results = [];
      
      // Test each threshold option
      for (const threshold of thresholdOptions) {
        // Run backtest with threshold-based rebalancing
        const result = await this.runThresholdBacktest({
          portfolio,
          startDate,
          endDate,
          initialAmount,
          targetWeights,
          navData,
          threshold: threshold / 100 // Convert percentage to decimal
        });
        
        results.push({
          threshold,
          ...result
        });
      }
      
      // Find optimal threshold based on risk-adjusted return
      results.sort((a, b) => b.sharpeRatio - a.sharpeRatio);
      
      return {
        portfolioId: portfolio.id,
        riskProfile: portfolio.riskProfile,
        startDate,
        endDate,
        initialAmount,
        optimalThreshold: results[0]?.threshold || thresholdOptions[0],
        results
      };
    } catch (error) {
      console.error('Error optimizing rebalancing strategy:', error);
      throw error;
    }
  }
  
  /**
   * Run backtest with threshold-based rebalancing
   */
  private async runThresholdBacktest(params: {
    portfolio: any;
    startDate: Date;
    endDate: Date;
    initialAmount: number;
    targetWeights: Record<number, number>;
    navData: any[];
    threshold: number;
  }): Promise<any> {
    const { 
      portfolio, 
      startDate, 
      endDate, 
      initialAmount,
      targetWeights,
      navData,
      threshold
    } = params;
    
    // Initialize metrics
    let currentValue = initialAmount;
    let highWaterMark = initialAmount;
    let maxDrawdown = 0;
    let returns: { date: Date; value: number }[] = [{ date: startDate, value: initialAmount }];
    let rebalanceCount = 0;
    
    const fundIds = Object.keys(targetWeights).map(Number);
    
    // Get fund units after initial allocation
    let fundUnits: Record<number, number> = {};
    
    for (const fundId of fundIds) {
      const weight = targetWeights[fundId];
      const initialFundAmount = initialAmount * weight;
      
      // Get NAV at start date
      const initialNav = await this.getNavValue(navData, fundId, startDate);
      
      if (!initialNav) {
        throw new Error(`No NAV data for fund ${fundId} at start date`);
      }
      
      // Calculate units
      fundUnits[fundId] = initialFundAmount / initialNav;
    }
    
    // Generate daily dates between start and end
    const dailyDates: Date[] = [];
    const currentDate = new Date(startDate);
    
    while (currentDate <= endDate) {
      dailyDates.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 1);
    }
    
    // Current fund weights
    let currentWeights: Record<number, number> = { ...targetWeights };
    
    // Process each day
    for (const date of dailyDates) {
      let portfolioValue = 0;
      let fundValues: Record<number, number> = {};
      
      // Calculate current value and fund values
      for (const fundId of fundIds) {
        const units = fundUnits[fundId];
        const nav = await this.getNavValue(navData, fundId, date);
        
        if (nav) {
          const fundValue = units * nav;
          fundValues[fundId] = fundValue;
          portfolioValue += fundValue;
        }
      }
      
      if (portfolioValue > 0) {
        // Update returns
        returns.push({ date: new Date(date), value: portfolioValue });
        
        // Update high water mark and max drawdown
        if (portfolioValue > highWaterMark) {
          highWaterMark = portfolioValue;
        }
        
        const currentDrawdown = (highWaterMark - portfolioValue) / highWaterMark;
        if (currentDrawdown > maxDrawdown) {
          maxDrawdown = currentDrawdown;
        }
        
        // Calculate current weights
        let needsRebalancing = false;
        
        for (const fundId of fundIds) {
          const fundValue = fundValues[fundId] || 0;
          const currentWeight = fundValue / portfolioValue;
          currentWeights[fundId] = currentWeight;
          
          // Check if weight deviation exceeds threshold
          const targetWeight = targetWeights[fundId];
          const deviation = Math.abs(currentWeight - targetWeight);
          
          if (deviation > threshold) {
            needsRebalancing = true;
          }
        }
        
        // Rebalance if needed
        if (needsRebalancing) {
          rebalanceCount++;
          
          // Rebalance to target weights
          for (const fundId of fundIds) {
            const targetWeight = targetWeights[fundId];
            const targetAmount = portfolioValue * targetWeight;
            
            // Get NAV for rebalance
            const nav = await this.getNavValue(navData, fundId, date);
            
            if (nav) {
              // Calculate new units
              fundUnits[fundId] = targetAmount / nav;
            }
          }
        }
      }
    }
    
    // Calculate performance metrics
    const startValue = returns[0]?.value || initialAmount;
    const endValue = returns[returns.length - 1]?.value || initialAmount;
    
    const totalReturn = ((endValue / startValue) - 1) * 100;
    const annualizedReturn = this.calculateAnnualizedReturn(startValue, endValue, startDate, endDate);
    
    const returnData = this.calculateReturns(returns);
    const volatility = this.calculateVolatility(returnData.dailyReturns);
    const sharpeRatio = this.calculateSharpeRatio(annualizedReturn, volatility);
    
    return {
      totalReturn,
      annualizedReturn,
      maxDrawdown: maxDrawdown * 100,
      volatility: volatility * 100,
      sharpeRatio,
      rebalanceCount,
      rebalanceFrequency: `${(dailyDates.length / Math.max(1, rebalanceCount)).toFixed(1)} days`,
      returns
    };
  }
}

// Export singleton instance
export const backtestingEngine = BacktestingEngine.getInstance();