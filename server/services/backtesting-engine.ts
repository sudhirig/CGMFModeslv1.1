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
   * Run a backtest on a model portfolio for a given time period
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
      const { 
        portfolioId, 
        riskProfile, 
        startDate, 
        endDate, 
        initialAmount,
        rebalancePeriod = 'quarterly'
      } = params;
      
      // Get portfolio - either by ID or by risk profile
      let portfolio;
      let usedGenerator = false;
      
      if (portfolioId) {
        // Lookup by ID
        portfolio = await storage.getModelPortfolio(portfolioId);
      } else if (riskProfile) {
        // First, try to get latest portfolio for risk profile
        const portfolios = await pool.query(`
          SELECT * FROM model_portfolios 
          WHERE risk_profile = $1 
          ORDER BY created_at DESC 
          LIMIT 1
        `, [riskProfile]);
        
        if (portfolios.rows.length > 0) {
          const portfolioId = parseInt(portfolios.rows[0].id);
          if (!isNaN(portfolioId)) {
            portfolio = await storage.getModelPortfolio(portfolioId);
          }
        }
        
        // If no portfolio found or portfolio has no allocations, generate one
        if (!portfolio || !portfolio.allocations || portfolio.allocations.length === 0) {
          // Dynamically import to avoid circular dependencies
          const { portfolioBuilder } = await import('./portfolio-builder');
          console.log(`Generating a new portfolio for risk profile: ${riskProfile}`);
          
          try {
            // Create a new portfolio with the specified risk profile
            const newPortfolio = await portfolioBuilder.generateModelPortfolio(riskProfile as any);
            if (newPortfolio && newPortfolio.id) {
              // Fetch the newly created portfolio with allocations
              portfolio = await storage.getModelPortfolio(newPortfolio.id);
              usedGenerator = true;
              console.log(`Created new portfolio with ID: ${portfolio.id} for backtesting`);
            }
          } catch (genError) {
            console.error("Error generating portfolio:", genError);
          }
        }
      }
      
      // Still no portfolio? Throw an error with helpful message
      if (!portfolio) {
        if (portfolioId) {
          throw new Error(`Portfolio with ID ${portfolioId} not found. Please select a valid portfolio or use a risk profile.`);
        } else if (riskProfile) {
          throw new Error(`Could not create a portfolio for risk profile '${riskProfile}'. Please try a different risk profile.`);
        } else {
          throw new Error('No portfolio ID or risk profile provided. Please specify one of these parameters.');
        }
      }
      
      // Get portfolio allocations
      let allocations = portfolio.allocations;
      
      // Handle case where portfolio has no allocations
      if (!allocations || allocations.length === 0) {
        if (usedGenerator) {
          // We already tried generating a portfolio but it had no allocations
          throw new Error(`Generated portfolio ${portfolio.id} (${portfolio.name}) has no fund allocations. This may indicate an issue with the portfolio builder.`);
        }
        
        console.log(`Portfolio ${portfolio.id} has no allocations. Creating a temporary portfolio for backtesting...`);
        
        // Try generating a new portfolio with the same risk profile
        try {
          const { portfolioBuilder } = await import('./portfolio-builder');
          const tempPortfolio = await portfolioBuilder.generateModelPortfolio(portfolio.riskProfile);
          
          if (tempPortfolio && tempPortfolio.allocations && tempPortfolio.allocations.length > 0) {
            console.log(`Using temporary portfolio ${tempPortfolio.id} with ${tempPortfolio.allocations.length} allocations for backtesting`);
            portfolio = tempPortfolio;
            allocations = tempPortfolio.allocations;
          } else {
            throw new Error('Failed to generate allocations');
          }
        } catch (allocError) {
          console.error("Error generating allocations:", allocError);
          throw new Error(`Portfolio ${portfolio.id} (${portfolio.name}) has no fund allocations and could not create a temporary portfolio. Please try a different portfolio.`);
        }
      }
      
      // Calculate rebalance dates
      const rebalanceDates = this.getRebalanceDates(startDate, endDate, rebalancePeriod);
      
      // Initialize metrics
      let currentValue = initialAmount;
      let highWaterMark = initialAmount;
      let maxDrawdown = 0;
      let returns: { date: Date; value: number }[] = [{ date: startDate, value: initialAmount }];
      
      const fundIds = allocations.map(allocation => allocation.fund.id);
      const navData = await this.getHistoricalNavData(fundIds, startDate, endDate);
      
      // Calculate weights
      const weights = allocations.reduce((acc, allocation) => {
        acc[allocation.fund.id] = parseFloat(allocation.allocationPercent) / 100;
        return acc;
      }, {} as Record<number, number>);
      
      // Get benchmark index performance
      const benchmark = await this.getBenchmarkPerformance('NIFTY 50', startDate, endDate);
      
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
      
      // Loop through each rebalance period
      for (let i = 0; i < rebalanceDates.length; i++) {
        const currentRebalanceDate = rebalanceDates[i];
        const nextRebalanceDate = i < rebalanceDates.length - 1 ? rebalanceDates[i + 1] : endDate;
        
        // Loop through each day in the period
        const currentDay = new Date(currentRebalanceDate);
        while (currentDay <= nextRebalanceDate) {
          let dailyValue = 0;
          
          // Calculate current value
          for (const fundId in fundUnits) {
            const units = fundUnits[fundId];
            const nav = await this.getNavValue(navData, parseInt(fundId), currentDay);
            
            if (nav) {
              dailyValue += units * nav;
            }
          }
          
          if (dailyValue > 0) {
            // Update returns
            returns.push({ date: new Date(currentDay), value: dailyValue });
            
            // Update high water mark and max drawdown
            if (dailyValue > highWaterMark) {
              highWaterMark = dailyValue;
            }
            
            const currentDrawdown = (highWaterMark - dailyValue) / highWaterMark;
            if (currentDrawdown > maxDrawdown) {
              maxDrawdown = currentDrawdown;
            }
          }
          
          // Move to next day
          currentDay.setDate(currentDay.getDate() + 1);
        }
        
        // Rebalance at end of period (except for the last period)
        if (i < rebalanceDates.length - 1) {
          // Get current portfolio value
          currentValue = returns[returns.length - 1]?.value || initialAmount;
          
          // Rebalance
          for (const allocation of allocations) {
            const fundId = allocation.fund.id;
            const weight = weights[fundId];
            const targetFundAmount = currentValue * weight;
            
            // Get NAV at rebalance date
            const rebalanceNav = await this.getNavValue(navData, fundId, nextRebalanceDate);
            
            if (!rebalanceNav) {
              continue;
            }
            
            // Calculate new units
            fundUnits[fundId] = targetFundAmount / rebalanceNav;
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
   * Get historical NAV data for funds
   */
  private async getHistoricalNavData(fundIds: number[], startDate: Date, endDate: Date): Promise<any[]> {
    try {
      const navData = await pool.query(`
        SELECT * FROM nav_data
        WHERE fund_id = ANY($1)
        AND nav_date BETWEEN $2 AND $3
        ORDER BY fund_id, nav_date
      `, [fundIds, startDate, endDate]);
      
      return navData.rows;
    } catch (error) {
      console.error('Error getting historical NAV data:', error);
      return [];
    }
  }
  
  /**
   * Get benchmark performance
   */
  private async getBenchmarkPerformance(indexName: string, startDate: Date, endDate: Date): Promise<{ date: Date; value: number }[]> {
    try {
      const indexData = await pool.query(`
        SELECT index_date, close_value
        FROM market_indices
        WHERE index_name = $1
        AND index_date BETWEEN $2 AND $3
        ORDER BY index_date
      `, [indexName, startDate, endDate]);
      
      return indexData.rows.map((row: any) => ({
        date: row.index_date,
        value: row.close_value
      }));
    } catch (error) {
      console.error('Error getting benchmark performance:', error);
      return [];
    }
  }
  
  /**
   * Get NAV value for a specific date
   */
  private async getNavValue(navData: any[], fundId: number, date: Date): Promise<number | null> {
    // Find exact match
    const exactMatch = navData.find(nav => 
      nav.fund_id === fundId && 
      nav.nav_date.getTime() === date.getTime()
    );
    
    if (exactMatch) {
      return exactMatch.nav_value;
    }
    
    // Find closest date before the target date
    const beforeDates = navData
      .filter(nav => nav.fund_id === fundId && nav.nav_date < date)
      .sort((a, b) => b.nav_date.getTime() - a.nav_date.getTime());
    
    if (beforeDates.length > 0) {
      return beforeDates[0].nav_value;
    }
    
    return null;
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