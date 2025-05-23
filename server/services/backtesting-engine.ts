import { db } from '../db';
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
      if (portfolioId) {
        portfolio = await storage.getModelPortfolio(portfolioId);
      } else if (riskProfile) {
        // Get latest portfolio for risk profile
        const portfolios = await db.execute(`
          SELECT * FROM model_portfolios 
          WHERE risk_profile = $1 
          ORDER BY created_at DESC 
          LIMIT 1
        `, { 1: riskProfile });
        
        if (portfolios.rows.length > 0) {
          const portfolioId = portfolios.rows[0].id;
          portfolio = await storage.getModelPortfolio(portfolioId);
        }
      }
      
      if (!portfolio) {
        throw new Error('Portfolio not found');
      }
      
      // Get portfolio allocations
      const allocations = portfolio.allocations;
      
      if (!allocations || allocations.length === 0) {
        throw new Error('Portfolio has no allocations');
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
        acc[allocation.fund.id] = allocation.allocation_percent / 100;
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
        riskProfile: portfolio.risk_profile,
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
      const navData = await db.execute(`
        SELECT * FROM nav_data
        WHERE fund_id = ANY($1)
        AND nav_date BETWEEN $2 AND $3
        ORDER BY fund_id, nav_date
      `, { 1: fundIds, 2: startDate, 3: endDate });
      
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
      const indexData = await db.execute(`
        SELECT index_date, close_value
        FROM market_indices
        WHERE index_name = $1
        AND index_date BETWEEN $2 AND $3
        ORDER BY index_date
      `, { 1: indexName, 2: startDate, 3: endDate });
      
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
}

// Export singleton instance
export const backtestingEngine = BacktestingEngine.getInstance();