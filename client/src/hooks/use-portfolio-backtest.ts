import { useState } from "react";
import { usePortfolio, PortfolioData, RiskProfile } from "@/hooks/use-portfolio";
import { apiRequest } from "@/lib/queryClient";

export interface BacktestParams {
  portfolioId: number;
  startDate: Date;
  endDate: Date;
  initialAmount: number;
  rebalancePeriod: 'monthly' | 'quarterly' | 'annually' | 'none';
}

export interface BacktestResults {
  portfolioPerformance: {
    date: string;
    value: number;
  }[];
  benchmarkPerformance: {
    date: string;
    value: number;
  }[];
  metrics: {
    totalReturn: number;
    annualizedReturn: number;
    volatility: number;
    sharpeRatio: number;
    maxDrawdown: number;
    successRate: number;
  };
  summary: {
    startValue: number;
    endValue: number;
    netProfit: number;
    percentageGain: number;
  };
}

export function usePortfolioBacktest() {
  const { portfolio, portfolios, generatePortfolio, getPortfolioById, isLoading, error } = usePortfolio();
  const [backtestResults, setBacktestResults] = useState<BacktestResults | null>(null);
  const [isRunningBacktest, setIsRunningBacktest] = useState(false);
  const [backtestError, setBacktestError] = useState<string | undefined>(undefined);

  // Run backtest on a portfolio
  const runBacktest = async (params: BacktestParams) => {
    try {
      setIsRunningBacktest(true);
      setBacktestError(undefined);
      
      const response = await apiRequest("POST", "/api/backtest/portfolio", params);
      
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.message || "Failed to run backtest");
      }
      
      const results: BacktestResults = await response.json();
      setBacktestResults(results);
      return results;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "An unknown error occurred";
      setBacktestError(errorMessage);
      throw error;
    } finally {
      setIsRunningBacktest(false);
    }
  };

  // Generate a portfolio and immediately run a backtest on it
  const generateAndBacktest = async (
    riskProfile: RiskProfile, 
    backtestParams: Omit<BacktestParams, "portfolioId">
  ) => {
    try {
      // First generate the portfolio
      const generatedPortfolio = await generatePortfolio(riskProfile);
      
      if (!generatedPortfolio) {
        throw new Error("Failed to generate portfolio");
      }
      
      // Then run a backtest on it
      const portfolioId = typeof generatedPortfolio.id === 'string' 
        ? parseInt(generatedPortfolio.id) 
        : generatedPortfolio.id;

      return await runBacktest({
        ...backtestParams,
        portfolioId
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "An unknown error occurred";
      setBacktestError(errorMessage);
      throw error;
    }
  };

  // Clear backtest results
  const clearBacktestResults = () => {
    setBacktestResults(null);
    setBacktestError(undefined);
  };

  return {
    // Portfolio-related properties and methods
    portfolio,
    portfolios,
    generatePortfolio,
    getPortfolioById,
    
    // Backtest-related properties and methods
    backtestResults,
    isRunningBacktest,
    backtestError,
    runBacktest,
    generateAndBacktest,
    clearBacktestResults,
    
    // Shared properties
    isLoading: isLoading || isRunningBacktest,
    error: error || backtestError
  };
}