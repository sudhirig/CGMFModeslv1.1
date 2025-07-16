import { useQuery } from "@tanstack/react-query";
import { useFund, useFundNavHistory, useFundScore, useFundHoldings } from "./use-funds";

export interface FundDetails {
  basicData: any;
  navHistory: any[];
  score: any;
  holdings: any[];
  performance: {
    currentNav: number;
    navChange: number;
    navChangePct: number;
    return1y: number;
    return3y: number;
    return5y: number;
    ytd: number;
  };
  riskMetrics: {
    sharpeRatio: number;
    beta: number;
    alpha: number;
    volatility: number;
    maxDrawdown: number;
  };
  fundamentals: {
    expenseRatio: number;
    aum: number;
    fundAge: number;
    minInvestment: number;
    exitLoad: number;
  };
}

export function useFundDetails(fundId: number) {
  const { fund: basicData, isLoading: basicLoading } = useFund(fundId);
  const { navHistory, isLoading: navLoading } = useFundNavHistory(fundId, 365);
  const { score, isLoading: scoreLoading } = useFundScore(fundId);
  const { holdings, isLoading: holdingsLoading } = useFundHoldings(fundId);

  // Calculate performance metrics from NAV history
  const performance = calculatePerformanceMetrics(navHistory);
  
  // Extract risk metrics from score data
  const riskMetrics = extractRiskMetrics(score);
  
  // Extract fundamentals from basic data and score
  const fundamentals = extractFundamentals(basicData, score);

  const isLoading = basicLoading || navLoading || scoreLoading || holdingsLoading;

  return {
    fundDetails: {
      basicData,
      navHistory,
      score,
      holdings,
      performance,
      riskMetrics,
      fundamentals
    } as FundDetails,
    isLoading,
    hasData: !!basicData
  };
}

function calculatePerformanceMetrics(navHistory: any[]) {
  if (!navHistory || navHistory.length === 0) {
    return {
      currentNav: 0,
      navChange: 0,
      navChangePct: 0,
      return1y: 0,
      return3y: 0,
      return5y: 0,
      ytd: 0
    };
  }

  const sortedNavs = navHistory.sort((a, b) => new Date(b.navDate).getTime() - new Date(a.navDate).getTime());
  const currentNav = parseFloat(sortedNavs[0]?.navValue || 0);
  const previousNav = parseFloat(sortedNavs[1]?.navValue || 0);
  
  const navChange = currentNav - previousNav;
  const navChangePct = previousNav ? ((navChange / previousNav) * 100) : 0;

  return {
    currentNav,
    navChange,
    navChangePct,
    return1y: calculateReturn(sortedNavs, 365),
    return3y: calculateReturn(sortedNavs, 365 * 3),
    return5y: calculateReturn(sortedNavs, 365 * 5),
    ytd: calculateYTDReturn(sortedNavs)
  };
}

function calculateReturn(navHistory: any[], days: number): number {
  if (!navHistory || navHistory.length < 2) return 0;
  
  const current = parseFloat(navHistory[0]?.navValue || 0);
  const historical = navHistory.find(nav => {
    const daysDiff = Math.abs(new Date().getTime() - new Date(nav.navDate).getTime()) / (1000 * 60 * 60 * 24);
    return daysDiff >= days;
  });
  
  if (!historical) return 0;
  
  const historicalValue = parseFloat(historical.navValue);
  return historicalValue ? ((current - historicalValue) / historicalValue) * 100 : 0;
}

function calculateYTDReturn(navHistory: any[]): number {
  if (!navHistory || navHistory.length < 2) return 0;
  
  const current = parseFloat(navHistory[0]?.navValue || 0);
  const yearStart = new Date(new Date().getFullYear(), 0, 1);
  
  const ytdNav = navHistory.find(nav => new Date(nav.navDate) <= yearStart);
  if (!ytdNav) return 0;
  
  const ytdValue = parseFloat(ytdNav.navValue);
  return ytdValue ? ((current - ytdValue) / ytdValue) * 100 : 0;
}

function extractRiskMetrics(score: any) {
  if (!score) {
    return {
      sharpeRatio: 0,
      beta: 0,
      alpha: 0,
      volatility: 0,
      maxDrawdown: 0
    };
  }

  return {
    sharpeRatio: parseFloat(score.sharpe_ratio_1y || 0),
    beta: parseFloat(score.beta_1y || 0),
    alpha: parseFloat(score.alpha_1y || 0),
    volatility: parseFloat(score.volatility_1y || 0),
    maxDrawdown: parseFloat(score.max_drawdown_1y || 0)
  };
}

function extractFundamentals(basicData: any, score: any) {
  if (!basicData) {
    return {
      expenseRatio: 0,
      aum: 0,
      fundAge: 0,
      minInvestment: 0,
      exitLoad: 0
    };
  }

  return {
    expenseRatio: parseFloat(basicData.expense_ratio || 0),
    aum: parseFloat(score?.aum_cr || 0),
    fundAge: calculateFundAge(basicData.inception_date),
    minInvestment: parseFloat(basicData.minimum_investment || 0),
    exitLoad: parseFloat(basicData.exit_load || 0)
  };
}

function calculateFundAge(inceptionDate: string): number {
  if (!inceptionDate) return 0;
  const inception = new Date(inceptionDate);
  const now = new Date();
  return Math.floor((now.getTime() - inception.getTime()) / (1000 * 60 * 60 * 24 * 365));
}