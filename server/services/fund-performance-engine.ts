import { executeRawQuery } from '../db';
import { storage } from '../storage';

interface FundPerformanceMetrics {
  fundId: number;
  returns1Y: number | null;
  returns3Y: number | null;
  returns5Y: number | null;
  volatility: number | null;
  sharpeRatio: number | null;
  maxDrawdown: number | null;
  consistencyScore: number | null;
  alpha: number | null;
  beta: number | null;
  informationRatio: number | null;
  totalNavRecords: number;
  dataQualityScore: number;
}

interface QuartileRanking {
  fundId: number;
  category: string;
  compositeScore: number;
  quartile: number;
  quartileLabel: string;
  rank: number;
  totalFunds: number;
  percentile: number;
}

/**
 * Calculate comprehensive fund performance metrics using authentic NAV data
 */
export async function calculateFundPerformance(fundId: number): Promise<FundPerformanceMetrics | null> {
  try {
    // Get NAV data for the fund
    const navResult = await executeRawQuery(`
      SELECT nav_date, nav_value 
      FROM nav_data 
      WHERE fund_id = $1 
      ORDER BY nav_date ASC
    `, [fundId]);

    if (navResult.rows.length < 252) { // Need at least 1 year of data
      console.log(`Fund ${fundId}: Insufficient data (${navResult.rows.length} records)`);
      return null;
    }

    const navData = navResult.rows.map(row => ({
      date: new Date(row.nav_date),
      nav: parseFloat(row.nav_value)
    }));

    // Calculate returns
    const returns1Y = calculateAnnualizedReturn(navData, 1);
    const returns3Y = calculateAnnualizedReturn(navData, 3);
    const returns5Y = calculateAnnualizedReturn(navData, 5);

    // Calculate risk metrics
    const dailyReturns = calculateDailyReturns(navData);
    const volatility = calculateVolatility(dailyReturns);
    const sharpeRatio = calculateSharpeRatio(returns1Y, volatility);
    const maxDrawdown = calculateMaxDrawdown(navData);
    const consistencyScore = calculateConsistencyScore(navData);

    // Calculate alpha and beta (simplified using market proxy)
    const { alpha, beta } = await calculateAlphaBeta(fundId, navData);
    const informationRatio = calculateInformationRatio(dailyReturns, alpha, volatility);

    // Data quality assessment
    const dataQualityScore = calculateDataQuality(navData);

    return {
      fundId,
      returns1Y,
      returns3Y,
      returns5Y,
      volatility,
      sharpeRatio,
      maxDrawdown,
      consistencyScore,
      alpha,
      beta,
      informationRatio,
      totalNavRecords: navData.length,
      dataQualityScore
    };

  } catch (error) {
    console.error(`Error calculating performance for fund ${fundId}:`, error);
    return null;
  }
}

/**
 * Calculate annualized return for a specific period
 */
function calculateAnnualizedReturn(navData: Array<{date: Date, nav: number}>, years: number): number | null {
  if (navData.length < years * 252) return null; // Not enough data

  const endDate = navData[navData.length - 1];
  const startDate = navData.find(d => {
    const yearsDiff = (endDate.date.getTime() - d.date.getTime()) / (365.25 * 24 * 60 * 60 * 1000);
    return yearsDiff >= years;
  });

  if (!startDate) return null;

  const totalReturn = (endDate.nav - startDate.nav) / startDate.nav;
  const actualYears = (endDate.date.getTime() - startDate.date.getTime()) / (365.25 * 24 * 60 * 60 * 1000);
  
  return Math.pow(1 + totalReturn, 1 / actualYears) - 1;
}

/**
 * Calculate daily returns array
 */
function calculateDailyReturns(navData: Array<{date: Date, nav: number}>): number[] {
  const returns = [];
  for (let i = 1; i < navData.length; i++) {
    const dailyReturn = (navData[i].nav - navData[i-1].nav) / navData[i-1].nav;
    returns.push(dailyReturn);
  }
  return returns;
}

/**
 * Calculate annualized volatility
 */
function calculateVolatility(dailyReturns: number[]): number {
  if (dailyReturns.length === 0) return 0;

  const mean = dailyReturns.reduce((sum, ret) => sum + ret, 0) / dailyReturns.length;
  const variance = dailyReturns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / dailyReturns.length;
  const dailyStdDev = Math.sqrt(variance);
  
  return dailyStdDev * Math.sqrt(252); // Annualized
}

/**
 * Calculate Sharpe ratio (assuming 6% risk-free rate)
 */
function calculateSharpeRatio(annualReturn: number | null, volatility: number): number | null {
  if (annualReturn === null || volatility === 0) return null;
  const riskFreeRate = 0.06; // 6% assumption
  return (annualReturn - riskFreeRate) / volatility;
}

/**
 * Calculate maximum drawdown
 */
function calculateMaxDrawdown(navData: Array<{date: Date, nav: number}>): number {
  let maxDrawdown = 0;
  let peak = navData[0].nav;

  for (const point of navData) {
    if (point.nav > peak) {
      peak = point.nav;
    }
    const drawdown = (peak - point.nav) / peak;
    maxDrawdown = Math.max(maxDrawdown, drawdown);
  }

  return maxDrawdown;
}

/**
 * Calculate consistency score based on rolling returns
 */
function calculateConsistencyScore(navData: Array<{date: Date, nav: number}>): number {
  if (navData.length < 504) return 0; // Need at least 2 years

  const quarterlyReturns = [];
  for (let i = 63; i < navData.length; i += 63) { // Quarterly periods
    const startNav = navData[i - 63].nav;
    const endNav = navData[i].nav;
    const quarterlyReturn = (endNav - startNav) / startNav;
    quarterlyReturns.push(quarterlyReturn);
  }

  if (quarterlyReturns.length < 4) return 0;

  // Calculate coefficient of variation (lower is more consistent)
  const mean = quarterlyReturns.reduce((sum, ret) => sum + ret, 0) / quarterlyReturns.length;
  const stdDev = Math.sqrt(quarterlyReturns.reduce((sum, ret) => sum + Math.pow(ret - mean, 2), 0) / quarterlyReturns.length);
  
  const coefficientOfVariation = Math.abs(stdDev / mean);
  return Math.max(0, 1 - coefficientOfVariation); // Convert to 0-1 score
}

/**
 * Calculate alpha and beta against market index
 */
async function calculateAlphaBeta(fundId: number, navData: Array<{date: Date, nav: number}>): Promise<{alpha: number | null, beta: number | null}> {
  try {
    // Get market index data (using NIFTY 50 as benchmark)
    const marketResult = await executeRawQuery(`
      SELECT index_date, close_value::float as close_value
      FROM market_indices 
      WHERE index_name = 'NIFTY 50'
      AND index_date >= $1 AND index_date <= $2
      ORDER BY index_date ASC
    `, [navData[0].date.toISOString().split('T')[0], navData[navData.length - 1].date.toISOString().split('T')[0]]);

    if (marketResult.rows.length < 100) {
      return { alpha: null, beta: null };
    }

    // Calculate fund and market returns for overlapping periods
    const fundReturns = calculateDailyReturns(navData);
    const marketReturns = [];
    
    for (let i = 1; i < marketResult.rows.length; i++) {
      const marketReturn = (marketResult.rows[i].close_value - marketResult.rows[i-1].close_value) / marketResult.rows[i-1].close_value;
      marketReturns.push(marketReturn);
    }

    if (fundReturns.length === 0 || marketReturns.length === 0) {
      return { alpha: null, beta: null };
    }

    // Simple linear regression to find alpha and beta
    const { alpha, beta } = calculateLinearRegression(fundReturns.slice(0, Math.min(fundReturns.length, marketReturns.length)), 
                                                     marketReturns.slice(0, Math.min(fundReturns.length, marketReturns.length)));

    return { alpha: alpha * 252, beta }; // Annualize alpha

  } catch (error) {
    console.error(`Error calculating alpha/beta for fund ${fundId}:`, error);
    return { alpha: null, beta: null };
  }
}

/**
 * Simple linear regression calculation
 */
function calculateLinearRegression(y: number[], x: number[]): {alpha: number, beta: number} {
  const n = Math.min(x.length, y.length);
  if (n === 0) return { alpha: 0, beta: 0 };

  const sumX = x.slice(0, n).reduce((sum, val) => sum + val, 0);
  const sumY = y.slice(0, n).reduce((sum, val) => sum + val, 0);
  const sumXY = x.slice(0, n).reduce((sum, val, i) => sum + val * y[i], 0);
  const sumXX = x.slice(0, n).reduce((sum, val) => sum + val * val, 0);

  const beta = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
  const alpha = (sumY - beta * sumX) / n;

  return { alpha, beta };
}

/**
 * Calculate information ratio
 */
function calculateInformationRatio(returns: number[], alpha: number | null, volatility: number): number | null {
  if (alpha === null || volatility === 0 || returns.length === 0) return null;
  
  const trackingError = calculateVolatility(returns);
  return trackingError > 0 ? alpha / trackingError : null;
}

/**
 * Calculate data quality score
 */
function calculateDataQuality(navData: Array<{date: Date, nav: number}>): number {
  if (navData.length === 0) return 0;

  // Check for data completeness, consistency, and recency
  const daysSinceLastUpdate = (new Date().getTime() - navData[navData.length - 1].date.getTime()) / (24 * 60 * 60 * 1000);
  const recencyScore = Math.max(0, 1 - daysSinceLastUpdate / 30); // Penalize if older than 30 days

  const completenessScore = Math.min(1, navData.length / (5 * 252)); // 5 years of data = 1.0
  
  // Check for data gaps
  let gapPenalty = 0;
  for (let i = 1; i < navData.length; i++) {
    const daysDiff = (navData[i].date.getTime() - navData[i-1].date.getTime()) / (24 * 60 * 60 * 1000);
    if (daysDiff > 7) { // More than a week gap
      gapPenalty += 0.01;
    }
  }

  const consistencyScore = Math.max(0, 1 - gapPenalty);

  return (recencyScore + completenessScore + consistencyScore) / 3;
}

/**
 * Calculate composite score for quartile ranking
 */
export function calculateCompositeScore(metrics: FundPerformanceMetrics): number {
  let score = 0;
  let weightSum = 0;

  // Returns (40% weight)
  if (metrics.returns1Y !== null) {
    score += metrics.returns1Y * 0.25;
    weightSum += 0.25;
  }
  if (metrics.returns3Y !== null) {
    score += metrics.returns3Y * 0.15;
    weightSum += 0.15;
  }

  // Risk-adjusted returns (30% weight)
  if (metrics.sharpeRatio !== null) {
    score += Math.min(metrics.sharpeRatio * 0.1, 0.2); // Cap Sharpe contribution
    weightSum += 0.2;
  }
  if (metrics.informationRatio !== null) {
    score += Math.min(metrics.informationRatio * 0.1, 0.1);
    weightSum += 0.1;
  }

  // Risk management (20% weight)
  if (metrics.maxDrawdown !== null) {
    score += (1 - metrics.maxDrawdown) * 0.1; // Lower drawdown is better
    weightSum += 0.1;
  }
  if (metrics.volatility !== null) {
    score += Math.max(0, (0.3 - metrics.volatility)) * 0.1; // Lower volatility is better
    weightSum += 0.1;
  }

  // Consistency (10% weight)
  if (metrics.consistencyScore !== null) {
    score += metrics.consistencyScore * 0.1;
    weightSum += 0.1;
  }

  return weightSum > 0 ? score / weightSum : 0;
}