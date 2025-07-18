import { useQuery } from "@tanstack/react-query";
import { useState, useEffect, useMemo } from "react";

interface Fund {
  id: number;
  schemeCode: string;
  fundName: string;
  amcName: string;
  category: string;
  subcategory: string | null;
  benchmarkName: string | null;
  fundManager: string | null;
  inceptionDate: string | null;
  status: string;
  minimumInvestment: number | null;
  minimumAdditional: number | null;
  exitLoad: number | null;
  lockInPeriod: number | null;
  expenseRatio: number | null;
}

export function useFunds(category?: string) {
  // Create a stable query key based on the category
  const queryKey = category && category !== 'All Categories' 
    ? ['/api/funds', 'category', category] 
    : ['/api/funds'];
  
  // Convert database fields to camelCase for the frontend
  const convertToCamelCase = (funds: any[]) => {
    if (!funds || !Array.isArray(funds)) return [];
    
    return funds.map(fund => ({
      id: fund.id,
      schemeCode: fund.scheme_code,
      isinDivPayout: fund.isin_div_payout,
      isinDivReinvest: fund.isin_div_reinvest,
      fundName: fund.fund_name,
      amcName: fund.amc_name,
      category: fund.category,
      subcategory: fund.subcategory,
      benchmarkName: fund.benchmark_name,
      fundManager: fund.fund_manager,
      inceptionDate: fund.inception_date,
      status: fund.status,
      minimumInvestment: fund.minimum_investment,
      minimumAdditional: fund.minimum_additional,
      exitLoad: fund.exit_load,
      lockInPeriod: fund.lock_in_period,
      expenseRatio: fund.expense_ratio,
      createdAt: fund.created_at,
      updatedAt: fund.updated_at,
      // Additional fields needed by fund cards
      minSip: fund.minimum_investment,
      aum: fund.aum_crores ? parseFloat(fund.aum_crores) * 10000000 : null, // Convert crores to rupees
      aumCrores: fund.aum_crores,
      nav: fund.latest_nav ? parseFloat(fund.latest_nav) : null,
      totalScore: fund.total_score
    }));
  };
  
  const { data, isLoading, error, refetch } = useQuery<Fund[]>({
    queryKey: queryKey,
    queryFn: async () => {
      try {
        // Check if we need to create sample data first
        const checkResponse = await fetch('/api/funds');
        const checkData = await checkResponse.json();
        console.log(`Found ${checkData?.length || 0} total funds in the database`);
        
        // If very few funds, make sure we have samples in all categories
        if (!checkData || checkData.length < 15) {
          console.log("Creating sample fund data...");
          // We'll use direct SQL through our custom endpoint
          if (category && category !== 'All Categories') {
            const sampleResponse = await fetch(`/api/funds/sql-category/${category}`);
            const funds = await sampleResponse.json();
            return convertToCamelCase(funds);
          }
        }
        
        // Normal path - either get by category or all funds
        if (category && category !== 'All Categories') {
          console.log(`Fetching funds for category: ${category}`);
          const response = await fetch(`/api/funds?category=${encodeURIComponent(category)}&limit=5000`);
          const funds = await response.json();
          console.log(`Found ${funds.length} funds for category ${category}`);
          return convertToCamelCase(funds);
        } else {
          // Get all funds with a higher limit to show all imported funds
          const response = await fetch('/api/funds?limit=5000');
          const funds = await response.json();
          return convertToCamelCase(funds);
        }
      } catch (err) {
        console.error("Error fetching funds:", err);
        return [];
      }
    },
    staleTime: 10000, // 10 seconds to avoid too many refreshes
    refetchOnMount: true,
    refetchOnWindowFocus: false // Don't constantly refresh on window focus
  });
  
  return {
    funds: data,
    isLoading,
    error: error ? (error as Error).message : undefined,
    refetch,
  };
}

export function useFund(fundId: number) {
  const { data, isLoading, error, refetch } = useQuery<Fund>({
    queryKey: [`/api/funds/${fundId}`],
    staleTime: 5 * 60 * 1000, // 5 minutes
    enabled: !!fundId,
  });
  
  return {
    fund: data,
    isLoading,
    error: error ? (error as Error).message : undefined,
    refetch,
  };
}

export function useFundNavHistory(fundId: number, days: number = 365) {
  // Memoize date calculations to prevent unnecessary re-renders
  const { startDate, endDate } = useMemo(() => {
    const end = new Date();
    const start = new Date();
    start.setDate(start.getDate() - days);
    
    // Format dates as YYYY-MM-DD to avoid millisecond precision issues
    const startStr = start.toISOString().split('T')[0];
    const endStr = end.toISOString().split('T')[0];
    
    return {
      startDate: startStr,
      endDate: endStr
    };
  }, [days]);
  
  const { data, isLoading, error } = useQuery({
    queryKey: [`/api/funds/${fundId}/nav?startDate=${startDate}&endDate=${endDate}&limit=1000`],
    staleTime: 30 * 60 * 1000, // 30 minutes - increase cache time
    gcTime: 60 * 60 * 1000, // 1 hour - keep in cache longer
    refetchOnWindowFocus: false, // Prevent refetch on window focus
    refetchOnMount: false, // Don't refetch if data exists
    enabled: !!fundId && fundId > 0,
  });
  
  return {
    navHistory: data,
    isLoading,
    error: error ? (error as Error).message : undefined,
  };
}

export function useFundScore(fundId: number) {
  const { data, isLoading, error } = useQuery({
    queryKey: [`/api/funds/${fundId}/score`],
    staleTime: 5 * 60 * 1000, // 5 minutes
    enabled: !!fundId,
  });
  
  return {
    score: data,
    isLoading,
    error: error ? (error as Error).message : undefined,
  };
}

export function useFundHoldings(fundId: number) {
  const { data, isLoading, error } = useQuery({
    queryKey: [`/api/funds/${fundId}/holdings`],
    staleTime: 5 * 60 * 1000, // 5 minutes
    enabled: !!fundId,
  });
  
  return {
    holdings: data,
    isLoading,
    error: error ? (error as Error).message : undefined,
  };
}
