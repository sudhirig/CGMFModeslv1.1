import { useQuery } from "@tanstack/react-query";
import { useState, useEffect } from "react";

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
            return await sampleResponse.json();
          }
        }
        
        // Normal path - either get by category or all funds
        if (category && category !== 'All Categories') {
          console.log(`Fetching funds for category: ${category}`);
          const response = await fetch(`/api/funds?category=${encodeURIComponent(category)}`);
          const funds = await response.json();
          console.log(`Found ${funds.length} funds for category ${category}`);
          return funds;
        } else {
          // Get all funds with a reasonable limit
          const response = await fetch('/api/funds?limit=100');
          return await response.json();
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
  const { data, isLoading, error } = useQuery({
    queryKey: [`/api/funds/${fundId}/nav?limit=${days}`],
    staleTime: 5 * 60 * 1000, // 5 minutes
    enabled: !!fundId,
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
