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
  // Directly pass the category to the query instead of building a complex endpoint
  const queryKey = category && category !== 'All Categories' 
    ? ['/api/funds', 'category', category] 
    : ['/api/funds'];
  
  const { data, isLoading, error, refetch } = useQuery<Fund[]>({
    queryKey: queryKey,
    queryFn: async () => {
      // Use direct SQL query approach for more reliable results
      let url = '/api/funds';
      
      if (category && category !== 'All Categories') {
        // First check if we need to import data
        const checkResponse = await fetch('/api/funds');
        const existingFunds = await checkResponse.json();
        
        if (!existingFunds || existingFunds.length < 5) {
          console.log("Importing funds since database appears empty...");
          const importResponse = await fetch('/api/import/amfi-data', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
          });
          await importResponse.json();
        }
        
        // Get funds by category with direct SQL
        const response = await fetch(`/api/funds/sql-category/${category}`);
        return response.json();
      } else {
        // Get all funds
        const response = await fetch(url);
        return response.json();
      }
    },
    staleTime: 0, // Don't cache to ensure we always get fresh data
    refetchOnMount: true, // Always refetch when component mounts
    refetchOnWindowFocus: true, // Refetch when window gets focus
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
