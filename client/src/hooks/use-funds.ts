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
  const [endpoint, setEndpoint] = useState<string>("/api/funds");
  
  useEffect(() => {
    if (category && category !== 'All Categories') {
      setEndpoint(`/api/funds?category=${encodeURIComponent(category)}`);
      console.log(`Setting endpoint to: /api/funds?category=${encodeURIComponent(category)}`);
    } else {
      setEndpoint("/api/funds");
      console.log("Setting endpoint to: /api/funds");
    }
  }, [category]);
  
  const { data, isLoading, error, refetch } = useQuery<Fund[]>({
    queryKey: [endpoint],
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
