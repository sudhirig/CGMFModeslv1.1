import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";
import { useState } from "react";

export type RiskProfile = 'Conservative' | 'Moderately Conservative' | 'Balanced' | 'Moderately Aggressive' | 'Aggressive';

interface AssetAllocation {
  equityLargeCap: number;
  equityMidCap: number;
  equitySmallCap: number;
  debtShortTerm: number;
  debtMediumTerm: number;
  hybrid: number;
}

interface ExpectedReturns {
  min: number;
  max: number;
}

interface Fund {
  id: number;
  fundName: string;
  amcName: string;
  category: string;
  subcategory: string | null;
}

interface Allocation {
  portfolioId: number;
  fundId: number;
  allocationPercent: number;
  fund: Fund;
}

export interface PortfolioData {
  id: number;
  name: string;
  riskProfile: RiskProfile;
  elivateScoreId: number;
  assetAllocation: AssetAllocation;
  expectedReturns: ExpectedReturns;
  allocations: Allocation[];
  fundScores?: Record<number, number>;
  createdAt: string;
}

export function usePortfolio() {
  const queryClient = useQueryClient();
  const [portfolio, setPortfolio] = useState<PortfolioData | null>(null);
  
  // Query for getting portfolios
  const { data: portfolios, isLoading: isLoadingPortfolios, error: portfoliosError } = useQuery({
    queryKey: ["/api/portfolios"],
    staleTime: 10 * 60 * 1000, // 10 minutes
  });
  
  // Mutation for generating a portfolio
  const { mutate, isPending: isLoading, error } = useMutation({
    mutationFn: async (riskProfile: RiskProfile) => {
      const response = await apiRequest("POST", "/api/portfolios/generate", { riskProfile });
      return response.json();
    },
    onSuccess: (data: PortfolioData) => {
      setPortfolio(data);
      // Invalidate the portfolios query
      queryClient.invalidateQueries({ queryKey: ["/api/portfolios"] });
    },
  });
  
  const generatePortfolio = (riskProfile: RiskProfile) => {
    mutate(riskProfile);
  };
  
  // Get portfolio details by ID
  const getPortfolioById = async (portfolioId: number) => {
    try {
      const response = await fetch(`/api/portfolios/${portfolioId}`, {
        credentials: "include",
      });
      
      if (!response.ok) {
        throw new Error(`Failed to fetch portfolio: ${response.statusText}`);
      }
      
      const data = await response.json();
      setPortfolio(data);
      return data;
    } catch (error) {
      console.error("Error fetching portfolio:", error);
      throw error;
    }
  };
  
  return {
    portfolio,
    portfolios,
    isLoading: isLoading || isLoadingPortfolios,
    error: error ? (error as Error).message : portfoliosError ? (portfoliosError as Error).message : undefined,
    generatePortfolio,
    getPortfolioById,
  };
}
