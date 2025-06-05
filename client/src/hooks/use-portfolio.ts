import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

interface Portfolio {
  id: number;
  name: string;
  riskProfile: string;
  allocations: Array<{
    fund: {
      id: number;
      fund_name: string;
      category: string;
    };
    allocation_percent: number;
  }>;
}

interface GeneratePortfolioRequest {
  riskProfile: string;
}

export function usePortfolio() {
  const [selectedPortfolio, setSelectedPortfolio] = useState<Portfolio | null>(null);
  const queryClient = useQueryClient();

  const { data: portfolios, isLoading: isLoadingPortfolios } = useQuery({
    queryKey: ['/api/portfolios'],
    queryFn: async () => {
      const response = await fetch('/api/portfolios');
      if (!response.ok) throw new Error('Failed to fetch portfolios');
      return response.json();
    }
  });

  const generatePortfolioMutation = useMutation({
    mutationFn: async (request: GeneratePortfolioRequest) => {
      const response = await fetch('/api/generate-portfolio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request)
      });
      if (!response.ok) throw new Error('Failed to generate portfolio');
      return response.json();
    },
    onSuccess: (data) => {
      setSelectedPortfolio(data);
      queryClient.invalidateQueries({ queryKey: ['/api/portfolios'] });
    }
  });

  return {
    portfolios,
    selectedPortfolio,
    setSelectedPortfolio,
    generatePortfolio: generatePortfolioMutation.mutate,
    isGenerating: generatePortfolioMutation.isPending,
    isLoadingPortfolios,
    error: generatePortfolioMutation.error
  };
}