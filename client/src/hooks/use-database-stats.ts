import { useQuery } from "@tanstack/react-query";

interface Fund {
  id: number;
  schemeCode: string;
  fundName: string;
  amcName: string;
  category: string;
  subcategory: string | null;
}

export function useDatabaseStats() {
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
      subcategory: fund.subcategory
    }));
  };
  
  const { data, isLoading, error } = useQuery<Fund[]>({
    queryKey: ['/api/funds/all'],
    queryFn: async () => {
      try {
        // No limit parameter to get ALL funds
        const response = await fetch('/api/funds');
        const funds = await response.json();
        console.log(`Found ${funds.length} total funds in database for statistics`);
        return convertToCamelCase(funds);
      } catch (err) {
        console.error("Error fetching all funds for stats:", err);
        return [];
      }
    },
    staleTime: 60000, // 1 minute
    refetchOnMount: true,
    refetchOnWindowFocus: false
  });
  
  return {
    funds: data || [],
    isLoading,
    error: error ? (error as Error).message : undefined,
  };
}