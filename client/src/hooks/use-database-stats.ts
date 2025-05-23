import { useQuery } from "@tanstack/react-query";

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
  
  const { data, isLoading, error } = useQuery({
    queryKey: ['/api/funds/stats'],
    queryFn: async () => {
      try {
        // Use a high limit to get all funds
        const response = await fetch('/api/funds?limit=5000');
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