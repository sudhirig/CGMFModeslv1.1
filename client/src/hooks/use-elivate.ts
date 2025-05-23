import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";

export interface ElivateScore {
  id: number;
  scoreDate: string;
  externalInfluenceScore: number;
  localStoryScore: number;
  inflationRatesScore: number;
  valuationEarningsScore: number;
  allocationCapitalScore: number;
  trendsSentimentsScore: number;
  totalElivateScore: number;
  marketStance: "BULLISH" | "NEUTRAL" | "BEARISH";
  // Detailed metrics
  usGdpGrowth?: number;
  fedFundsRate?: number;
  dxyIndex?: number;
  chinaPmi?: number;
  indiaGdpGrowth?: number;
  gstCollectionCr?: number;
  iipGrowth?: number;
  indiaPmi?: number;
  cpiInflation?: number;
  wpiInflation?: number;
  repoRate?: number;
  tenYearYield?: number;
  niftyPe?: number;
  niftyPb?: number;
  earningsGrowth?: number;
  fiiFlowsCr?: number;
  diiFlowsCr?: number;
  sipInflowsCr?: number;
  stocksAbove200dmaPct?: number;
  indiaVix?: number;
  advanceDeclineRatio?: number;
}

export function useElivate() {
  const queryClient = useQueryClient();
  
  const { data: elivateScore, isLoading, error } = useQuery<ElivateScore>({
    queryKey: ["/api/elivate/score"],
    staleTime: 60 * 60 * 1000, // 1 hour
  });
  
  const { mutate: calculateElivateScore, isPending: isCalculating } = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", "/api/elivate/calculate", {});
      return response.json();
    },
    onSuccess: () => {
      // Invalidate the elivate score query to refetch it
      queryClient.invalidateQueries({ queryKey: ["/api/elivate/score"] });
    },
  });
  
  return {
    elivateScore,
    isLoading,
    error: error ? (error as Error).message : undefined,
    calculateElivateScore,
    isCalculating,
  };
}
