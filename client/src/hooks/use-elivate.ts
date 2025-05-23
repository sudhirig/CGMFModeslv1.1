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
  
  const { data: rawElivateScore, isLoading, error } = useQuery({
    queryKey: ["/api/elivate/score"],
    staleTime: 60 * 60 * 1000, // 1 hour
  });
  
  // Process the raw data to ensure all numeric fields are properly converted from strings
  const elivateScore = rawElivateScore ? {
    ...rawElivateScore,
    // Convert all score string values to numbers
    externalInfluenceScore: parseFloat(rawElivateScore.external_influence_score || '0'),
    localStoryScore: parseFloat(rawElivateScore.local_story_score || '0'),
    inflationRatesScore: parseFloat(rawElivateScore.inflation_rates_score || '0'),
    valuationEarningsScore: parseFloat(rawElivateScore.valuation_earnings_score || '0'),
    allocationCapitalScore: parseFloat(rawElivateScore.allocation_capital_score || '0'),
    trendsSentimentsScore: parseFloat(rawElivateScore.trends_sentiments_score || '0'),
    totalElivateScore: parseFloat(rawElivateScore.total_elivate_score || '0'),
    marketStance: rawElivateScore.market_stance,
    scoreDate: rawElivateScore.score_date,
    
    // Detail metrics
    usGdpGrowth: parseFloat(rawElivateScore.us_gdp_growth || '0'),
    fedFundsRate: parseFloat(rawElivateScore.fed_funds_rate || '0'),
    dxyIndex: parseFloat(rawElivateScore.dxy_index || '0'),
    chinaPmi: parseFloat(rawElivateScore.china_pmi || '0'),
    indiaGdpGrowth: parseFloat(rawElivateScore.india_gdp_growth || '0'),
    gstCollectionCr: parseFloat(rawElivateScore.gst_collection_cr || '0'),
    iipGrowth: parseFloat(rawElivateScore.iip_growth || '0'),
    indiaPmi: parseFloat(rawElivateScore.india_pmi || '0'),
    cpiInflation: parseFloat(rawElivateScore.cpi_inflation || '0'),
    wpiInflation: parseFloat(rawElivateScore.wpi_inflation || '0'),
    repoRate: parseFloat(rawElivateScore.repo_rate || '0'),
    tenYearYield: parseFloat(rawElivateScore.ten_year_yield || '0'),
    niftyPe: parseFloat(rawElivateScore.nifty_pe || '0'),
    niftyPb: parseFloat(rawElivateScore.nifty_pb || '0'),
    earningsGrowth: parseFloat(rawElivateScore.earnings_growth || '0'),
    fiiFlowsCr: parseFloat(rawElivateScore.fii_flows_cr || '0'),
    diiFlowsCr: parseFloat(rawElivateScore.dii_flows_cr || '0'),
    sipInflowsCr: parseFloat(rawElivateScore.sip_inflows_cr || '0'),
    stocksAbove200dmaPct: parseFloat(rawElivateScore.stocks_above_200dma_pct || '0'),
    indiaVix: parseFloat(rawElivateScore.india_vix || '0'),
    advanceDeclineRatio: parseFloat(rawElivateScore.advance_decline_ratio || '0'),
  } as ElivateScore : undefined;
  
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
