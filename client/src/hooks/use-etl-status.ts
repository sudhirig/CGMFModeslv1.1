import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { apiRequest } from "@/lib/queryClient";

interface ETLRun {
  id: number;
  pipelineName: string;
  status: string;
  startTime: string;
  endTime?: string;
  recordsProcessed?: number;
  errorMessage?: string;
  createdAt: string;
}

// Fund Details Stats interface
export interface FundDetailsStats {
  totalFunds: number;
  enhancedFunds: number;
  pendingFunds: number;
  percentComplete: number;
  isCollectionInProgress: boolean;
}

interface FundDetailsStatus {
  success: boolean;
  status: ETLRun | null;
  detailsStats: FundDetailsStats;
}

export function useEtlStatus() {
  const queryClient = useQueryClient();
  
  // Get ETL status
  const { data: etlRuns, isLoading, error, refetch: refreshEtlStatus } = useQuery<ETLRun[]>({
    queryKey: ["/api/etl/status"],
    staleTime: 60 * 1000, // 1 minute
  });
  
  // Get fund details status and stats
  const { data: fundDetailsData, isLoading: isLoadingFundDetails } = useQuery<FundDetailsStatus>({
    queryKey: ["/api/fund-details/status"],
    staleTime: 30 * 1000, // 30 seconds
  });
  
  // Trigger data collection
  const { mutate: triggerDataCollection, isPending: isCollecting } = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", "/api/etl/collect", {});
      return response.json();
    },
    onSuccess: () => {
      // Invalidate ETL status query
      queryClient.invalidateQueries({ queryKey: ["/api/etl/status"] });
    },
  });
  
  // Trigger fund details collection
  const { mutate: triggerFundDetailsCollection, isPending: isCollectingDetails } = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", "/api/fund-details", {});
      return response.json();
    },
    onSuccess: () => {
      // Invalidate both ETL status and fund details status queries
      queryClient.invalidateQueries({ queryKey: ["/api/etl/status"] });
      queryClient.invalidateQueries({ queryKey: ["/api/fund-details/status"] });
    },
  });
  
  // Schedule regular fund details collection
  const { mutate: scheduleFundDetailsCollection } = useMutation({
    mutationFn: async (hours: number = 168) => { // Default: weekly
      const response = await apiRequest("POST", "/api/fund-details/schedule", { hours });
      return response.json();
    },
    onSuccess: () => {
      // Invalidate both ETL status and fund details status queries
      queryClient.invalidateQueries({ queryKey: ["/api/etl/status"] });
      queryClient.invalidateQueries({ queryKey: ["/api/fund-details/status"] });
    },
  });
  
  // Schedule bulk processing of fund details
  const { mutate: scheduleBulkProcessing, isPending: isSchedulingBulk } = useMutation({
    mutationFn: async (config: { batchSize?: number; batchCount?: number; intervalHours?: number }) => {
      const response = await apiRequest("POST", "/api/fund-details/schedule-bulk", config);
      return response.json();
    },
    onSuccess: () => {
      // Invalidate relevant queries
      queryClient.invalidateQueries({ queryKey: ["/api/etl/status"] });
      queryClient.invalidateQueries({ queryKey: ["/api/fund-details/status"] });
    },
  });
  
  // Stop scheduled bulk processing
  const { mutate: stopBulkProcessing } = useMutation({
    mutationFn: async () => {
      const response = await apiRequest("POST", "/api/fund-details/stop-bulk", {});
      return response.json();
    },
    onSuccess: () => {
      // Invalidate relevant queries
      queryClient.invalidateQueries({ queryKey: ["/api/etl/status"] });
      queryClient.invalidateQueries({ queryKey: ["/api/fund-details/status"] });
    },
  });
  
  // Extract fund details stats from the response or provide defaults
  const fundDetailsStats: FundDetailsStats = fundDetailsData?.detailsStats || {
    totalFunds: 0,
    enhancedFunds: 0,
    pendingFunds: 0,
    percentComplete: 0,
    isCollectionInProgress: false
  };

  return {
    etlRuns,
    isLoading,
    error: error ? (error as Error).message : undefined,
    refreshEtlStatus,
    triggerDataCollection,
    isCollecting,
    triggerFundDetailsCollection,
    isCollectingDetails,
    scheduleFundDetailsCollection,
    scheduleBulkProcessing,
    isSchedulingBulk,
    stopBulkProcessing,
    fundDetailsStats,
    isLoadingFundDetails
  };
}
