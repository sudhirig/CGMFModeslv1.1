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

export function useEtlStatus() {
  const queryClient = useQueryClient();
  
  // Get ETL status
  const { data: etlRuns, isLoading, error, refetch: refreshEtlStatus } = useQuery<ETLRun[]>({
    queryKey: ["/api/etl/status"],
    staleTime: 60 * 1000, // 1 minute
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
  
  return {
    etlRuns,
    isLoading,
    error: error ? (error as Error).message : undefined,
    refreshEtlStatus,
    triggerDataCollection,
    isCollecting,
  };
}
