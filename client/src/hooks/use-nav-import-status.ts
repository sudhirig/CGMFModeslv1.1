import { useQuery } from "@tanstack/react-query";

export interface NavImportStatusData {
  fundCount: number;
  navCount: number; 
  navWithHistoryCount: number;
  success?: boolean;
  error?: any;
}

export function useNavImportStatus() {
  // Get NAV import status
  const { 
    data: amfiStatus, 
    isLoading, 
    error, 
    refetch 
  } = useQuery<NavImportStatusData>({
    queryKey: ["/api/amfi/status"],
    staleTime: 10 * 1000, // 10 seconds - more frequent updates for NAV import tracking
  });
  
  // Get scheduled status 
  const { 
    data: scheduledStatus,
    isLoading: isLoadingScheduled,
    refetch: refetchScheduled
  } = useQuery<{
    success: boolean;
    scheduledImports: {
      daily: { active: boolean; lastRun: string | null };
      historical: { active: boolean; lastRun: string | null };
    }
  }>({
    queryKey: ["/api/amfi/scheduled-status"],
    staleTime: 20 * 1000, // 20 seconds
  });

  return {
    // Provide safe defaults if data is loading
    amfiStatus: amfiStatus || {
      fundCount: 0,
      navCount: 0,
      navWithHistoryCount: 0,
      success: false
    },
    scheduledImports: scheduledStatus?.scheduledImports || {
      daily: { active: false, lastRun: null },
      historical: { active: false, lastRun: null }
    },
    isLoading,
    isLoadingScheduled,
    error: error ? (error as Error).message : undefined,
    refetchNavStatus: refetch,
    refetchScheduledStatus: refetchScheduled
  };
}