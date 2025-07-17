import { useQuery } from "@tanstack/react-query";

// Hook to fetch multiple fund scores at once
export function useFundScores(fundIds: number[]) {
  return useQuery({
    queryKey: ['/api/funds/scores', fundIds],
    queryFn: async () => {
      if (fundIds.length === 0) return {};
      
      // Fetch scores for all fund IDs in parallel
      const scorePromises = fundIds.map(id => 
        fetch(`/api/funds/${id}/score`).then(res => 
          res.ok ? res.json() : null
        ).catch(() => null)
      );
      
      const scores = await Promise.all(scorePromises);
      
      // Create a map of fund ID to score data
      const scoreMap: Record<number, any> = {};
      fundIds.forEach((id, index) => {
        if (scores[index]) {
          scoreMap[id] = scores[index];
        }
      });
      
      return scoreMap;
    },
    enabled: fundIds.length > 0,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
}