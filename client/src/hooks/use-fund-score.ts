import { useQuery } from "@tanstack/react-query";

export function useFundScore(fundId: number) {
  return useQuery({
    queryKey: [`/api/funds/${fundId}/score`],
    enabled: !!fundId && fundId > 0,
  });
}