import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { GitBranch } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

interface PortfolioOverlapCardProps {
  fundName: string;
}

export default function PortfolioOverlapCard({ fundName }: PortfolioOverlapCardProps) {
  const { data: overlaps, isLoading } = useQuery({
    queryKey: [`/api/advisorkhoj/portfolio-overlap/search?fundName=${encodeURIComponent(fundName)}`],
    enabled: !!fundName,
    staleTime: 30 * 60 * 1000, // 30 minutes
  });

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <GitBranch className="w-5 h-5" />
            Portfolio Overlap Analysis
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <Skeleton className="h-4 w-full" />
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
          </div>
        </CardContent>
      </Card>
    );
  }

  if (!overlaps?.data || overlaps.data.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <GitBranch className="w-5 h-5" />
            Portfolio Overlap Analysis
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-gray-500 text-sm">No portfolio overlap data available for this fund.</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <GitBranch className="w-5 h-5" />
          Portfolio Overlap Analysis
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {overlaps.data.slice(0, 5).map((overlap: any, index: number) => (
            <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <div className="flex-1">
                <p className="text-sm font-medium text-gray-900 line-clamp-1">
                  {overlap.fund2Name || overlap.fund1Name}
                </p>
                <p className="text-xs text-gray-500 mt-1">Portfolio overlap</p>
              </div>
              <Badge variant={
                overlap.overlapPercentage >= 80 ? "destructive" :
                overlap.overlapPercentage >= 60 ? "default" :
                "secondary"
              }>
                {overlap.overlapPercentage}%
              </Badge>
            </div>
          ))}
        </div>
        {overlaps.data.length > 5 && (
          <p className="text-xs text-gray-500 mt-3 text-center">
            +{overlaps.data.length - 5} more funds with overlap
          </p>
        )}
      </CardContent>
    </Card>
  );
}