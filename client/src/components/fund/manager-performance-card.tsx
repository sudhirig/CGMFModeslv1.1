import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { User2, TrendingUp, TrendingDown } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

interface ManagerPerformanceCardProps {
  fundManager?: string;
}

export default function ManagerPerformanceCard({ fundManager }: ManagerPerformanceCardProps) {
  const { data: managers, isLoading } = useQuery({
    queryKey: ['/api/advisorkhoj/managers'],
    staleTime: 30 * 60 * 1000, // 30 minutes
  });

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <User2 className="w-5 h-5" />
            Fund Manager Analytics
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <Skeleton className="h-24 w-full" />
          </div>
        </CardContent>
      </Card>
    );
  }

  // Find the manager data if fund manager name is provided
  const managerData = fundManager && managers?.data 
    ? managers.data.find((m: any) => m.managerName === fundManager)
    : null;

  if (!managerData && !managers?.data) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <User2 className="w-5 h-5" />
            Fund Manager Analytics
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-gray-500 text-sm">No manager analytics data available.</p>
        </CardContent>
      </Card>
    );
  }

  // Show specific manager data if available, otherwise show top managers
  const displayData = managerData ? [managerData] : (managers?.data || []).slice(0, 3);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <User2 className="w-5 h-5" />
          {managerData ? "Fund Manager Performance" : "Top Fund Managers"}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          {displayData.map((manager: any, index: number) => (
            <div key={index} className="border-b border-gray-100 last:border-0 pb-3 last:pb-0">
              <div className="flex items-start justify-between">
                <div className="flex-1">
                  <h4 className="text-sm font-medium text-gray-900">{manager.managerName}</h4>
                  <p className="text-xs text-gray-500 mt-1">
                    Managing {manager.managedFundsCount || 0} funds • AUM: ₹{manager.totalAumManaged ? `${(manager.totalAumManaged / 1000).toFixed(1)}K Cr` : 'N/A'}
                  </p>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-3 mt-3">
                <div>
                  <p className="text-xs text-gray-500">1Y Avg Return</p>
                  <div className="flex items-center gap-1">
                    {manager.avgPerformance1y >= 0 ? (
                      <TrendingUp className="w-3 h-3 text-green-600" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-600" />
                    )}
                    <span className={`text-sm font-medium ${
                      manager.avgPerformance1y >= 0 ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {manager.avgPerformance1y ? `${manager.avgPerformance1y.toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                </div>
                <div>
                  <p className="text-xs text-gray-500">3Y Avg Return</p>
                  <div className="flex items-center gap-1">
                    {manager.avgPerformance3y >= 0 ? (
                      <TrendingUp className="w-3 h-3 text-green-600" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-600" />
                    )}
                    <span className={`text-sm font-medium ${
                      manager.avgPerformance3y >= 0 ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {manager.avgPerformance3y ? `${manager.avgPerformance3y.toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
        {!managerData && managers?.data && managers.data.length > 3 && (
          <p className="text-xs text-gray-500 mt-3 text-center">
            View all {managers.data.length} fund managers
          </p>
        )}
      </CardContent>
    </Card>
  );
}