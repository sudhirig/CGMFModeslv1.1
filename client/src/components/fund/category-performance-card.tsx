import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { BarChart3, TrendingUp, TrendingDown } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

interface CategoryPerformanceCardProps {
  category?: string;
  subcategory?: string;
}

export default function CategoryPerformanceCard({ category, subcategory }: CategoryPerformanceCardProps) {
  const { data: categoryData, isLoading } = useQuery({
    queryKey: ['/api/advisorkhoj/category-performance'],
    staleTime: 30 * 60 * 1000, // 30 minutes
  });

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="w-5 h-5" />
            Category Performance
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

  // Find matching category data
  const matchingData = categoryData?.data?.filter((item: any) => {
    if (subcategory && item.subcategory) {
      return item.subcategory === subcategory;
    }
    if (category && item.categoryName) {
      return item.categoryName === category;
    }
    return false;
  }) || [];

  // If no matching data, show top performing categories
  const displayData = matchingData.length > 0 ? matchingData : (categoryData?.data || []).slice(0, 5);

  if (!displayData || displayData.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <BarChart3 className="w-5 h-5" />
            Category Performance
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-gray-500 text-sm">No category performance data available.</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <BarChart3 className="w-5 h-5" />
          {matchingData.length > 0 ? "Category Performance" : "Top Performing Categories"}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {displayData.slice(0, 5).map((cat: any, index: number) => (
            <div key={index} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
              <div className="flex-1">
                <h4 className="text-sm font-medium text-gray-900">
                  {cat.subcategory || cat.categoryName}
                </h4>
                <p className="text-xs text-gray-500 mt-1">
                  {cat.fundCount} funds in category
                </p>
              </div>
              <div className="flex items-center gap-4">
                <div className="text-right">
                  <p className="text-xs text-gray-500">1Y Return</p>
                  <div className="flex items-center gap-1">
                    {cat.avgReturn1y >= 0 ? (
                      <TrendingUp className="w-3 h-3 text-green-600" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-600" />
                    )}
                    <span className={`text-sm font-medium ${
                      cat.avgReturn1y >= 0 ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {cat.avgReturn1y ? `${cat.avgReturn1y.toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-xs text-gray-500">3Y Return</p>
                  <div className="flex items-center gap-1">
                    {cat.avgReturn3y >= 0 ? (
                      <TrendingUp className="w-3 h-3 text-green-600" />
                    ) : (
                      <TrendingDown className="w-3 h-3 text-red-600" />
                    )}
                    <span className={`text-sm font-medium ${
                      cat.avgReturn3y >= 0 ? 'text-green-600' : 'text-red-600'
                    }`}>
                      {cat.avgReturn3y ? `${cat.avgReturn3y.toFixed(1)}%` : 'N/A'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
        {matchingData.length === 0 && categoryData?.data && categoryData.data.length > 5 && (
          <p className="text-xs text-gray-500 mt-3 text-center">
            View all {categoryData.data.length} categories
          </p>
        )}
      </CardContent>
    </Card>
  );
}