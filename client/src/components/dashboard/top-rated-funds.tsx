import React, { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { useQuery } from "@tanstack/react-query";

export default function TopRatedFunds() {
  const [selectedCategory, setSelectedCategory] = useState<string>("All Categories");
  
  const categoryParam = selectedCategory === "All Categories" ? "" : selectedCategory;
  const apiUrl = categoryParam ? `/api/funds/top-rated/${categoryParam}` : '/api/funds/top-rated';
  const { data: topFunds, isLoading, error } = useQuery({
    queryKey: [apiUrl],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  const handleViewFund = (fundId: number) => {
    // In a full implementation, this would navigate to a fund detail page
    console.log(`View fund: ${fundId}`);
  };
  
  return (
    <div className="px-4 sm:px-6 lg:px-8 mb-6">
      <Card>
        <CardContent className="p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-neutral-900">Top-Rated Mutual Funds</h2>
            <div className="flex items-center">
              <div className="relative">
                <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                  <SelectTrigger className="w-full pl-3 pr-10 py-2 text-sm">
                    <SelectValue placeholder="Select category" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All Categories">All Categories</SelectItem>
                    <SelectItem value="Equity: Large Cap">Equity: Large Cap</SelectItem>
                    <SelectItem value="Equity: Mid Cap">Equity: Mid Cap</SelectItem>
                    <SelectItem value="Equity: Small Cap">Equity: Small Cap</SelectItem>
                    <SelectItem value="Equity: Multi Cap">Equity: Multi Cap</SelectItem>
                    <SelectItem value="Debt: Short Duration">Debt: Short Duration</SelectItem>
                    <SelectItem value="Debt: Medium Duration">Debt: Medium Duration</SelectItem>
                    <SelectItem value="Hybrid: Balanced Advantage">Hybrid: Balanced Advantage</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button variant="outline" className="ml-3 flex items-center" size="sm">
                <span className="material-icons text-sm">filter_list</span>
                <span className="ml-1">More Filters</span>
              </Button>
            </div>
          </div>
          
          {isLoading ? (
            <div className="space-y-4">
              <Skeleton className="h-10 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
              <Skeleton className="h-16 w-full" />
            </div>
          ) : error ? (
            <div className="text-red-500">Error loading top-rated funds</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-neutral-200">
                <thead>
                  <tr>
                    <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Fund Name</th>
                    <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Category</th>
                    <th className="px-4 py-3 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Total Score</th>
                    <th className="px-4 py-3 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Returns (1Y)</th>
                    <th className="px-4 py-3 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Risk Grade</th>
                    <th className="px-4 py-3 bg-neutral-50 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">AUM (Cr)</th>
                    <th className="px-4 py-3 bg-neutral-50 text-center text-xs font-medium text-neutral-500 uppercase tracking-wider">Recommendation</th>
                    <th className="px-4 py-3 bg-neutral-50"></th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-neutral-200">
                  {Array.isArray(topFunds) && topFunds.map((fund: any) => {
                    const riskGrade = 
                      fund.riskGradeTotal >= 25 ? { text: "Low Risk", class: "bg-success bg-opacity-10 text-success" } :
                      fund.riskGradeTotal >= 20 ? { text: "Medium Risk", class: "bg-warning bg-opacity-10 text-warning" } :
                      { text: "High Risk", class: "bg-danger bg-opacity-10 text-danger" };
                    
                    return (
                      <tr key={fund.fundId} className="hover:bg-neutral-50">
                        <td className="px-4 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <div>
                              <div className="text-sm font-medium text-neutral-900">{fund.fund.fundName}</div>
                              <div className="text-xs text-neutral-500">{fund.fund.amcName}</div>
                            </div>
                          </div>
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap">
                          <div className="text-sm text-neutral-900">{fund.fund.category}</div>
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-neutral-900">{fund.totalScore.toFixed(1)}</div>
                          <div className="text-xs text-neutral-500">Quartile {fund.quartile}</div>
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-right text-sm">
                          {fund.return1y ? (
                            <span className={fund.return1y >= 0 ? 'text-success' : 'text-danger'}>
                              {fund.return1y >= 0 ? '+' : ''}{fund.return1y.toFixed(1)}%
                            </span>
                          ) : "N/A"}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-right">
                          <div className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${riskGrade.class}`}>
                            {riskGrade.text}
                          </div>
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-right text-sm text-neutral-900">
                          ₹{fund.fund.aumCr ? fund.fund.aumCr.toLocaleString('en-IN') : "N/A"}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-center">
                          <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                            fund.recommendation === "BUY" 
                              ? "bg-green-100 text-green-800" 
                              : fund.recommendation === "HOLD" 
                              ? "bg-blue-100 text-blue-800" 
                              : fund.recommendation === "REVIEW" 
                              ? "bg-yellow-100 text-yellow-800" 
                              : "bg-red-100 text-red-800"
                          }`}>
                            {fund.recommendation}
                          </span>
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-right text-sm font-medium">
                          <Button 
                            variant="link" 
                            className="text-primary-600 hover:text-primary-900"
                            onClick={() => handleViewFund(fund.fundId)}
                          >
                            View
                          </Button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
          
          <div className="mt-4 flex justify-between items-center">
            <div className="text-sm text-neutral-500">
              Showing {Array.isArray(topFunds) ? topFunds.length : 0} of {Array.isArray(topFunds) && topFunds.length > 0 ? (topFunds[0].categoryTotal || topFunds.length) : 0} funds
            </div>
            <div className="flex space-x-2">
              <Button variant="outline" size="sm">
                Previous
              </Button>
              <Button variant="outline" size="sm">
                Next
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
