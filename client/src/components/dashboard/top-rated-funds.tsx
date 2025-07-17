import React, { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Star, TrendingUp, TrendingDown, Filter, ExternalLink, Award, Shield } from "lucide-react";
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
      <Card className="bg-gradient-to-r from-purple-50 to-pink-50 border-0 shadow-lg">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6">
            <div className="flex items-center space-x-3">
              <div className="p-2 bg-purple-100 rounded-lg">
                <Star className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-gray-900">Top-Rated Mutual Funds</h2>
                <p className="text-sm text-gray-600">Best performing funds based on comprehensive scoring</p>
              </div>
            </div>
            <div className="flex items-center space-x-3 mt-4 sm:mt-0">
              <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                <SelectTrigger className="w-48">
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
              <Button variant="outline" size="sm">
                <Filter className="w-4 h-4 mr-2" />
                More Filters
              </Button>
            </div>
          </div>
          
          {isLoading ? (
            <div className="space-y-4">
              <Skeleton className="h-24 w-full rounded-lg" />
              <Skeleton className="h-24 w-full rounded-lg" />
              <Skeleton className="h-24 w-full rounded-lg" />
              <Skeleton className="h-24 w-full rounded-lg" />
              <Skeleton className="h-24 w-full rounded-lg" />
            </div>
          ) : error ? (
            <div className="text-center py-8">
              <Award className="w-12 h-12 mx-auto mb-2 text-gray-400" />
              <p className="text-red-500">Error loading top-rated funds</p>
            </div>
          ) : (
            <div className="space-y-4">
              {Array.isArray(topFunds) && topFunds.map((fund: any, index: number) => {
                const riskGrade = 
                  fund.riskGradeTotal >= 25 ? { text: "Low Risk", class: "bg-green-100 text-green-800", icon: Shield } :
                  fund.riskGradeTotal >= 20 ? { text: "Medium Risk", class: "bg-yellow-100 text-yellow-800", icon: Shield } :
                  { text: "High Risk", class: "bg-red-100 text-red-800", icon: Shield };
                
                return (
                  <Card key={fund.fundId} className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-all duration-300">
                    <CardContent className="p-6">
                      <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-start space-x-4">
                            <div className="flex-shrink-0">
                              <div className="w-12 h-12 bg-gradient-to-br from-purple-100 to-pink-100 rounded-lg flex items-center justify-center">
                                <span className="text-lg font-bold text-purple-600">#{index + 1}</span>
                              </div>
                            </div>
                            <div className="flex-1 min-w-0">
                              <h3 className="text-lg font-semibold text-gray-900 mb-1 truncate">{fund.fund.fundName}</h3>
                              <p className="text-sm text-gray-600 mb-2">{fund.fund.amcName}</p>
                              <div className="flex items-center space-x-2">
                                <Badge variant="outline" className="text-xs">{fund.fund.category}</Badge>
                                <Badge variant="outline" className="text-xs">Q{fund.quartile}</Badge>
                              </div>
                            </div>
                          </div>
                        </div>
                        
                        <div className="mt-4 lg:mt-0 lg:ml-6">
                          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
                            <div className="text-center">
                              <p className="text-xs font-medium text-gray-500 uppercase">Performance Score</p>
                              <p className="text-xl font-bold text-gray-900">{fund.totalScore.toFixed(1)}</p>
                            </div>
                            <div className="text-center">
                              <p className="text-xs font-medium text-gray-500 uppercase">1Y Return</p>
                              <div className="flex items-center justify-center space-x-1">
                                {fund.return1y && fund.return1y >= 0 ? (
                                  <TrendingUp className="w-4 h-4 text-green-600" />
                                ) : (
                                  <TrendingDown className="w-4 h-4 text-red-600" />
                                )}
                                <span className={`text-lg font-semibold ${fund.return1y >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                  {fund.return1y ? `${fund.return1y >= 0 ? '+' : ''}${fund.return1y.toFixed(1)}%` : "N/A"}
                                </span>
                              </div>
                            </div>
                            <div className="text-center">
                              <p className="text-xs font-medium text-gray-500 uppercase">Risk Grade</p>
                              <Badge className={`${riskGrade.class} mt-1`}>
                                <riskGrade.icon className="w-3 h-3 mr-1" />
                                {riskGrade.text}
                              </Badge>
                            </div>
                            <div className="text-center">
                              <p className="text-xs font-medium text-gray-500 uppercase">AUM</p>
                              <p className="text-lg font-semibold text-gray-900">
                                ₹{fund.fund.aumCr ? `${fund.fund.aumCr.toLocaleString('en-IN')}Cr` : "N/A"}
                              </p>
                            </div>
                          </div>
                          
                          <div className="mt-4 flex items-center justify-between">
                            <Badge 
                              className={`${
                                fund.recommendation === "BUY" 
                                  ? "bg-green-100 text-green-800" 
                                  : fund.recommendation === "HOLD" 
                                  ? "bg-blue-100 text-blue-800" 
                                  : fund.recommendation === "REVIEW" 
                                  ? "bg-yellow-100 text-yellow-800" 
                                  : "bg-red-100 text-red-800"
                              }`}
                            >
                              {fund.recommendation}
                            </Badge>
                            <Button 
                              variant="outline" 
                              size="sm"
                              onClick={() => handleViewFund(fund.fundId)}
                              className="ml-3"
                            >
                              <ExternalLink className="w-4 h-4 mr-2" />
                              View Details
                            </Button>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          )}
          
          <Card className="bg-white/60 backdrop-blur-sm border-0 shadow-sm mt-4">
            <CardContent className="p-4">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
                <div className="text-sm text-gray-600 mb-2 sm:mb-0">
                  <span className="font-medium">Showing {Array.isArray(topFunds) ? topFunds.length : 0}</span> of{" "}
                  <span className="font-medium">{Array.isArray(topFunds) && topFunds.length > 0 ? (topFunds[0].categoryTotal || topFunds.length) : 0}</span> funds
                  {selectedCategory !== "All Categories" && (
                    <Badge variant="outline" className="ml-2 text-xs">{selectedCategory}</Badge>
                  )}
                </div>
                <div className="flex items-center space-x-2">
                  <Button variant="outline" size="sm" disabled>
                    Previous
                  </Button>
                  <Button variant="outline" size="sm" disabled>
                    Next
                  </Button>
                  <Button variant="outline" size="sm">
                    <ExternalLink className="w-4 h-4 mr-2" />
                    View All
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </CardContent>
      </Card>
    </div>
  );
}
