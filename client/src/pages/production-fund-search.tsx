import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Search, TrendingUp, Shield, Award, Filter } from "lucide-react";

interface FundScore {
  fund_id: number;
  fund_name: string;
  subcategory: string;
  total_score: number;
  quartile: number;
  subcategory_rank: number;
  subcategory_total: number;
  subcategory_percentile: number;
  historical_returns_total: number;
  risk_grade_total: number;
  fundamentals_total: number;
  other_metrics_total: number;
  return_1y_score: number;
  return_3y_score: number;
  return_5y_score: number;
  calmar_ratio_1y: number;
  sortino_ratio_1y: number;
  recommendation: string;
}

export default function ProductionFundSearch() {
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedSubcategory, setSelectedSubcategory] = useState("all");
  const [selectedQuartile, setSelectedQuartile] = useState("all");
  const [selectedFund, setSelectedFund] = useState<FundScore | null>(null);

  // Fetch all fund scores with search and filter capabilities
  const { data: fundScores, isLoading, error } = useQuery({
    queryKey: ['/api/fund-scores/search', searchQuery, selectedSubcategory, selectedQuartile],
    enabled: true
  });

  // Fetch subcategories for filter dropdown
  const { data: subcategories } = useQuery({
    queryKey: ['/api/fund-scores/subcategories'],
    enabled: true
  });

  const getQuartileColor = (quartile: number) => {
    switch (quartile) {
      case 1: return "bg-green-100 text-green-800 border-green-200";
      case 2: return "bg-blue-100 text-blue-800 border-blue-200";
      case 3: return "bg-yellow-100 text-yellow-800 border-yellow-200";
      case 4: return "bg-red-100 text-red-800 border-red-200";
      default: return "bg-gray-100 text-gray-800 border-gray-200";
    }
  };

  const getRecommendationColor = (recommendation: string) => {
    switch (recommendation?.toUpperCase()) {
      case 'STRONG_BUY': return "bg-green-600 text-white";
      case 'BUY': return "bg-green-500 text-white";
      case 'HOLD': return "bg-yellow-500 text-white";
      case 'SELL': return "bg-red-500 text-white";
      case 'STRONG_SELL': return "bg-red-600 text-white";
      default: return "bg-gray-500 text-white";
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <Loader2 className="h-8 w-8 animate-spin" />
        <span className="ml-2">Loading fund analysis...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <p className="text-red-600 mb-4">Error loading fund data</p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </div>
    );
  }

  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-neutral-900">Fund Analysis Platform</h1>
          <p className="mt-2 text-neutral-600">
            Comprehensive analysis of {fundScores?.length || 0} mutual funds with authentic scoring methodology
          </p>
        </div>

        {/* Search and Filter Bar */}
        <Card className="mb-6">
          <CardContent className="pt-6">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="relative">
                <Search className="absolute left-3 top-3 h-4 w-4 text-gray-400" />
                <Input
                  placeholder="Search funds or AMC..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-10"
                />
              </div>
              
              <Select value={selectedSubcategory} onValueChange={setSelectedSubcategory}>
                <SelectTrigger>
                  <SelectValue placeholder="Category" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Categories</SelectItem>
                  {subcategories?.map((sub: string) => (
                    <SelectItem key={sub} value={sub}>{sub}</SelectItem>
                  ))}
                </SelectContent>
              </Select>

              <Select value={selectedQuartile} onValueChange={setSelectedQuartile}>
                <SelectTrigger>
                  <SelectValue placeholder="Performance" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Performance</SelectItem>
                  <SelectItem value="1">Top Quartile (Q1)</SelectItem>
                  <SelectItem value="2">Second Quartile (Q2)</SelectItem>
                  <SelectItem value="3">Third Quartile (Q3)</SelectItem>
                  <SelectItem value="4">Bottom Quartile (Q4)</SelectItem>
                </SelectContent>
              </Select>

              <Button onClick={() => {
                setSearchQuery("");
                setSelectedSubcategory("all");
                setSelectedQuartile("all");
              }}>
                <Filter className="h-4 w-4 mr-2" />
                Clear Filters
              </Button>
            </div>
          </CardContent>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Fund List */}
          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <TrendingUp className="h-5 w-5 mr-2" />
                  Fund Rankings
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-3 max-h-96 overflow-y-auto">
                  {fundScores?.map((fund: FundScore) => (
                    <div
                      key={fund.fund_id}
                      className={`p-4 rounded-lg border cursor-pointer transition-colors ${
                        selectedFund?.fund_id === fund.fund_id
                          ? 'border-blue-500 bg-blue-50'
                          : 'border-gray-200 hover:border-gray-300'
                      }`}
                      onClick={() => setSelectedFund(fund)}
                    >
                      <div className="flex justify-between items-start mb-2">
                        <h3 className="font-semibold text-gray-900 line-clamp-2">
                          {fund.fund_name}
                        </h3>
                        <Badge className={getQuartileColor(fund.quartile)}>
                          Q{fund.quartile}
                        </Badge>
                      </div>
                      
                      <div className="flex justify-between items-center mb-2">
                        <span className="text-sm text-gray-600">{fund.subcategory}</span>
                        <span className="font-bold text-lg">{fund.total_score}/100</span>
                      </div>
                      
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-gray-500">
                          Rank {fund.subcategory_rank}/{fund.subcategory_total}
                        </span>
                        {fund.recommendation && (
                          <Badge className={getRecommendationColor(fund.recommendation)}>
                            {fund.recommendation.replace('_', ' ')}
                          </Badge>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Fund Details */}
          <div>
            {selectedFund ? (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center">
                    <Award className="h-5 w-5 mr-2" />
                    Fund Analysis
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <Tabs defaultValue="overview" className="w-full">
                    <TabsList className="grid w-full grid-cols-2">
                      <TabsTrigger value="overview">Overview</TabsTrigger>
                      <TabsTrigger value="details">Details</TabsTrigger>
                    </TabsList>
                    
                    <TabsContent value="overview" className="space-y-4">
                      <div>
                        <h3 className="font-semibold text-lg mb-2 line-clamp-3">
                          {selectedFund.fund_name}
                        </h3>
                        <p className="text-sm text-gray-600 mb-4">{selectedFund.subcategory}</p>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div className="text-center p-3 bg-gray-50 rounded">
                          <div className="text-2xl font-bold text-blue-600">
                            {selectedFund.total_score}
                          </div>
                          <div className="text-xs text-gray-600">Total Score</div>
                        </div>
                        <div className="text-center p-3 bg-gray-50 rounded">
                          <div className="text-2xl font-bold text-green-600">
                            Q{selectedFund.quartile}
                          </div>
                          <div className="text-xs text-gray-600">Quartile</div>
                        </div>
                      </div>

                      <div className="space-y-2">
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Category Rank:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.subcategory_rank}/{selectedFund.subcategory_total}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Percentile:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.subcategory_percentile?.toFixed(1)}%
                          </span>
                        </div>
                      </div>

                      {selectedFund.recommendation && (
                        <div className="mt-4">
                          <Badge className={`${getRecommendationColor(selectedFund.recommendation)} w-full justify-center py-2`}>
                            {selectedFund.recommendation.replace('_', ' ')}
                          </Badge>
                        </div>
                      )}
                    </TabsContent>
                    
                    <TabsContent value="details" className="space-y-4">
                      <div className="space-y-3">
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Returns Score:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.historical_returns_total}/40
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Risk Score:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.risk_grade_total}/30
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Fundamentals:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.fundamentals_total}/30
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-sm text-gray-600">Other Metrics:</span>
                          <span className="text-sm font-medium">
                            {selectedFund.other_metrics_total}/30
                          </span>
                        </div>
                      </div>

                      {(selectedFund.calmar_ratio_1y || selectedFund.sortino_ratio_1y) && (
                        <div className="border-t pt-3">
                          <h4 className="font-medium text-sm mb-2">Risk Analytics</h4>
                          <div className="space-y-2">
                            {selectedFund.calmar_ratio_1y && (
                              <div className="flex justify-between">
                                <span className="text-sm text-gray-600">Calmar Ratio:</span>
                                <span className="text-sm font-medium">
                                  {selectedFund.calmar_ratio_1y.toFixed(2)}
                                </span>
                              </div>
                            )}
                            {selectedFund.sortino_ratio_1y && (
                              <div className="flex justify-between">
                                <span className="text-sm text-gray-600">Sortino Ratio:</span>
                                <span className="text-sm font-medium">
                                  {selectedFund.sortino_ratio_1y.toFixed(2)}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>
                      )}
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardContent className="flex items-center justify-center h-64">
                  <div className="text-center text-gray-500">
                    <Shield className="h-12 w-12 mx-auto mb-4 opacity-50" />
                    <p>Select a fund to view detailed analysis</p>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}