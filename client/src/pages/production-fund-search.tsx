import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Search, TrendingUp, Shield, Award, Filter, Star, Target, BarChart3, Eye, ArrowUpRight, ArrowDownRight, Minus, Zap, X } from "lucide-react";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Progress } from "@/components/ui/progress";

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
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [sortBy, setSortBy] = useState("total_score");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("desc");

  // Fetch all fund scores with search and filter capabilities
  const { data: fundScores, isLoading, error } = useQuery<FundScore[]>({
    queryKey: ['/api/fund-scores/search', searchQuery, selectedSubcategory, selectedQuartile],
    enabled: true
  });

  // Fetch subcategories for filter dropdown
  const { data: subcategories } = useQuery<string[]>({
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

  const getScoreGradient = (score: number) => {
    if (score >= 80) return "from-green-500 to-green-600";
    if (score >= 60) return "from-blue-500 to-blue-600";
    if (score >= 40) return "from-yellow-500 to-yellow-600";
    return "from-red-500 to-red-600";
  };

  const getPerformanceIcon = (recommendation: string) => {
    switch (recommendation?.toUpperCase()) {
      case 'STRONG_BUY':
      case 'BUY':
        return <ArrowUpRight className="w-4 h-4 text-green-600" />;
      case 'HOLD':
        return <Minus className="w-4 h-4 text-blue-600" />;
      case 'SELL':
      case 'STRONG_SELL':
        return <ArrowDownRight className="w-4 h-4 text-red-600" />;
      default:
        return <Target className="w-4 h-4 text-gray-600" />;
    }
  };

  const resetFilters = () => {
    setSearchQuery("");
    setSelectedSubcategory("all");
    setSelectedQuartile("all");
    setSortBy("total_score");
    setSortOrder("desc");
  };

  const filterCount = [searchQuery, selectedSubcategory !== "all", selectedQuartile !== "all"].filter(Boolean).length;

  // Sort and filter funds
  const sortedFunds = React.useMemo(() => {
    if (!fundScores) return [];
    
    let filtered = [...fundScores];
    
    // Apply sorting
    filtered.sort((a, b) => {
      let aValue = a[sortBy as keyof FundScore];
      let bValue = b[sortBy as keyof FundScore];
      
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortOrder === 'asc' ? aValue - bValue : bValue - aValue;
      }
      
      return 0;
    });
    
    return filtered;
  }, [fundScores, sortBy, sortOrder]);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <div className="text-center">
          <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4" />
          <span className="text-lg">Loading fund analysis...</span>
        </div>
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
    <div className="py-6 bg-gradient-to-br from-gray-50 to-white min-h-screen">
      <div className="px-4 sm:px-6 lg:px-8">
        {/* Enhanced Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-gradient-to-r from-blue-500 to-purple-600 rounded-2xl shadow-lg">
                <Search className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className="text-4xl font-bold bg-gradient-to-r from-gray-900 to-gray-600 bg-clip-text text-transparent">
                  Production Fund Search
                </h1>
                <p className="mt-2 text-lg text-gray-600">
                  Discover top-performing mutual funds with comprehensive scoring and analysis
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-6">
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">
                  {fundScores?.length?.toLocaleString() || '0'}
                </div>
                <div className="text-sm text-gray-500">Total Funds</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">
                  {fundScores?.filter(f => f.recommendation === 'STRONG_BUY' || f.recommendation === 'BUY').length || '0'}
                </div>
                <div className="text-sm text-gray-500">Buy Rated</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">
                  {fundScores?.filter(f => f.quartile === 1).length || '0'}
                </div>
                <div className="text-sm text-gray-500">Q1 Funds</div>
              </div>
            </div>
          </div>
        </div>

        {/* Enhanced Search and Filter Section */}
        <Card className="mb-6 shadow-lg border-0 bg-white">
          <CardContent className="p-6">
            <div className="flex flex-col space-y-4">
              <div className="flex items-center space-x-4">
                <div className="flex-1 relative">
                  <Search className="absolute left-3 top-3 h-4 w-4 text-gray-400" />
                  <Input
                    placeholder="Search funds by name..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-10 h-12 text-lg border-gray-200 focus:border-blue-500"
                  />
                </div>
                <Button
                  variant="outline"
                  onClick={() => setIsFiltersOpen(!isFiltersOpen)}
                  className="h-12 px-6 border-gray-200 hover:bg-gray-50"
                >
                  <Filter className="h-4 w-4 mr-2" />
                  Filters
                  {filterCount > 0 && (
                    <Badge className="ml-2 bg-blue-100 text-blue-800">
                      {filterCount}
                    </Badge>
                  )}
                </Button>
                {filterCount > 0 && (
                  <Button
                    variant="ghost"
                    onClick={resetFilters}
                    className="h-12 px-4 text-gray-500 hover:text-gray-700"
                  >
                    <X className="h-4 w-4 mr-2" />
                    Clear
                  </Button>
                )}
              </div>

              <Collapsible open={isFiltersOpen} onOpenChange={setIsFiltersOpen}>
                <CollapsibleContent className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-4 border-t border-gray-200">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Subcategory</label>
                      <Select value={selectedSubcategory} onValueChange={setSelectedSubcategory}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select subcategory" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All Subcategories</SelectItem>
                          {subcategories?.map((subcategory) => (
                            <SelectItem key={subcategory} value={subcategory}>
                              {subcategory}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Quartile</label>
                      <Select value={selectedQuartile} onValueChange={setSelectedQuartile}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select quartile" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="all">All Quartiles</SelectItem>
                          <SelectItem value="1">Q1 (Top 25%)</SelectItem>
                          <SelectItem value="2">Q2 (26-50%)</SelectItem>
                          <SelectItem value="3">Q3 (51-75%)</SelectItem>
                          <SelectItem value="4">Q4 (Bottom 25%)</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Sort By</label>
                      <Select value={sortBy} onValueChange={setSortBy}>
                        <SelectTrigger>
                          <SelectValue placeholder="Sort by" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="total_score">Total Score</SelectItem>
                          <SelectItem value="return_1y_score">1Y Return</SelectItem>
                          <SelectItem value="return_3y_score">3Y Return</SelectItem>
                          <SelectItem value="subcategory_rank">Subcategory Rank</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </CollapsibleContent>
              </Collapsible>
            </div>
          </CardContent>
        </Card>

        {/* Enhanced Fund List */}
        <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-6">
          {sortedFunds?.map((fund) => (
            <Card key={fund.fund_id} className="hover:shadow-lg transition-shadow duration-300 border-0 bg-white">
              <CardHeader className="pb-3">
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <CardTitle className="text-lg leading-tight mb-2">
                      {fund.fund_name}
                    </CardTitle>
                    <Badge variant="outline" className="text-xs mb-2">
                      {fund.subcategory}
                    </Badge>
                  </div>
                  <div className="flex items-center space-x-2">
                    {getPerformanceIcon(fund.recommendation)}
                    <Badge className={`${getRecommendationColor(fund.recommendation)} text-xs px-2 py-1`}>
                      {fund.recommendation}
                    </Badge>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                {/* Score Display */}
                <div className="flex items-center justify-between">
                  <div className="text-center">
                    <div className="text-2xl font-bold text-gray-900">
                      {fund.total_score.toFixed(1)}
                    </div>
                    <div className="text-sm text-gray-500">Total Score</div>
                  </div>
                  <div className="text-center">
                    <Badge className={`${getQuartileColor(fund.quartile)} text-sm px-3 py-1 font-semibold`}>
                      Q{fund.quartile}
                    </Badge>
                    <div className="text-sm text-gray-500 mt-1">Quartile</div>
                  </div>
                  <div className="text-center">
                    <div className="text-lg font-semibold text-gray-900">
                      #{fund.subcategory_rank}
                    </div>
                    <div className="text-sm text-gray-500">of {fund.subcategory_total}</div>
                  </div>
                </div>

                {/* Score Breakdown */}
                <div className="space-y-2">
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-600">Historical Returns</span>
                    <span className="text-sm font-medium">{fund.historical_returns_total.toFixed(1)}</span>
                  </div>
                  <Progress value={(fund.historical_returns_total / 50) * 100} className="h-2" />
                  
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-600">Risk Grade</span>
                    <span className="text-sm font-medium">{fund.risk_grade_total.toFixed(1)}</span>
                  </div>
                  <Progress value={(fund.risk_grade_total / 30) * 100} className="h-2" />
                  
                  <div className="flex justify-between items-center">
                    <span className="text-sm text-gray-600">Fundamentals</span>
                    <span className="text-sm font-medium">{fund.fundamentals_total.toFixed(1)}</span>
                  </div>
                  <Progress value={(fund.fundamentals_total / 30) * 100} className="h-2" />
                </div>

                {/* Risk Metrics */}
                {(fund.calmar_ratio_1y || fund.sortino_ratio_1y) && (
                  <div className="grid grid-cols-2 gap-4 pt-3 border-t border-gray-100">
                    {fund.calmar_ratio_1y && (
                      <div className="text-center">
                        <div className="text-sm font-medium text-gray-900">
                          {fund.calmar_ratio_1y.toFixed(2)}
                        </div>
                        <div className="text-xs text-gray-500">Calmar Ratio</div>
                      </div>
                    )}
                    {fund.sortino_ratio_1y && (
                      <div className="text-center">
                        <div className="text-sm font-medium text-gray-900">
                          {fund.sortino_ratio_1y.toFixed(2)}
                        </div>
                        <div className="text-xs text-gray-500">Sortino Ratio</div>
                      </div>
                    )}
                  </div>
                )}

                {/* Action Button */}
                <Button
                  onClick={() => setSelectedFund(fund)}
                  className="w-full mt-4 bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700"
                >
                  <Eye className="w-4 h-4 mr-2" />
                  View Details
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>

        {/* Fund Details Modal */}
        {selectedFund && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-4xl w-full max-h-[90vh] overflow-y-auto">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <CardTitle className="text-2xl">{selectedFund.fund_name}</CardTitle>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelectedFund(null)}
                  >
                    <X className="w-4 h-4" />
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="space-y-4">
                    <div className="text-center p-4 bg-gray-50 rounded-lg">
                      <div className="text-3xl font-bold text-gray-900 mb-2">
                        {selectedFund.total_score.toFixed(1)}
                      </div>
                      <div className="text-lg text-gray-600">Total Score</div>
                    </div>
                    
                    <div className="space-y-3">
                      <div className="flex justify-between">
                        <span className="text-gray-600">Subcategory</span>
                        <span className="font-medium">{selectedFund.subcategory}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-600">Quartile</span>
                        <Badge className={getQuartileColor(selectedFund.quartile)}>
                          Q{selectedFund.quartile}
                        </Badge>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-600">Subcategory Rank</span>
                        <span className="font-medium">#{selectedFund.subcategory_rank} of {selectedFund.subcategory_total}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-600">Percentile</span>
                        <span className="font-medium">{selectedFund.subcategory_percentile.toFixed(1)}%</span>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-4">
                    <h3 className="text-lg font-semibold">Score Breakdown</h3>
                    <div className="space-y-3">
                      <div>
                        <div className="flex justify-between mb-1">
                          <span className="text-sm text-gray-600">Historical Returns</span>
                          <span className="text-sm font-medium">{selectedFund.historical_returns_total.toFixed(1)}/50</span>
                        </div>
                        <Progress value={(selectedFund.historical_returns_total / 50) * 100} className="h-2" />
                      </div>
                      <div>
                        <div className="flex justify-between mb-1">
                          <span className="text-sm text-gray-600">Risk Grade</span>
                          <span className="text-sm font-medium">{selectedFund.risk_grade_total.toFixed(1)}/30</span>
                        </div>
                        <Progress value={(selectedFund.risk_grade_total / 30) * 100} className="h-2" />
                      </div>
                      <div>
                        <div className="flex justify-between mb-1">
                          <span className="text-sm text-gray-600">Fundamentals</span>
                          <span className="text-sm font-medium">{selectedFund.fundamentals_total.toFixed(1)}/30</span>
                        </div>
                        <Progress value={(selectedFund.fundamentals_total / 30) * 100} className="h-2" />
                      </div>
                      <div>
                        <div className="flex justify-between mb-1">
                          <span className="text-sm text-gray-600">Other Metrics</span>
                          <span className="text-sm font-medium">{selectedFund.other_metrics_total.toFixed(1)}/30</span>
                        </div>
                        <Progress value={(selectedFund.other_metrics_total / 30) * 100} className="h-2" />
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}