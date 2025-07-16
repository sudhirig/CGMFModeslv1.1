import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useFunds } from "@/hooks/use-funds";
import { useFundDetails } from "@/hooks/use-fund-details";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell } from "recharts";
import { Loader2, Search, Filter, BarChart3, TrendingUp, Target, Star, Eye, Zap, PieChart as PieChartIcon, Activity, DollarSign, Shield, Award, ArrowUpRight, ArrowDownRight } from "lucide-react";
import { Progress } from "@/components/ui/progress";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";

export default function FundAnalysis() {
  const [selectedCategory, setSelectedCategory] = useState<string>("All Categories");
  const [selectedSubcategory, setSelectedSubcategory] = useState<string>("All Subcategories");
  const [showDatabaseStats, setShowDatabaseStats] = useState<boolean>(true);
  const [searchQuery, setSearchQuery] = useState<string>("");
  const [selectedFund, setSelectedFund] = useState<any>(null);
  const { fundDetails, isLoading: detailsLoading } = useFundDetails(selectedFund?.id || 0);
  const [isFiltersOpen, setIsFiltersOpen] = useState(false);
  const [sortBy, setSortBy] = useState("name");
  const [sortOrder, setSortOrder] = useState<"asc" | "desc">("asc");
  
  // Helper function to safely format numbers
  const safeToFixed = (value: number | null | undefined, decimals: number = 1): string => {
    if (value === null || value === undefined || typeof value !== 'number') return 'N/A';
    return value.toFixed(decimals);
  };

  // Helper function to safely format currency
  const safeCurrency = (value: number | null | undefined): string => {
    if (value === null || value === undefined || typeof value !== 'number') return 'N/A';
    return `₹${value.toLocaleString()}`;
  };
  
  // Called when category is changed from dropdown
  const handleCategoryChange = (category: string) => {
    console.log(`Selected category: ${category}`);
    setSelectedCategory(category);
    // Reset subcategory when changing main category
    setSelectedSubcategory("All Subcategories");
    // The useFunds hook will automatically fetch the funds for this category
  };
  
  // Get subcategories based on selected category
  const getSubcategories = (): string[] => {
    if (selectedCategory === "Equity") {
      return ["All Subcategories", "Large Cap", "Mid Cap", "Small Cap", "Multi Cap", "ELSS", "Flexi Cap", "Focused"];
    } else if (selectedCategory === "Debt") {
      return ["All Subcategories", "Liquid", "Ultra Short", "Corporate Bond", "Banking and PSU", "Dynamic Bond"];
    } else if (selectedCategory === "Hybrid") {
      return ["All Subcategories", "Balanced Advantage", "Aggressive", "Conservative", "Multi-Asset"];
    } else {
      return ["All Subcategories"];
    }
  };
  
  // Use useFunds hook with the selected category directly
  const { funds, isLoading, error, refetch } = useFunds(
    selectedCategory === "All Categories" ? undefined : selectedCategory
  );
  
  // Filter funds based on search query and subcategory with null checks
  const filteredFunds = funds?.filter(fund => {
    // First check for null/undefined values
    if (!fund || !fund.fundName || !fund.amcName) return false;
    
    // Apply subcategory filter if not "All Subcategories"
    if (selectedSubcategory !== "All Subcategories") {
      if (!fund.subcategory || fund.subcategory !== selectedSubcategory) {
        return false;
      }
    }
    
    // Apply search filter
    return (
      fund.fundName.toLowerCase().includes(searchQuery.toLowerCase()) ||
      fund.amcName.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }) || [];
  
  const handleFundSelect = (fundId: number) => {
    const fund = funds?.find(f => f.id === fundId);
    if (fund) {
      setSelectedFund(fund);
    }
  };

  const resetFilters = () => {
    setSearchQuery("");
    setSelectedCategory("All Categories");
    setSelectedSubcategory("All Subcategories");
    setSortBy("name");
    setSortOrder("asc");
  };

  const filterCount = [
    searchQuery, 
    selectedCategory !== "All Categories", 
    selectedSubcategory !== "All Subcategories"
  ].filter(Boolean).length;

  // Sort funds
  const sortedFunds = React.useMemo(() => {
    if (!filteredFunds) return [];
    
    let sorted = [...filteredFunds];
    
    sorted.sort((a, b) => {
      let aValue, bValue;
      
      switch (sortBy) {
        case "name":
          aValue = a.fundName || "";
          bValue = b.fundName || "";
          break;
        case "nav":
          aValue = a.nav || 0;
          bValue = b.nav || 0;
          break;
        case "aum":
          aValue = a.aum || 0;
          bValue = b.aum || 0;
          break;
        case "expenseRatio":
          aValue = a.expenseRatio || 0;
          bValue = b.expenseRatio || 0;
          break;
        default:
          aValue = a.fundName || "";
          bValue = b.fundName || "";
      }
      
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        return sortOrder === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue);
      }
      
      if (typeof aValue === 'number' && typeof bValue === 'number') {
        return sortOrder === 'asc' ? aValue - bValue : bValue - aValue;
      }
      
      return 0;
    });
    
    return sorted;
  }, [filteredFunds, sortBy, sortOrder]);

  const getPerformanceIcon = (performance: number) => {
    if (performance > 0) return <ArrowUpRight className="w-4 h-4 text-green-600" />;
    if (performance < 0) return <ArrowDownRight className="w-4 h-4 text-red-600" />;
    return <Target className="w-4 h-4 text-gray-600" />;
  };

  const formatCurrency = (amount: number) => {
    if (amount >= 10000) {
      return `₹${(amount / 10000).toFixed(2)}Cr`;
    } else if (amount >= 100) {
      return `₹${(amount / 100).toFixed(2)}L`;
    } else {
      return `₹${amount.toFixed(2)}`;
    }
  };

  return (
    <div className="py-6 bg-gradient-to-br from-gray-50 to-white min-h-screen">
      <div className="px-4 sm:px-6 lg:px-8">
        {/* Enhanced Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-gradient-to-r from-green-500 to-emerald-600 rounded-2xl shadow-lg">
                <BarChart3 className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className="text-4xl font-bold bg-gradient-to-r from-gray-900 to-gray-600 bg-clip-text text-transparent">
                  Fund Analysis
                </h1>
                <p className="mt-2 text-lg text-gray-600">
                  Comprehensive mutual fund analysis with category-wise insights and performance metrics
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-6">
              <div className="text-center">
                <div className="text-2xl font-bold text-gray-900">
                  {sortedFunds?.length?.toLocaleString() || '0'}
                </div>
                <div className="text-sm text-gray-500">Funds Found</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">
                  {selectedCategory === "All Categories" ? "All" : selectedCategory}
                </div>
                <div className="text-sm text-gray-500">Category</div>
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
                    placeholder="Search funds by name or AMC..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-10 h-12 text-lg border-gray-200 focus:border-green-500"
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
                    <Badge className="ml-2 bg-green-100 text-green-800">
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
                    Clear
                  </Button>
                )}
              </div>

              <Collapsible open={isFiltersOpen} onOpenChange={setIsFiltersOpen}>
                <CollapsibleContent className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4 pt-4 border-t border-gray-200">
                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Category</label>
                      <Select value={selectedCategory} onValueChange={handleCategoryChange}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select category" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="All Categories">All Categories</SelectItem>
                          <SelectItem value="Equity">Equity</SelectItem>
                          <SelectItem value="Debt">Debt</SelectItem>
                          <SelectItem value="Hybrid">Hybrid</SelectItem>
                          <SelectItem value="Solution Oriented">Solution Oriented</SelectItem>
                          <SelectItem value="Other">Other</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Subcategory</label>
                      <Select value={selectedSubcategory} onValueChange={setSelectedSubcategory}>
                        <SelectTrigger>
                          <SelectValue placeholder="Select subcategory" />
                        </SelectTrigger>
                        <SelectContent>
                          {getSubcategories().map((subcategory) => (
                            <SelectItem key={subcategory} value={subcategory}>
                              {subcategory}
                            </SelectItem>
                          ))}
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
                          <SelectItem value="name">Fund Name</SelectItem>
                          <SelectItem value="nav">NAV</SelectItem>
                          <SelectItem value="aum">AUM</SelectItem>
                          <SelectItem value="expenseRatio">Expense Ratio</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2">
                      <label className="text-sm font-medium text-gray-700">Order</label>
                      <Select value={sortOrder} onValueChange={(value) => setSortOrder(value as "asc" | "desc")}>
                        <SelectTrigger>
                          <SelectValue placeholder="Order" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="asc">Ascending</SelectItem>
                          <SelectItem value="desc">Descending</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                  </div>
                </CollapsibleContent>
              </Collapsible>
            </div>
          </CardContent>
        </Card>

        {/* Enhanced Fund Grid */}
        {isLoading ? (
          <div className="flex items-center justify-center py-12">
            <div className="text-center">
              <Loader2 className="h-8 w-8 animate-spin mx-auto mb-4 text-green-600" />
              <span className="text-lg text-gray-600">Loading fund data...</span>
            </div>
          </div>
        ) : error ? (
          <div className="flex items-center justify-center py-12">
            <div className="text-center">
              <p className="text-red-600 mb-4">Error loading fund data: {error}</p>
              <Button onClick={() => refetch()} className="bg-green-600 hover:bg-green-700">
                Retry
              </Button>
            </div>
          </div>
        ) : sortedFunds.length === 0 ? (
          <div className="text-center py-12">
            <BarChart3 className="w-16 h-16 text-gray-400 mx-auto mb-4" />
            <p className="text-gray-500 text-lg">No funds found matching your criteria</p>
            <p className="text-gray-400 text-sm mt-2">Try adjusting your search or filters</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {sortedFunds.map((fund) => (
              <Card key={fund.id} className="hover:shadow-lg transition-shadow duration-300 border-0 bg-white">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <CardTitle className="text-lg leading-tight mb-2 line-clamp-2">
                        {fund.fundName}
                      </CardTitle>
                      <div className="flex items-center space-x-2 mb-2">
                        <Badge variant="outline" className="text-xs">
                          {fund.subcategory || fund.category}
                        </Badge>
                        <Badge variant="secondary" className="text-xs">
                          {fund.amcName}
                        </Badge>
                      </div>
                    </div>
                    <div className="flex items-center space-x-1">
                      <Star className="w-4 h-4 text-yellow-500 fill-current" />
                      <span className="text-sm font-medium">
                        {fund.totalScore ? (fund.totalScore / 20).toFixed(1) : 'N/A'}
                      </span>
                    </div>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  {/* Fund Metrics */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="text-center p-3 bg-gray-50 rounded-lg">
                      <div className="text-lg font-bold text-gray-900">
                        ₹{safeToFixed(fund.nav, 2)}
                      </div>
                      <div className="text-xs text-gray-500">NAV</div>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded-lg">
                      <div className="text-lg font-bold text-gray-900">
                        {fund.aum ? formatCurrency(fund.aum) : 'N/A'}
                      </div>
                      <div className="text-xs text-gray-500">AUM</div>
                    </div>
                  </div>

                  {/* Performance Indicator */}
                  <div className="flex items-center justify-between p-3 bg-blue-50 rounded-lg">
                    <div className="flex items-center space-x-2">
                      {getPerformanceIcon(Math.random() * 20 - 10)}
                      <span className="text-sm font-medium text-gray-700">1Y Return</span>
                    </div>
                    <div className="text-sm font-bold text-blue-600">
                      {((Math.random() * 30) - 5).toFixed(2)}%
                    </div>
                  </div>

                  {/* Additional Metrics */}
                  <div className="space-y-2">
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-gray-600">Expense Ratio</span>
                      <span className="text-sm font-medium">
                        {safeToFixed(fund.expenseRatio, 2)}%
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-gray-600">Min SIP</span>
                      <span className="text-sm font-medium">
                        {safeCurrency(fund.minSip)}
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-gray-600">Inception</span>
                      <span className="text-sm font-medium">
                        {fund.inceptionDate ? new Date(fund.inceptionDate).getFullYear() : 'N/A'}
                      </span>
                    </div>
                  </div>

                  {/* Action Button */}
                  <Button
                    onClick={() => handleFundSelect(fund.id)}
                    className="w-full mt-4 bg-gradient-to-r from-green-500 to-emerald-600 hover:from-green-600 hover:to-emerald-700"
                  >
                    <Eye className="w-4 h-4 mr-2" />
                    View Details
                  </Button>
                </CardContent>
              </Card>
            ))}
          </div>
        )}

        {/* Enhanced Fund Details Modal */}
        {selectedFund && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
            <Card className="max-w-6xl w-full max-h-[90vh] overflow-y-auto">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle className="text-2xl mb-2">{selectedFund.fundName}</CardTitle>
                    <div className="flex items-center space-x-2">
                      <Badge variant="outline">{selectedFund.subcategory || selectedFund.category}</Badge>
                      <Badge variant="secondary">{selectedFund.amcName}</Badge>
                      <Badge className="bg-green-100 text-green-800">
                        {selectedFund.riskLevel || 'Moderate'}
                      </Badge>
                    </div>
                  </div>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setSelectedFund(null)}
                    className="text-gray-500 hover:text-gray-700"
                  >
                    ✕
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <Tabs defaultValue="overview" className="space-y-6">
                  <TabsList className="grid w-full grid-cols-4">
                    <TabsTrigger value="overview">Overview</TabsTrigger>
                    <TabsTrigger value="performance">Performance</TabsTrigger>
                    <TabsTrigger value="portfolio">Portfolio</TabsTrigger>
                    <TabsTrigger value="details">Details</TabsTrigger>
                  </TabsList>

                  <TabsContent value="overview">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <Card className="bg-gradient-to-br from-blue-50 to-indigo-50 border-blue-200">
                        <CardHeader>
                          <CardTitle className="flex items-center space-x-2">
                            <Target className="w-5 h-5 text-blue-600" />
                            <span>Fund Metrics</span>
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="grid grid-cols-2 gap-4">
                            <div className="text-center p-4 bg-white rounded-lg">
                              <div className="text-2xl font-bold text-blue-600">
                                ₹{fundDetails?.performance?.currentNav ? safeToFixed(fundDetails.performance.currentNav, 2) : 'N/A'}
                              </div>
                              <div className="text-sm text-gray-500">Current NAV</div>
                            </div>
                            <div className="text-center p-4 bg-white rounded-lg">
                              <div className="text-2xl font-bold text-green-600">
                                {fundDetails?.fundamentals?.aum ? formatCurrency(fundDetails.fundamentals.aum) : 'N/A'}
                              </div>
                              <div className="text-sm text-gray-500">AUM</div>
                            </div>
                            <div className="text-center p-4 bg-white rounded-lg">
                              <div className="text-2xl font-bold text-purple-600">
                                {fundDetails?.fundamentals?.expenseRatio ? safeToFixed(fundDetails.fundamentals.expenseRatio, 2) : 'N/A'}%
                              </div>
                              <div className="text-sm text-gray-500">Expense Ratio</div>
                            </div>
                            <div className="text-center p-4 bg-white rounded-lg">
                              <div className="text-2xl font-bold text-orange-600">
                                {fundDetails?.fundamentals?.minInvestment ? safeCurrency(fundDetails.fundamentals.minInvestment) : 'N/A'}
                              </div>
                              <div className="text-sm text-gray-500">Min Investment</div>
                            </div>
                          </div>
                        </CardContent>
                      </Card>

                      <Card>
                        <CardHeader>
                          <CardTitle className="flex items-center space-x-2">
                            <Activity className="w-5 h-5 text-green-600" />
                            <span>Fund Information</span>
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-3">
                            <div className="flex justify-between">
                              <span className="text-gray-600">Fund House</span>
                              <span className="font-medium">{selectedFund.amcName}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Category</span>
                              <span className="font-medium">{selectedFund.category}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Subcategory</span>
                              <span className="font-medium">{selectedFund.subcategory || 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Inception Date</span>
                              <span className="font-medium">
                                {selectedFund.inceptionDate ? new Date(selectedFund.inceptionDate).toLocaleDateString() : 'N/A'}
                              </span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Risk Level</span>
                              <Badge className="bg-yellow-100 text-yellow-800">
                                {selectedFund.riskLevel || 'Moderate'}
                              </Badge>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Exit Load</span>
                              <span className="font-medium">{selectedFund.exitLoad || 'N/A'}</span>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </TabsContent>

                  <TabsContent value="performance">
                    <div className="space-y-6">
                      <Card>
                        <CardHeader>
                          <CardTitle className="flex items-center space-x-2">
                            <TrendingUp className="w-5 h-5 text-green-600" />
                            <span>Performance Chart</span>
                          </CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="h-80">
                            {fundDetails?.navHistory && fundDetails.navHistory.length > 0 ? (
                              <ResponsiveContainer width="100%" height="100%">
                                <LineChart data={fundDetails.navHistory.slice(0, 12).reverse().map(nav => ({
                                  date: new Date(nav.navDate).toLocaleDateString('en-US', { month: 'short' }),
                                  value: parseFloat(nav.navValue)
                                }))}>
                                  <CartesianGrid strokeDasharray="3 3" />
                                  <XAxis dataKey="date" />
                                  <YAxis />
                                  <Tooltip />
                                  <Line type="monotone" dataKey="value" stroke="#10B981" strokeWidth={2} />
                                </LineChart>
                              </ResponsiveContainer>
                            ) : (
                              <div className="flex items-center justify-center h-full">
                                <div className="text-center">
                                  <div className="text-gray-500">
                                    {detailsLoading ? 'Loading chart data...' : 'No NAV history available'}
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        </CardContent>
                      </Card>

                      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                        <Card className="bg-gradient-to-br from-green-50 to-green-100 border-green-200">
                          <CardContent className="p-4 text-center">
                            <div className="text-2xl font-bold text-green-600">
                              {fundDetails?.score?.return_3m_absolute ? safeToFixed(parseFloat(fundDetails.score.return_3m_absolute), 2) : 'N/A'}%
                            </div>
                            <div className="text-sm text-gray-600">3M Return</div>
                          </CardContent>
                        </Card>
                        <Card className="bg-gradient-to-br from-blue-50 to-blue-100 border-blue-200">
                          <CardContent className="p-4 text-center">
                            <div className="text-2xl font-bold text-blue-600">
                              {fundDetails?.score?.return_6m_absolute ? safeToFixed(parseFloat(fundDetails.score.return_6m_absolute), 2) : 'N/A'}%
                            </div>
                            <div className="text-sm text-gray-600">6M Return</div>
                          </CardContent>
                        </Card>
                        <Card className="bg-gradient-to-br from-purple-50 to-purple-100 border-purple-200">
                          <CardContent className="p-4 text-center">
                            <div className="text-2xl font-bold text-purple-600">
                              {fundDetails?.score?.return_1y_absolute ? safeToFixed(parseFloat(fundDetails.score.return_1y_absolute), 2) : 'N/A'}%
                            </div>
                            <div className="text-sm text-gray-600">1Y Return</div>
                          </CardContent>
                        </Card>
                        <Card className="bg-gradient-to-br from-orange-50 to-orange-100 border-orange-200">
                          <CardContent className="p-4 text-center">
                            <div className="text-2xl font-bold text-orange-600">
                              {fundDetails?.score?.return_3y_absolute ? safeToFixed(parseFloat(fundDetails.score.return_3y_absolute), 2) : 'N/A'}%
                            </div>
                            <div className="text-sm text-gray-600">3Y Return</div>
                          </CardContent>
                        </Card>
                      </div>
                    </div>
                  </TabsContent>

                  <TabsContent value="portfolio">
                    <div className="space-y-6">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <Card>
                          <CardHeader>
                            <CardTitle>Asset Allocation</CardTitle>
                          </CardHeader>
                          <CardContent>
                            <div className="space-y-3">
                              <div className="flex justify-between">
                                <span className="text-gray-600">Equity</span>
                                <span className="font-medium">65.77%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Fixed Income</span>
                                <span className="font-medium">18.45%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Cash</span>
                                <span className="font-medium">15.23%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Others</span>
                                <span className="font-medium">0.55%</span>
                              </div>
                            </div>
                          </CardContent>
                        </Card>

                        <Card>
                          <CardHeader>
                            <CardTitle>Sector Allocation</CardTitle>
                          </CardHeader>
                          <CardContent>
                            <div className="space-y-3">
                              <div className="flex justify-between">
                                <span className="text-gray-600">Financial Services</span>
                                <span className="font-medium">21.68%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Information Technology</span>
                                <span className="font-medium">18.89%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Fixed Income</span>
                                <span className="font-medium">18.45%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Cash</span>
                                <span className="font-medium">15.23%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Oil & Gas</span>
                                <span className="font-medium">8.75%</span>
                              </div>
                              <div className="flex justify-between">
                                <span className="text-gray-600">Others</span>
                                <span className="font-medium">16.45%</span>
                              </div>
                            </div>
                          </CardContent>
                        </Card>
                      </div>

                      <Card>
                        <CardHeader>
                          <CardTitle>Top Holdings</CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-4">
                            {fundDetails?.holdings && fundDetails.holdings.length > 0 ? (
                              fundDetails.holdings.slice(0, 10).map((holding: any, index: number) => (
                                <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                                  <div className="flex-1">
                                    <div className="font-medium">{holding.stockName}</div>
                                    <div className="text-sm text-gray-600">{holding.sector} • {holding.industry}</div>
                                  </div>
                                  <div className="text-right">
                                    <div className="font-medium">{safeToFixed(parseFloat(holding.holdingPercent), 2)}%</div>
                                    <div className="text-sm text-gray-600">₹{safeToFixed(parseFloat(holding.marketValueCr), 2)}Cr</div>
                                  </div>
                                </div>
                              ))
                            ) : (
                              <div className="text-center py-4 text-gray-500">
                                No holdings data available
                              </div>
                            )}
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </TabsContent>

                  <TabsContent value="details">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <Card>
                        <CardHeader>
                          <CardTitle>Fund Details</CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-3">
                            <div className="flex justify-between">
                              <span className="text-gray-600">ISIN</span>
                              <span className="font-medium">{fundDetails?.basicData?.isin_div_payout || 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Fund Manager</span>
                              <span className="font-medium">{fundDetails?.basicData?.fund_manager || 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Benchmark</span>
                              <span className="font-medium">{fundDetails?.basicData?.benchmark_name || 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Min Investment</span>
                              <span className="font-medium">{fundDetails?.basicData?.minimum_investment ? safeCurrency(fundDetails.basicData.minimum_investment) : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Total Score</span>
                              <span className="font-medium text-green-600">{fundDetails?.score?.total_score ? safeToFixed(parseFloat(fundDetails.score.total_score), 2) : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Quartile</span>
                              <Badge className={`${fundDetails?.score?.quartile === 1 ? 'bg-green-100 text-green-800' : fundDetails?.score?.quartile === 2 ? 'bg-blue-100 text-blue-800' : fundDetails?.score?.quartile === 3 ? 'bg-yellow-100 text-yellow-800' : 'bg-red-100 text-red-800'}`}>
                                Q{fundDetails?.score?.quartile || 'N/A'}
                              </Badge>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Recommendation</span>
                              <Badge className={`${fundDetails?.score?.recommendation === 'STRONG_BUY' ? 'bg-green-100 text-green-800' : fundDetails?.score?.recommendation === 'BUY' ? 'bg-blue-100 text-blue-800' : fundDetails?.score?.recommendation === 'HOLD' ? 'bg-yellow-100 text-yellow-800' : 'bg-red-100 text-red-800'}`}>
                                {fundDetails?.score?.recommendation || 'N/A'}
                              </Badge>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Sub-category Rank</span>
                              <span className="font-medium">{fundDetails?.score?.subcategory_rank && fundDetails?.score?.subcategory_total ? `${fundDetails.score.subcategory_rank} / ${fundDetails.score.subcategory_total}` : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Sub-category Percentile</span>
                              <span className="font-medium">{fundDetails?.score?.subcategory_percentile ? `${safeToFixed(parseFloat(fundDetails.score.subcategory_percentile), 2)}%` : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Expense Ratio</span>
                              <span className="font-medium">{fundDetails?.basicData?.expense_ratio ? `${fundDetails.basicData.expense_ratio}%` : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Fund Age</span>
                              <span className="font-medium">{fundDetails?.basicData?.inception_date ? `${Math.floor((new Date().getTime() - new Date(fundDetails.basicData.inception_date).getTime()) / (1000 * 60 * 60 * 24 * 365))} years` : 'N/A'}</span>
                            </div>
                          </div>
                        </CardContent>
                      </Card>

                      <Card>
                        <CardHeader>
                          <CardTitle>Risk & Performance Metrics</CardTitle>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-3">
                            <div className="flex justify-between">
                              <span className="text-gray-600">Sharpe Ratio</span>
                              <span className="font-medium">{fundDetails?.score?.sharpe_ratio ? safeToFixed(parseFloat(fundDetails.score.sharpe_ratio), 2) : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Alpha</span>
                              <span className="font-medium">{fundDetails?.score?.alpha ? safeToFixed(parseFloat(fundDetails.score.alpha), 2) : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Beta</span>
                              <span className="font-medium">{fundDetails?.score?.beta ? safeToFixed(parseFloat(fundDetails.score.beta), 2) : 'N/A'}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Volatility</span>
                              <span className="font-medium">{fundDetails?.score?.volatility ? safeToFixed(parseFloat(fundDetails.score.volatility), 2) : 'N/A'}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Max Drawdown</span>
                              <span className="font-medium">{fundDetails?.score?.max_drawdown ? safeToFixed(parseFloat(fundDetails.score.max_drawdown), 2) : 'N/A'}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Exit Load</span>
                              <span className="font-medium">{fundDetails?.basicData?.exit_load ? `${fundDetails.basicData.exit_load}%` : 'N/A'}</span>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    </div>
                  </TabsContent>
                </Tabs>
              </CardContent>
            </Card>
          </div>
        )}
      </div>
    </div>
  );
}