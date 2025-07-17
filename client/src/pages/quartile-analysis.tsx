import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useQuery } from "@tanstack/react-query";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { PieChart, Pie, BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend, LineChart, Line } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";
import { TrendingUp, TrendingDown, Target, Award, Info, Download, FileSpreadsheet } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";

export default function QuartileAnalysis() {
  const [selectedCategory, setSelectedCategory] = useState<string>("all");
  const [selectedQuartile, setSelectedQuartile] = useState<string>("1");
  
  // Get available categories for filtering
  const { data: categoriesData, isLoading: isCategoriesLoading } = useQuery({
    queryKey: [`/api/quartile/categories`],
    staleTime: 5 * 60 * 1000,
  });
  
  // Get authentic quartile distribution from fund_scores_corrected (with optional category filter)
  const { data: distributionData, isLoading: isDistributionLoading } = useQuery({
    queryKey: [`/api/quartile/distribution`, selectedCategory],
    queryFn: () => {
      const url = selectedCategory && selectedCategory !== "all"
        ? `/api/quartile/category/${encodeURIComponent(selectedCategory)}/distribution`
        : `/api/quartile/distribution`;
      return fetch(url).then(res => res.json());
    },
    staleTime: 5 * 60 * 1000,
  });
  
  // Get authentic quartile metrics from fund_scores_corrected (with optional category filter)
  const { data: metricsData, isLoading: isMetricsLoading } = useQuery({
    queryKey: [`/api/quartile/metrics`, selectedCategory],
    queryFn: () => {
      const url = selectedCategory && selectedCategory !== "all"
        ? `/api/quartile/category/${encodeURIComponent(selectedCategory)}/metrics`
        : `/api/quartile/metrics`;
      return fetch(url).then(res => res.json());
    },
    staleTime: 5 * 60 * 1000,
  });
  
  // Get sample funds by quartile from fund_scores_corrected (with optional category filter)
  const { data: fundsData, isLoading: isFundsLoading } = useQuery({
    queryKey: [`/api/quartile/funds/${selectedQuartile}`, selectedCategory],
    queryFn: () => {
      const categoryParam = selectedCategory && selectedCategory !== "all" 
        ? `?category=${encodeURIComponent(selectedCategory)}` 
        : '';
      const url = `/api/quartile/funds/${selectedQuartile}${categoryParam}`;
      return fetch(url).then(res => res.json());
    },
    staleTime: 5 * 60 * 1000,
  });

  // Colors for quartiles
  const QUARTILE_COLORS = {
    "Q1": "#22c55e", // Green - Top performers
    "Q2": "#3b82f6", // Blue - Above average  
    "Q3": "#f59e0b", // Yellow - Below average
    "Q4": "#ef4444"  // Red - Poor performers
  };

  // Prepare authentic quartile distribution data
  const getQuartileDistribution = () => {
    if (!distributionData) return [];
    
    return [
      { 
        name: "Q1 (Top 25%)", 
        value: distributionData.q1Count || 0, 
        description: "Elite Performers",
        color: QUARTILE_COLORS.Q1,
        recommendation: "STRONG_BUY / BUY"
      },
      { 
        name: "Q2 (26-50%)", 
        value: distributionData.q2Count || 0, 
        description: "Above Average",
        color: QUARTILE_COLORS.Q2,
        recommendation: "HOLD"
      },
      { 
        name: "Q3 (51-75%)", 
        value: distributionData.q3Count || 0, 
        description: "Below Average",
        color: QUARTILE_COLORS.Q3,
        recommendation: "HOLD / SELL"
      },
      { 
        name: "Q4 (Bottom 25%)", 
        value: distributionData.q4Count || 0, 
        description: "Poor Performers",
        color: QUARTILE_COLORS.Q4,
        recommendation: "SELL / STRONG_SELL"
      },
    ];
  };

  // Prepare authentic performance comparison data
  const getPerformanceData = () => {
    if (!metricsData?.returnsData) return [];
    
    return metricsData.returnsData.map(quartile => ({
      quartile: quartile.name,
      avgScore: quartile.avgScore || 0,
      return1Y: quartile.return1Y || 0,
      return3Y: quartile.return3Y || 0,
      fundCount: quartile.fundCount || 0,
      color: QUARTILE_COLORS[quartile.name as keyof typeof QUARTILE_COLORS]
    }));
  };

  // Get quartile summary stats
  const getQuartileSummary = () => {
    const distribution = getQuartileDistribution();
    const totalFunds = distribution.reduce((sum, q) => sum + q.value, 0);
    
    return {
      totalFunds,
      q1Percentage: totalFunds > 0 ? ((distribution[0]?.value || 0) / totalFunds * 100).toFixed(1) : "0",
      q2Percentage: totalFunds > 0 ? ((distribution[1]?.value || 0) / totalFunds * 100).toFixed(1) : "0",
      q3Percentage: totalFunds > 0 ? ((distribution[2]?.value || 0) / totalFunds * 100).toFixed(1) : "0",
      q4Percentage: totalFunds > 0 ? ((distribution[3]?.value || 0) / totalFunds * 100).toFixed(1) : "0",
    };
  };

  // Export to CSV function
  const exportToCSV = () => {
    if (!fundsData?.funds) return;
    
    const headers = ["Fund Name", "Category", "AMC", "Rank", "Score", "Recommendation"];
    const rows = fundsData.funds.map(fund => [
      fund.fundName,
      fund.category,
      fund.amc,
      fund.rank,
      fund.totalScore?.toFixed(2) || "N/A",
      fund.recommendation || "N/A"
    ]);
    
    const csvContent = [headers, ...rows]
      .map(row => row.join(","))
      .join("\n");
    
    const blob = new Blob([csvContent], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `quartile-${selectedQuartile}-funds.csv`;
    a.click();
  };

  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Quartile Analysis</h1>
              <p className="mt-1 text-sm text-gray-500">
                Comprehensive quartile-based fund ranking and performance analysis
              </p>
            </div>
            <HoverCard>
              <HoverCardTrigger asChild>
                <Button variant="ghost" size="icon">
                  <Info className="h-5 w-5" />
                </Button>
              </HoverCardTrigger>
              <HoverCardContent className="w-80">
                <div className="space-y-2">
                  <h4 className="text-sm font-semibold">About Quartile Analysis</h4>
                  <p className="text-sm text-gray-600">
                    Funds are ranked by their ELIVATE scores and divided into four quartiles:
                  </p>
                  <ul className="text-sm text-gray-600 space-y-1">
                    <li>• Q1: Top 25% performers (Elite)</li>
                    <li>• Q2: 26-50% (Above average)</li>
                    <li>• Q3: 51-75% (Below average)</li>
                    <li>• Q4: Bottom 25% (Poor performers)</li>
                  </ul>
                </div>
              </HoverCardContent>
            </HoverCard>
          </div>
        </div>

        {/* Summary Cards */}
        {!isDistributionLoading && distributionData && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
            {getQuartileDistribution().map((quartile, index) => (
              <Card key={quartile.name} className="border-0 shadow-sm">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div 
                      className="w-3 h-3 rounded-full" 
                      style={{ backgroundColor: quartile.color }}
                    />
                    <Badge 
                      variant="outline" 
                      className="text-xs"
                      style={{ borderColor: quartile.color, color: quartile.color }}
                    >
                      {quartile.recommendation}
                    </Badge>
                  </div>
                  <h3 className="text-lg font-semibold text-gray-900">
                    {quartile.name.split(" ")[0]}
                  </h3>
                  <p className="text-2xl font-bold text-gray-900 mt-1">
                    {quartile.value.toLocaleString()}
                  </p>
                  <p className="text-sm text-gray-500 mt-1">
                    {quartile.description}
                  </p>
                  <Progress 
                    value={getQuartileSummary().totalFunds > 0 ? (quartile.value / getQuartileSummary().totalFunds) * 100 : 0} 
                    className="mt-2 h-2"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    {getQuartileSummary()[`q${index + 1}Percentage`]}% of total
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
        
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Filter Panel */}
          <div className="lg:col-span-1">
            <Card className="shadow-sm border-0">
              <CardHeader className="pb-4">
                <CardTitle className="text-lg">Filters</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <label className="text-sm font-medium text-gray-700">Fund Category</label>
                    <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                      <SelectTrigger className="w-full mt-1">
                        <SelectValue placeholder="Select category" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="all">All Categories</SelectItem>
                        <SelectItem value="Equity">Equity</SelectItem>
                        <SelectItem value="Debt">Debt</SelectItem>
                        <SelectItem value="Hybrid">Hybrid</SelectItem>
                        <SelectItem value="Solution Oriented">Solution Oriented</SelectItem>
                        <SelectItem value="Other">Other</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  
                  <div>
                    <label className="text-sm font-medium text-gray-700">Select Quartile</label>
                    <Select value={selectedQuartile} onValueChange={setSelectedQuartile}>
                      <SelectTrigger className="w-full mt-1">
                        <SelectValue placeholder="Select quartile" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="1">Q1 (Top 25%)</SelectItem>
                        <SelectItem value="2">Q2 (26-50%)</SelectItem>
                        <SelectItem value="3">Q3 (51-75%)</SelectItem>
                        <SelectItem value="4">Q4 (Bottom 25%)</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {/* Quartile Distribution Pie Chart */}
                  {!isDistributionLoading && distributionData && (
                    <div className="mt-6 bg-gray-50 rounded-lg p-4">
                      <h3 className="text-sm font-medium text-gray-900 mb-3">Distribution</h3>
                      <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={getQuartileDistribution()}
                              cx="50%"
                              cy="50%"
                              labelLine={false}
                              label={({ value, percent }) => `${value} (${(percent * 100).toFixed(0)}%)`}
                              outerRadius={80}
                              fill="#8884d8"
                              dataKey="value"
                            >
                              {getQuartileDistribution().map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={entry.color} />
                              ))}
                            </Pie>
                            <Tooltip />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>
          
          {/* Main Content Area */}
          <div className="lg:col-span-3">
            <Tabs defaultValue="overview" className="space-y-4">
              <TabsList className="grid grid-cols-3 w-full">
                <TabsTrigger value="overview">Overview</TabsTrigger>
                <TabsTrigger value="performance">Performance</TabsTrigger>
                <TabsTrigger value="funds">Fund List</TabsTrigger>
              </TabsList>
              
              {/* Overview Tab */}
              <TabsContent value="overview" className="space-y-4">
                <Card className="shadow-sm border-0">
                  <CardHeader>
                    <CardTitle>Quartile Performance Comparison</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {isMetricsLoading ? (
                      <Skeleton className="h-96 w-full" />
                    ) : metricsData && getPerformanceData().length > 0 ? (
                      <div className="h-96">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={getPerformanceData()}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="quartile" />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Bar dataKey="avgScore" fill="#8b5cf6" name="Avg Score" />
                            <Bar dataKey="return1Y" fill="#3b82f6" name="1Y Return (%)" />
                            <Bar dataKey="return3Y" fill="#10b981" name="3Y Return (%)" />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    ) : (
                      <div className="h-96 flex items-center justify-center">
                        <p className="text-gray-500">No performance data available</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>
              
              {/* Performance Tab */}
              <TabsContent value="performance" className="space-y-4">
                <Card className="shadow-sm border-0">
                  <CardHeader>
                    <CardTitle>Return Analysis by Quartile</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {isMetricsLoading ? (
                      <Skeleton className="h-96 w-full" />
                    ) : metricsData && getPerformanceData().length > 0 ? (
                      <div className="space-y-6">
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <LineChart data={getPerformanceData()}>
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis dataKey="quartile" />
                              <YAxis />
                              <Tooltip />
                              <Legend />
                              <Line type="monotone" dataKey="return1Y" stroke="#3b82f6" name="1Y Returns (%)" strokeWidth={2} />
                              <Line type="monotone" dataKey="return3Y" stroke="#10b981" name="3Y Returns (%)" strokeWidth={2} />
                            </LineChart>
                          </ResponsiveContainer>
                        </div>
                        
                        {/* Performance Metrics Table */}
                        <div className="overflow-x-auto">
                          <table className="min-w-full divide-y divide-gray-200">
                            <thead>
                              <tr>
                                <th className="px-4 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase">Quartile</th>
                                <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">Avg Score</th>
                                <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">1Y Return</th>
                                <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">3Y Return</th>
                                <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">Fund Count</th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-200">
                              {getPerformanceData().map((data) => (
                                <tr key={data.quartile}>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm font-medium">
                                    <div className="flex items-center">
                                      <div 
                                        className="w-3 h-3 rounded-full mr-2" 
                                        style={{ backgroundColor: data.color }}
                                      />
                                      {data.quartile}
                                    </div>
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">{data.avgScore.toFixed(2)}</td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                                    <span className={data.return1Y > 0 ? "text-green-600" : "text-red-600"}>
                                      {data.return1Y > 0 ? "+" : ""}{data.return1Y.toFixed(2)}%
                                    </span>
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">
                                    <span className={data.return3Y > 0 ? "text-green-600" : "text-red-600"}>
                                      {data.return3Y > 0 ? "+" : ""}{data.return3Y.toFixed(2)}%
                                    </span>
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right">{data.fundCount.toLocaleString()}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    ) : (
                      <div className="h-96 flex items-center justify-center">
                        <p className="text-gray-500">No performance data available</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>
              
              {/* Fund List Tab */}
              <TabsContent value="funds">
                <Card className="shadow-sm border-0">
                  <CardHeader>
                    <div className="flex items-center justify-between">
                      <CardTitle>Funds in Q{selectedQuartile}</CardTitle>
                      <Button onClick={exportToCSV} variant="outline" size="sm">
                        <Download className="h-4 w-4 mr-2" />
                        Export CSV
                      </Button>
                    </div>
                  </CardHeader>
                  <CardContent>
                    {isFundsLoading ? (
                      <Skeleton className="h-96 w-full" />
                    ) : fundsData && fundsData.funds && fundsData.funds.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-gray-200">
                          <thead>
                            <tr>
                              <th className="px-4 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase">Fund Name</th>
                              <th className="px-4 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase">Category</th>
                              <th className="px-4 py-3 bg-gray-50 text-left text-xs font-medium text-gray-500 uppercase">AMC</th>
                              <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">Rank</th>
                              <th className="px-4 py-3 bg-gray-50 text-right text-xs font-medium text-gray-500 uppercase">Score</th>
                              <th className="px-4 py-3 bg-gray-50 text-center text-xs font-medium text-gray-500 uppercase">Recommendation</th>
                            </tr>
                          </thead>
                          <tbody className="bg-white divide-y divide-gray-200">
                            {fundsData.funds.map((fund) => (
                              <tr key={fund.id} className="hover:bg-gray-50">
                                <td className="px-4 py-3 text-sm font-medium text-blue-600">
                                  {fund.fundName}
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-500">
                                  {fund.category}
                                </td>
                                <td className="px-4 py-3 text-sm text-gray-500">
                                  {fund.amc}
                                </td>
                                <td className="px-4 py-3 text-sm text-right text-gray-900">
                                  #{fund.rank}
                                </td>
                                <td className="px-4 py-3 text-sm text-right font-medium">
                                  {fund.totalScore ? parseFloat(fund.totalScore).toFixed(2) : "N/A"}
                                </td>
                                <td className="px-4 py-3 text-center">
                                  <Badge 
                                    variant="outline"
                                    className={
                                      fund.recommendation === 'STRONG_BUY' || fund.recommendation === 'BUY' ? 'border-green-500 text-green-700' : 
                                      fund.recommendation === 'HOLD' ? 'border-blue-500 text-blue-700' :
                                      fund.recommendation === 'SELL' || fund.recommendation === 'STRONG_SELL' ? 'border-red-500 text-red-700' :
                                      'border-gray-500 text-gray-700'
                                    }
                                  >
                                    {fund.recommendation || "N/A"}
                                  </Badge>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="h-96 flex items-center justify-center">
                        <p className="text-gray-500">No funds available for the selected quartile and category</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>
            </Tabs>
          </div>
        </div>
      </div>
    </div>
  );
}