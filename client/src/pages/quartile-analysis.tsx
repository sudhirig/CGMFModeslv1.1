import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useQuery } from "@tanstack/react-query";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PieChart, Pie, BarChart, Bar, Cell, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

export default function QuartileAnalysis() {
  const [selectedCategory, setSelectedCategory] = useState<string>("All Categories");
  const [selectedQuartile, setSelectedQuartile] = useState<string>("1");
  
  const categoryParam = selectedCategory === "All Categories" ? "" : selectedCategory;
  
  // Get quartile distribution data
  const { data: distributionData, isLoading: isDistributionLoading } = useQuery({
    queryKey: [`/api/quartile/distribution`, categoryParam],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  // Get quartile metrics
  const { data: metricsData, isLoading: isMetricsLoading } = useQuery({
    queryKey: [`/api/quartile/metrics`],
    staleTime: 5 * 60 * 1000,
  });
  
  // Get funds by selected quartile
  const { data: fundsData, isLoading: isFundsLoading } = useQuery({
    queryKey: [`/api/quartile/funds/${selectedQuartile}`, categoryParam],
    staleTime: 5 * 60 * 1000,
  });
  
  // Colors for quartiles
  const COLORS = ["#22c55e", "#3b82f6", "#f59e0b", "#ef4444"];
  
  // Prepare data for pie chart
  const getPieData = () => {
    if (!distributionData) return [];
    
    return [
      { name: "Q1 (Top 25%)", value: distributionData.q1Count, description: "Elite funds - BUY" },
      { name: "Q2 (26-50%)", value: distributionData.q2Count, description: "Quality funds - HOLD/REVIEW" },
      { name: "Q3 (51-75%)", value: distributionData.q3Count, description: "Below-average funds - REVIEW/SELL" },
      { name: "Q4 (Bottom 25%)", value: distributionData.q4Count, description: "Poor performers - SELL" },
    ];
  };
  
  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Quartile Analysis</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Analyze mutual funds ranked by quartiles based on comprehensive scoring
          </p>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="md:col-span-1">
            <Card>
              <CardHeader>
                <CardTitle>Filter Options</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <label className="text-sm font-medium text-neutral-700">Fund Category</label>
                    <Select value={selectedCategory} onValueChange={setSelectedCategory}>
                      <SelectTrigger className="w-full mt-1">
                        <SelectValue placeholder="Select category" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="All Categories">All Categories</SelectItem>
                        <SelectItem value="Equity: Large Cap">Equity: Large Cap</SelectItem>
                        <SelectItem value="Equity: Mid Cap">Equity: Mid Cap</SelectItem>
                        <SelectItem value="Equity: Small Cap">Equity: Small Cap</SelectItem>
                        <SelectItem value="Debt: Short Term">Debt: Short Term</SelectItem>
                        <SelectItem value="Debt: Medium Term">Debt: Medium Term</SelectItem>
                        <SelectItem value="Hybrid: Balanced">Hybrid: Balanced</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  
                  <div>
                    <label className="text-sm font-medium text-neutral-700">Select Quartile</label>
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
                </div>
                
                {isDistributionLoading ? (
                  <div className="mt-6">
                    <Skeleton className="h-64 w-full" />
                  </div>
                ) : distributionData ? (
                  <div className="mt-6 bg-neutral-50 rounded-lg p-4">
                    <h3 className="text-base font-medium text-neutral-900 mb-3">Quartile Distribution</h3>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={getPieData()}
                            cx="50%"
                            cy="50%"
                            labelLine={false}
                            label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                            outerRadius={80}
                            fill="#8884d8"
                            dataKey="value"
                          >
                            {getPieData().map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                          </Pie>
                          <Tooltip 
                            formatter={(value) => [
                              `${value} funds`,
                              ""
                            ]}
                          />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                    
                    <div className="mt-4 text-sm text-center">
                      <p>Total: <span className="font-semibold">{distributionData.totalCount} funds</span></p>
                    </div>
                  </div>
                ) : (
                  <div className="mt-6 bg-neutral-50 rounded-lg p-4 text-center py-12">
                    <p className="text-neutral-500">No distribution data available</p>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
          
          <div className="md:col-span-2">
            <Tabs defaultValue="metrics">
              <TabsList className="grid w-full grid-cols-2">
                <TabsTrigger value="metrics">Performance Metrics</TabsTrigger>
                <TabsTrigger value="funds">Fund List</TabsTrigger>
              </TabsList>
              
              <TabsContent value="metrics" className="mt-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Performance Metrics by Quartile</CardTitle>
                  </CardHeader>
                  <CardContent>
                    {isMetricsLoading ? (
                      <div className="h-96 flex items-center justify-center">
                        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                      </div>
                    ) : metricsData ? (
                      <div>
                        <div className="grid grid-cols-2 gap-4 mb-6">
                          <div>
                            <h4 className="text-base font-medium mb-2">Average Returns</h4>
                            <div className="h-64">
                              <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={metricsData.returnsData}>
                                  <CartesianGrid strokeDasharray="3 3" />
                                  <XAxis dataKey="name" />
                                  <YAxis tickFormatter={(value) => `${value}%`} />
                                  <Tooltip formatter={(value) => [`${value}%`, "Avg Return"]} />
                                  <Bar dataKey="return1Y" name="1 Year" fill="#3b82f6" />
                                  <Bar dataKey="return3Y" name="3 Year" fill="#22c55e" />
                                </BarChart>
                              </ResponsiveContainer>
                            </div>
                          </div>
                          <div>
                            <h4 className="text-base font-medium mb-2">Risk Metrics</h4>
                            <div className="h-64">
                              <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={metricsData.riskData}>
                                  <CartesianGrid strokeDasharray="3 3" />
                                  <XAxis dataKey="name" />
                                  <YAxis />
                                  <Tooltip />
                                  <Bar dataKey="sharpeRatio" name="Sharpe Ratio" fill="#22c55e" />
                                  <Bar dataKey="maxDrawdown" name="Max Drawdown (%)" fill="#ef4444" />
                                </BarChart>
                              </ResponsiveContainer>
                            </div>
                          </div>
                        </div>
                        
                        <div className="bg-neutral-50 rounded-lg p-4">
                          <h4 className="text-base font-medium mb-3">Quartile Scoring Breakdown</h4>
                          <div className="overflow-x-auto">
                            <table className="min-w-full divide-y divide-neutral-200">
                              <thead>
                                <tr>
                                  <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase">Quartile</th>
                                  <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Historical Returns</th>
                                  <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Risk Grade</th>
                                  <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Other Metrics</th>
                                  <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Total Score</th>
                                </tr>
                              </thead>
                              <tbody className="bg-white divide-y divide-neutral-200">
                                {metricsData.scoringData.map((quartile) => (
                                  <tr key={quartile.name}>
                                    <td className="px-4 py-3 whitespace-nowrap">
                                      <div className="flex items-center">
                                        <div className={`w-3 h-3 rounded-full mr-2 ${
                                          quartile.name === "Q1" ? "bg-green-500" : 
                                          quartile.name === "Q2" ? "bg-blue-500" : 
                                          quartile.name === "Q3" ? "bg-yellow-500" : "bg-red-500"
                                        }`}></div>
                                        <div className="text-sm font-medium text-neutral-900">{quartile.label}</div>
                                      </div>
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                      {(quartile.compositeScore || 0).toFixed(1)}
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                      -
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                      -
                                    </td>
                                    <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                                      {(quartile.totalScore || 0).toFixed(1)}
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div className="h-96 flex items-center justify-center">
                        <p className="text-neutral-500">No metrics data available</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </TabsContent>
              
              <TabsContent value="funds" className="mt-4">
                <Card>
                  <CardHeader>
                    <CardTitle>
                      {selectedCategory === "All Categories" 
                        ? `Quartile ${selectedQuartile} Funds` 
                        : `${selectedCategory} - Quartile ${selectedQuartile} Funds`}
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {isFundsLoading ? (
                      <div className="h-96 flex items-center justify-center">
                        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary"></div>
                      </div>
                    ) : fundsData && fundsData.funds && fundsData.funds.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-neutral-200">
                          <thead>
                            <tr>
                              <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase">Fund Name</th>
                              <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase">Category</th>
                              <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase">AMC</th>
                              <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Rank</th>
                              <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase">Composite Score</th>
                              <th className="px-4 py-3 bg-neutral-100 text-center text-xs font-medium text-neutral-500 uppercase">Recommendation</th>
                            </tr>
                          </thead>
                          <tbody className="bg-white divide-y divide-neutral-200">
                            {fundsData.funds.map((fund) => (
                              <tr key={fund.id}>
                                <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-primary-600">
                                  {fund.fundName}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-neutral-500">
                                  {fund.category}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-neutral-500">
                                  {fund.amc}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                  #{fund.rank}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium">
                                  {fund.totalScore ? parseFloat(fund.totalScore).toFixed(2) : "N/A"}
                                </td>
                                <td className="px-4 py-3 whitespace-nowrap text-xs text-center">
                                  <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full font-medium
                                    ${fund.recommendation === 'BUY' ? 'bg-green-100 text-green-800' : 
                                      fund.recommendation === 'HOLD' ? 'bg-blue-100 text-blue-800' :
                                      fund.recommendation === 'REVIEW' ? 'bg-yellow-100 text-yellow-800' :
                                      'bg-red-100 text-red-800'}`
                                  }>
                                    {fund.recommendation || "N/A"}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="h-96 flex items-center justify-center">
                        <p className="text-neutral-500">No funds available for the selected quartile and category</p>
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