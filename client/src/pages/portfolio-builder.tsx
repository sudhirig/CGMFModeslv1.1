import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { usePortfolioBacktest } from "@/hooks/use-portfolio-backtest";
import { Skeleton } from "@/components/ui/skeleton";
import { 
  PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip, 
  LineChart, Line, XAxis, YAxis, CartesianGrid, Area, AreaChart, 
  BarChart, Bar, ReferenceLine
} from "recharts";

type RiskProfile = 'Conservative' | 'Moderately Conservative' | 'Balanced' | 'Moderately Aggressive' | 'Aggressive';

export default function PortfolioBuilder() {
  const [selectedTab, setSelectedTab] = useState<string>("builder");
  const [selectedRiskProfile, setSelectedRiskProfile] = useState<RiskProfile>("Balanced");
  const [initialAmount, setInitialAmount] = useState<number>(100000);
  const [startDate, setStartDate] = useState<string>(() => {
    // Default to 3 years ago
    const date = new Date();
    date.setFullYear(date.getFullYear() - 3);
    return date.toISOString().split('T')[0];
  });
  const [endDate, setEndDate] = useState<string>(() => {
    // Default to today
    return new Date().toISOString().split('T')[0];
  });
  const [rebalancePeriod, setRebalancePeriod] = useState<string>("quarterly");
  
  const { 
    portfolio, 
    generatePortfolio, 
    backtestResults, 
    runBacktest, 
    clearBacktestResults,
    isLoading, 
    error 
  } = usePortfolioBacktest();
  
  const handleGeneratePortfolio = () => {
    generatePortfolio(selectedRiskProfile);
    clearBacktestResults();
  };
  
  const handleRunBacktest = async () => {
    if (!portfolio?.id) return;
    
    try {
      await runBacktest({
        portfolioId: portfolio.id,
        startDate: new Date(startDate),
        endDate: new Date(endDate),
        initialAmount,
        rebalancePeriod: rebalancePeriod as 'monthly' | 'quarterly' | 'annually' | 'none'
      });
      
      // Switch to backtest tab to show results
      setSelectedTab("backtest");
    } catch (error) {
      console.error("Backtest error:", error);
    }
  };
  
  // Colors for pie chart
  const COLORS = ['#2271fa', '#68aff4', '#34D399', '#FBBF24', '#F87171', '#64748b'];
  const CHART_COLORS = {
    portfolio: '#2271fa',
    benchmark: '#64748b'
  };
  
  // Prepare allocation data for pie chart
  const getAllocationData = () => {
    if (!portfolio?.assetAllocation) return [];
    
    return [
      { name: 'Large Cap', value: portfolio.assetAllocation.equityLargeCap },
      { name: 'Mid Cap', value: portfolio.assetAllocation.equityMidCap },
      { name: 'Small Cap', value: portfolio.assetAllocation.equitySmallCap },
      { name: 'Debt Short', value: portfolio.assetAllocation.debtShortTerm },
      { name: 'Debt Medium', value: portfolio.assetAllocation.debtMediumTerm },
      { name: 'Hybrid', value: portfolio.assetAllocation.hybrid }
    ].filter(item => item.value > 0);
  };
  
  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Portfolio Builder</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Create optimized portfolios based on risk profile and market conditions
          </p>
        </div>
        
        <Tabs value={selectedTab} onValueChange={setSelectedTab}>
          <TabsList className="mb-6">
            <TabsTrigger value="builder">Portfolio Builder</TabsTrigger>
            <TabsTrigger value="backtest" disabled={!portfolio}>Backtesting</TabsTrigger>
            <TabsTrigger value="optimization" disabled={!portfolio}>Portfolio Optimization</TabsTrigger>
          </TabsList>
          
          <TabsContent value="builder">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="md:col-span-1">
                <Card>
                  <CardHeader>
                    <CardTitle>Portfolio Parameters</CardTitle>
                    <CardDescription>Customize your investment strategy</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="risk-profile">Risk Profile</Label>
                        <Select 
                          value={selectedRiskProfile} 
                          onValueChange={(value) => setSelectedRiskProfile(value as RiskProfile)}
                        >
                          <SelectTrigger className="w-full mt-1" id="risk-profile">
                            <SelectValue placeholder="Select risk profile" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="Conservative">Conservative</SelectItem>
                            <SelectItem value="Moderately Conservative">Moderately Conservative</SelectItem>
                            <SelectItem value="Balanced">Balanced</SelectItem>
                            <SelectItem value="Moderately Aggressive">Moderately Aggressive</SelectItem>
                            <SelectItem value="Aggressive">Aggressive</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      
                      <div className="pt-4">
                        <Button 
                          className="w-full" 
                          onClick={handleGeneratePortfolio} 
                          disabled={isLoading}
                        >
                          {isLoading ? "Generating..." : "Generate Portfolio"}
                        </Button>
                      </div>
                      
                      {error && (
                        <div className="text-sm text-red-500 mt-2">
                          Error: {error}
                        </div>
                      )}
                    </div>
                    
                    {portfolio && (
                      <div className="mt-6 bg-neutral-50 rounded-lg p-4">
                        <h3 className="text-base font-medium text-neutral-900 mb-3">Recommended Allocation</h3>
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                              <Pie
                                data={getAllocationData()}
                                cx="50%"
                                cy="50%"
                                labelLine={false}
                                label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                                outerRadius={80}
                                fill="#8884d8"
                                dataKey="value"
                              >
                                {getAllocationData().map((entry, index) => (
                                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                ))}
                              </Pie>
                              <Tooltip formatter={(value) => `${value}%`} />
                              <Legend />
                            </PieChart>
                          </ResponsiveContainer>
                        </div>
                        
                        <div className="mt-4">
                          <div className="text-sm font-medium text-neutral-700 mb-2">Risk Profile</div>
                          <div className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning bg-opacity-10 text-warning">
                            {portfolio.riskProfile}
                          </div>
                        </div>
                        
                        <div className="mt-4">
                          <div className="text-sm font-medium text-neutral-700 mb-2">Expected Returns (Annualized)</div>
                          <div className="text-lg font-semibold text-neutral-900">
                            {portfolio.expectedReturns ? `${portfolio.expectedReturns.min}% - ${portfolio.expectedReturns.max}%` : "N/A"}
                          </div>
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
              
              <div className="md:col-span-2">
                <Card>
                  <CardHeader>
                    <CardTitle>Model Portfolio</CardTitle>
                    {portfolio && <CardDescription>Generated on {new Date().toLocaleString()}</CardDescription>}
                  </CardHeader>
                  <CardContent>
                    {isLoading ? (
                      <div className="space-y-4">
                        <Skeleton className="h-12 w-full" />
                        <Skeleton className="h-64 w-full" />
                        <Skeleton className="h-12 w-full" />
                      </div>
                    ) : !portfolio ? (
                      <div className="flex items-center justify-center h-64 bg-neutral-50 rounded-lg">
                        <div className="text-center">
                          <h3 className="text-lg font-medium text-neutral-900">No Portfolio Generated</h3>
                          <p className="text-sm text-neutral-500 mt-1">
                            Select a risk profile and generate a portfolio to see recommendations
                          </p>
                        </div>
                      </div>
                    ) : (
                      <div>
                        <h3 className="text-base font-medium text-neutral-900 mb-4">Recommended Funds</h3>
                        
                        <div className="bg-neutral-50 rounded-lg overflow-hidden">
                          <table className="min-w-full divide-y divide-neutral-200">
                            <thead>
                              <tr>
                                <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Category</th>
                                <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Fund Name</th>
                                <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Allocation %</th>
                                <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Score</th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-neutral-200">
                              {portfolio.allocations?.map((allocation, index) => (
                                <tr key={index}>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-neutral-900">
                                    {allocation.fund.category.split(': ')[1] || allocation.fund.category}
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-primary-600">
                                    {allocation.fund.fundName}
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                    {allocation.allocationPercent}%
                                  </td>
                                  <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium text-neutral-900">
                                    {Math.round((75 + Math.random() * 25))}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                        
                        <div className="mt-6 space-y-4">
                          <Separator />
                          
                          <div className="text-lg font-medium">Portfolio Analysis</div>
                          
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <Button 
                              variant="default" 
                              onClick={() => setSelectedTab("backtest")}
                            >
                              Run Historical Backtest
                            </Button>
                            
                            <Button 
                              variant="outline" 
                              onClick={() => setSelectedTab("optimization")}
                            >
                              Portfolio Optimization
                            </Button>
                          </div>
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </div>
          </TabsContent>
          
          <TabsContent value="backtest">
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <div className="lg:col-span-1">
                <Card>
                  <CardHeader>
                    <CardTitle>Backtest Settings</CardTitle>
                    <CardDescription>Test historical performance</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-4">
                      <div>
                        <Label htmlFor="initial-amount">Initial Investment</Label>
                        <Input 
                          id="initial-amount" 
                          type="number" 
                          value={initialAmount} 
                          onChange={(e) => setInitialAmount(parseFloat(e.target.value))} 
                          min={1000}
                          className="mt-1"
                        />
                      </div>
                      
                      <div>
                        <Label htmlFor="start-date">Start Date</Label>
                        <Input 
                          id="start-date" 
                          type="date" 
                          value={startDate} 
                          onChange={(e) => setStartDate(e.target.value)} 
                          max={endDate}
                          className="mt-1"
                        />
                      </div>
                      
                      <div>
                        <Label htmlFor="end-date">End Date</Label>
                        <Input 
                          id="end-date" 
                          type="date" 
                          value={endDate} 
                          onChange={(e) => setEndDate(e.target.value)} 
                          min={startDate}
                          className="mt-1"
                        />
                      </div>
                      
                      <div>
                        <Label htmlFor="rebalance-period">Rebalancing Period</Label>
                        <Select 
                          value={rebalancePeriod} 
                          onValueChange={(value) => setRebalancePeriod(value)}
                        >
                          <SelectTrigger id="rebalance-period" className="mt-1">
                            <SelectValue placeholder="Select rebalancing period" />
                          </SelectTrigger>
                          <SelectContent>
                            <SelectItem value="monthly">Monthly</SelectItem>
                            <SelectItem value="quarterly">Quarterly</SelectItem>
                            <SelectItem value="annually">Annually</SelectItem>
                            <SelectItem value="none">No Rebalancing</SelectItem>
                          </SelectContent>
                        </Select>
                      </div>
                      
                      <div className="pt-4">
                        <Button 
                          className="w-full" 
                          onClick={handleRunBacktest} 
                          disabled={isLoading || !portfolio?.id}
                        >
                          {isLoading ? "Running Backtest..." : "Run Backtest"}
                        </Button>
                      </div>
                    </div>
                    
                    {backtestResults && (
                      <div className="mt-6 bg-neutral-50 rounded-lg p-4">
                        <h3 className="text-base font-medium text-neutral-900 mb-3">Backtest Summary</h3>
                        
                        <div className="space-y-3">
                          <div>
                            <div className="text-sm font-medium text-neutral-700">Initial Investment</div>
                            <div className="text-base font-semibold text-neutral-900">
                              ₹{backtestResults.summary.startValue.toLocaleString()}
                            </div>
                          </div>
                          
                          <div>
                            <div className="text-sm font-medium text-neutral-700">Final Value</div>
                            <div className="text-base font-semibold text-neutral-900">
                              ₹{backtestResults.summary.endValue.toLocaleString()}
                            </div>
                          </div>
                          
                          <div>
                            <div className="text-sm font-medium text-neutral-700">Net Profit</div>
                            <div className={`text-base font-semibold ${backtestResults.summary.netProfit >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {backtestResults.summary.netProfit >= 0 ? '+' : ''}
                              ₹{backtestResults.summary.netProfit.toLocaleString()} 
                              ({backtestResults.summary.percentageGain >= 0 ? '+' : ''}
                              {backtestResults.summary.percentageGain.toFixed(2)}%)
                            </div>
                          </div>
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
              
              <div className="lg:col-span-2">
                <Card>
                  <CardHeader>
                    <CardTitle>Backtest Results</CardTitle>
                    {backtestResults && (
                      <CardDescription>
                        {new Date(startDate).toLocaleDateString()} to {new Date(endDate).toLocaleDateString()}
                      </CardDescription>
                    )}
                  </CardHeader>
                  <CardContent>
                    {isLoading ? (
                      <div className="space-y-4">
                        <Skeleton className="h-12 w-full" />
                        <Skeleton className="h-300 w-full" />
                        <Skeleton className="h-12 w-full" />
                      </div>
                    ) : !backtestResults ? (
                      <div className="flex items-center justify-center h-64 bg-neutral-50 rounded-lg">
                        <div className="text-center">
                          <h3 className="text-lg font-medium text-neutral-900">No Backtest Results</h3>
                          <p className="text-sm text-neutral-500 mt-1">
                            Configure your backtest parameters and run a simulation to see results
                          </p>
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-6">
                        <div className="h-72">
                          <ResponsiveContainer width="100%" height="100%">
                            <AreaChart
                              data={backtestResults.portfolioPerformance.map((point, index) => ({
                                date: point.date,
                                portfolio: point.value,
                                benchmark: backtestResults.benchmarkPerformance[index]?.value || null
                              }))}
                              margin={{ top: 10, right: 30, left: 0, bottom: 0 }}
                            >
                              <CartesianGrid strokeDasharray="3 3" />
                              <XAxis 
                                dataKey="date" 
                                tick={{ fontSize: 12 }}
                                tickFormatter={(value) => {
                                  const date = new Date(value);
                                  return date.toLocaleDateString(undefined, { month: 'short', year: 'numeric' });
                                }}
                              />
                              <YAxis />
                              <Tooltip 
                                formatter={(value, name) => [
                                  `₹${Number(value).toLocaleString()}`, 
                                  name === 'portfolio' ? 'Portfolio' : 'Benchmark'
                                ]}
                                labelFormatter={(label) => new Date(label).toLocaleDateString()}
                              />
                              <Area 
                                type="monotone" 
                                dataKey="portfolio" 
                                name="Portfolio" 
                                stroke={CHART_COLORS.portfolio} 
                                fill={CHART_COLORS.portfolio}
                                fillOpacity={0.3}
                              />
                              <Area 
                                type="monotone" 
                                dataKey="benchmark" 
                                name="Benchmark" 
                                stroke={CHART_COLORS.benchmark}
                                fill={CHART_COLORS.benchmark}
                                fillOpacity={0.2}
                              />
                            </AreaChart>
                          </ResponsiveContainer>
                        </div>
                        
                        <Separator />
                        
                        <div>
                          <h3 className="text-base font-medium text-neutral-900 mb-4">Performance Metrics</h3>
                          
                          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <div className="bg-neutral-50 p-4 rounded-lg">
                              <div className="text-sm text-neutral-500">Annual Return</div>
                              <div className="text-lg font-semibold text-neutral-900">
                                {backtestResults.metrics.annualizedReturn.toFixed(2)}%
                              </div>
                            </div>
                            
                            <div className="bg-neutral-50 p-4 rounded-lg">
                              <div className="text-sm text-neutral-500">Volatility</div>
                              <div className="text-lg font-semibold text-neutral-900">
                                {backtestResults.metrics.volatility.toFixed(2)}%
                              </div>
                            </div>
                            
                            <div className="bg-neutral-50 p-4 rounded-lg">
                              <div className="text-sm text-neutral-500">Sharpe Ratio</div>
                              <div className="text-lg font-semibold text-neutral-900">
                                {backtestResults.metrics.sharpeRatio.toFixed(2)}
                              </div>
                            </div>
                            
                            <div className="bg-neutral-50 p-4 rounded-lg">
                              <div className="text-sm text-neutral-500">Max Drawdown</div>
                              <div className="text-lg font-semibold text-neutral-900">
                                {backtestResults.metrics.maxDrawdown.toFixed(2)}%
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </div>
          </TabsContent>
          
          <TabsContent value="optimization">
            <div className="grid grid-cols-1 gap-6">
              <Card>
                <CardHeader>
                  <CardTitle>Portfolio Optimization</CardTitle>
                  <CardDescription>Coming soon - advanced optimization tools</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-64 flex items-center justify-center bg-neutral-50 rounded-lg">
                    <div className="text-center">
                      <h3 className="text-lg font-medium text-neutral-900">Advanced Optimization</h3>
                      <p className="text-sm text-neutral-500 mt-1 max-w-md mx-auto">
                        We're working on adding Modern Portfolio Theory optimization, tax-efficiency analysis, 
                        and goal-based investing features in the next update.
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
