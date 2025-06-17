import { useQuery, useMutation } from "@tanstack/react-query";
import { useState, useEffect } from "react";
import { queryClient } from "@/lib/queryClient";
import { z } from "zod";
import { format } from "date-fns";
import { useToast } from "@/hooks/use-toast";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";

import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { DatePicker } from "@/components/ui/date-picker";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";

import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, 
  Legend, ResponsiveContainer, BarChart, Bar, Area, AreaChart
} from 'recharts';

const backtestFormSchema = z.object({
  portfolioId: z.string().optional(),
  riskProfile: z.string().optional(),
  fundId: z.string().optional(),
  fundIds: z.array(z.string()).optional(),
  elivateScoreRange: z.object({
    min: z.number(),
    max: z.number()
  }).optional(),
  quartile: z.enum(["Q1", "Q2", "Q3", "Q4"]).optional(),
  recommendation: z.enum(["BUY", "HOLD", "SELL"]).optional(),
  maxFunds: z.string().optional(),
  scoreWeighting: z.boolean().optional(),
  startDate: z.date(),
  endDate: z.date(),
  initialAmount: z.string().refine((val) => !isNaN(parseFloat(val)) && parseFloat(val) > 0, {
    message: "Amount must be a positive number",
  }),
  rebalancePeriod: z.enum(["monthly", "quarterly", "annually"]),
});

export default function BacktestingPage() {
  const { toast } = useToast();
  const [activeTab, setActiveTab] = useState("risk-profile");
  const [backtestResults, setBacktestResults] = useState<any>(null);
  const [systemStatus, setSystemStatus] = useState<'checking' | 'operational' | 'degraded'>('checking');
  
  // Check system status on mount
  useEffect(() => {
    const checkSystemStatus = async () => {
      try {
        const response = await fetch('/api/comprehensive-backtest', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            fundId: 8319,
            startDate: '2024-01-01',
            endDate: '2024-12-31',
            initialAmount: 100000,
            rebalancePeriod: 'quarterly'
          }),
        });
        setSystemStatus(response.ok ? 'operational' : 'degraded');
      } catch {
        setSystemStatus('degraded');
      }
    };
    checkSystemStatus();
  }, []);

  // Get portfolios for selection
  const { data: portfolios, isLoading: isLoadingPortfolios } = useQuery({
    queryKey: ['/api/portfolios'],
    retry: false,
  });

  // Get top-rated funds for selection
  const { data: topFunds, isLoading: isLoadingFunds } = useQuery({
    queryKey: ['/api/funds/top-rated'],
    retry: false,
  });
  
  // Backtesting form
  const form = useForm<z.infer<typeof backtestFormSchema>>({
    resolver: zodResolver(backtestFormSchema),
    defaultValues: {
      startDate: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), // 1 year ago
      endDate: new Date(),
      initialAmount: "100000",
      rebalancePeriod: "quarterly",
      maxFunds: "10",
      scoreWeighting: true,
      elivateScoreRange: { min: 50, max: 100 }
    },
  });
  
  // Run backtest mutation
  const runBacktestMutation = useMutation({
    mutationFn: async (data: z.infer<typeof backtestFormSchema>) => {
      // Format dates to ISO strings
      const formattedData = {
        ...data,
        startDate: data.startDate.toISOString(),
        endDate: data.endDate.toISOString(),
      };
      
      const response = await fetch('/api/comprehensive-backtest', {
        method: 'POST',
        body: JSON.stringify(formattedData),
        headers: {
          'Content-Type': 'application/json',
        },
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || 'Failed to run backtest');
      }
      
      return response.json();
    },
    onSuccess: (data) => {
      console.log('Backtest results received:', data);
      setBacktestResults(data);
      toast({
        title: "Backtest Complete",
        description: "Portfolio backtest results are ready.",
      });
    },
    onError: (error) => {
      console.error('Backtest error:', error);
      toast({
        variant: "destructive",
        title: "Backtest Failed",
        description: error instanceof Error ? error.message : "An error occurred during backtesting.",
      });
    },
  });
  
  function onSubmit(data: z.infer<typeof backtestFormSchema>) {
    // Validate that we have at least one selection criteria
    const hasSelection = data.portfolioId || data.riskProfile || data.fundId || 
                        (data.fundIds && data.fundIds.length > 0) || data.elivateScoreRange ||
                        data.quartile || data.recommendation;
    
    if (!hasSelection) {
      toast({
        variant: "destructive",
        title: "Validation Error",
        description: "Please select at least one backtesting criteria.",
      });
      return;
    }
    
    // Clear any existing results
    setBacktestResults(null);
    
    // Run the backtest
    runBacktestMutation.mutate(data);
  }
  
  function formatCurrency(value: number) {
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0,
    }).format(value);
  }
  
  function formatPercentage(value: number) {
    return `${value.toFixed(2)}%`;
  }
  
  function formatDateForChart(date: string) {
    return format(new Date(date), "MMM yyyy");
  }
  
  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      <div className="max-w-7xl mx-auto p-6">
        <div className="flex items-center justify-between mb-8">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Portfolio Backtesting</h1>
            <p className="text-gray-600 dark:text-gray-300 mt-2">
              Test investment strategies using historical data with ELIVATE scoring
            </p>
          </div>
          <div className="flex items-center space-x-2">
            <div className={`h-3 w-3 rounded-full ${
              systemStatus === 'operational' ? 'bg-green-500' : 
              systemStatus === 'degraded' ? 'bg-red-500' : 'bg-yellow-500'
            }`}></div>
            <span className="text-sm text-gray-600 dark:text-gray-300">
              {systemStatus === 'operational' ? 'System Operational' : 
               systemStatus === 'degraded' ? 'System Degraded' : 'Checking Status'}
            </span>
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-1">
            <Card>
              <CardHeader>
                <CardTitle>Backtest Configuration</CardTitle>
                <CardDescription>
                  Set up your backtesting parameters
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                  <TabsList className="grid w-full grid-cols-3">
                    <TabsTrigger value="risk-profile">Risk Profile</TabsTrigger>
                    <TabsTrigger value="portfolio">Portfolio</TabsTrigger>
                    <TabsTrigger value="individual">Individual</TabsTrigger>
                  </TabsList>
                  <TabsList className="grid w-full grid-cols-3 mt-2">
                    <TabsTrigger value="score-range">Score Range</TabsTrigger>
                    <TabsTrigger value="quartile">Quartile</TabsTrigger>
                    <TabsTrigger value="recommendation">Recommendation</TabsTrigger>
                  </TabsList>
                  
                  <Form {...form}>
                    <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6 mt-6">
                      <TabsContent value="risk-profile" className="space-y-4">
                        <FormField
                          control={form.control}
                          name="riskProfile"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Risk Profile</FormLabel>
                              <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                  <SelectTrigger>
                                    <SelectValue placeholder="Select risk profile" />
                                  </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                  <SelectItem value="Conservative">Conservative</SelectItem>
                                  <SelectItem value="Balanced">Balanced</SelectItem>
                                  <SelectItem value="Aggressive">Aggressive</SelectItem>
                                </SelectContent>
                              </Select>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>
                      
                      <TabsContent value="portfolio" className="space-y-4">
                        <FormField
                          control={form.control}
                          name="portfolioId"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Portfolio</FormLabel>
                              <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                  <SelectTrigger>
                                    <SelectValue placeholder="Select portfolio" />
                                  </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                  {isLoadingPortfolios ? (
                                    <SelectItem value="loading">Loading...</SelectItem>
                                  ) : portfolios && Array.isArray(portfolios) && portfolios.length > 0 ? (
                                    portfolios.map((portfolio: any) => (
                                      <SelectItem key={portfolio.id} value={portfolio.id.toString()}>
                                        {portfolio.name}
                                      </SelectItem>
                                    ))
                                  ) : (
                                    <SelectItem value="none">No portfolios available</SelectItem>
                                  )}
                                </SelectContent>
                              </Select>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>

                      <TabsContent value="individual" className="space-y-4">
                        <FormField
                          control={form.control}
                          name="fundId"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Individual Fund</FormLabel>
                              <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                  <SelectTrigger>
                                    <SelectValue placeholder="Select a fund" />
                                  </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                  {isLoadingFunds ? (
                                    <SelectItem value="loading">Loading...</SelectItem>
                                  ) : topFunds && Array.isArray(topFunds) && topFunds.length > 0 ? (
                                    topFunds.map((fund: any) => (
                                      <SelectItem key={fund.fundId} value={fund.fundId.toString()}>
                                        {fund.fund.fundName || `Fund ${fund.fundId}`}
                                      </SelectItem>
                                    ))
                                  ) : (
                                    <SelectItem value="none">No funds available</SelectItem>
                                  )}
                                </SelectContent>
                              </Select>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>

                      <TabsContent value="score-range" className="space-y-4">
                        <div className="grid grid-cols-2 gap-4">
                          <FormField
                            control={form.control}
                            name="elivateScoreRange.min"
                            render={({ field }) => (
                              <FormItem>
                                <FormLabel>Min ELIVATE Score</FormLabel>
                                <FormControl>
                                  <Input 
                                    type="number" 
                                    placeholder="50" 
                                    {...field} 
                                    onChange={(e) => field.onChange(parseFloat(e.target.value))}
                                  />
                                </FormControl>
                                <FormMessage />
                              </FormItem>
                            )}
                          />
                          <FormField
                            control={form.control}
                            name="elivateScoreRange.max"
                            render={({ field }) => (
                              <FormItem>
                                <FormLabel>Max ELIVATE Score</FormLabel>
                                <FormControl>
                                  <Input 
                                    type="number" 
                                    placeholder="100" 
                                    {...field}
                                    onChange={(e) => field.onChange(parseFloat(e.target.value))}
                                  />
                                </FormControl>
                                <FormMessage />
                              </FormItem>
                            )}
                          />
                        </div>
                        <FormField
                          control={form.control}
                          name="maxFunds"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Maximum Funds</FormLabel>
                              <FormControl>
                                <Input placeholder="10" {...field} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>

                      <TabsContent value="quartile" className="space-y-4">
                        <FormField
                          control={form.control}
                          name="quartile"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>ELIVATE Quartile</FormLabel>
                              <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                  <SelectTrigger>
                                    <SelectValue placeholder="Select quartile" />
                                  </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                  <SelectItem value="Q1">Q1 - Top Performers</SelectItem>
                                  <SelectItem value="Q2">Q2 - Above Average</SelectItem>
                                  <SelectItem value="Q3">Q3 - Below Average</SelectItem>
                                  <SelectItem value="Q4">Q4 - Bottom Performers</SelectItem>
                                </SelectContent>
                              </Select>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                        <FormField
                          control={form.control}
                          name="maxFunds"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Maximum Funds</FormLabel>
                              <FormControl>
                                <Input placeholder="15" {...field} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>

                      <TabsContent value="recommendation" className="space-y-4">
                        <FormField
                          control={form.control}
                          name="recommendation"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Investment Recommendation</FormLabel>
                              <Select onValueChange={field.onChange} defaultValue={field.value}>
                                <FormControl>
                                  <SelectTrigger>
                                    <SelectValue placeholder="Select recommendation" />
                                  </SelectTrigger>
                                </FormControl>
                                <SelectContent>
                                  <SelectItem value="BUY">BUY - Recommended Funds</SelectItem>
                                  <SelectItem value="HOLD">HOLD - Neutral Funds</SelectItem>
                                  <SelectItem value="SELL">SELL - Underperforming Funds</SelectItem>
                                </SelectContent>
                              </Select>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                        <FormField
                          control={form.control}
                          name="maxFunds"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Maximum Funds</FormLabel>
                              <FormControl>
                                <Input placeholder="20" {...field} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                      </TabsContent>
                      
                      <Separator />
                      
                      <div className="grid grid-cols-1 gap-4">
                        <FormField
                          control={form.control}
                          name="startDate"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Start Date</FormLabel>
                              <FormControl>
                                <DatePicker date={field.value} setDate={field.onChange} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                        
                        <FormField
                          control={form.control}
                          name="endDate"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>End Date</FormLabel>
                              <FormControl>
                                <DatePicker date={field.value} setDate={field.onChange} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                        
                        <FormField
                          control={form.control}
                          name="initialAmount"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Initial Investment (₹)</FormLabel>
                              <FormControl>
                                <Input placeholder="100000" {...field} />
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />
                        
                        <FormField
                          control={form.control}
                          name="rebalancePeriod"
                          render={({ field }) => (
                            <FormItem>
                              <FormLabel>Rebalance Period</FormLabel>
                              <FormControl>
                                <RadioGroup
                                  onValueChange={field.onChange}
                                  defaultValue={field.value}
                                  className="flex flex-col space-y-1"
                                >
                                  <div className="flex items-center space-x-2">
                                    <RadioGroupItem value="monthly" id="monthly" />
                                    <label htmlFor="monthly">Monthly</label>
                                  </div>
                                  <div className="flex items-center space-x-2">
                                    <RadioGroupItem value="quarterly" id="quarterly" />
                                    <label htmlFor="quarterly">Quarterly</label>
                                  </div>
                                  <div className="flex items-center space-x-2">
                                    <RadioGroupItem value="annually" id="annually" />
                                    <label htmlFor="annually">Annually</label>
                                  </div>
                                </RadioGroup>
                              </FormControl>
                              <FormMessage />
                            </FormItem>
                          )}
                        />

                        {(activeTab === "score-range" || activeTab === "individual") && (
                          <FormField
                            control={form.control}
                            name="scoreWeighting"
                            render={({ field }) => (
                              <FormItem className="flex flex-row items-center justify-between rounded-lg border p-4">
                                <div className="space-y-0.5">
                                  <FormLabel className="text-base">Score-based Weighting</FormLabel>
                                  <div className="text-sm text-gray-600 dark:text-gray-300">
                                    Weight allocations by ELIVATE scores
                                  </div>
                                </div>
                                <FormControl>
                                  <input
                                    type="checkbox"
                                    checked={field.value}
                                    onChange={field.onChange}
                                    className="h-4 w-4"
                                  />
                                </FormControl>
                              </FormItem>
                            )}
                          />
                        )}
                      </div>
                      
                      <Button 
                        type="submit" 
                        className="w-full"
                        disabled={runBacktestMutation.isPending}
                      >
                        {runBacktestMutation.isPending ? "Running Backtest..." : "Run Backtest"}
                      </Button>
                    </form>
                  </Form>
                </Tabs>
              </CardContent>
            </Card>
          </div>
          
          <div className="lg:col-span-2">
            {runBacktestMutation.isPending ? (
              <Card>
                <CardHeader>
                  <CardTitle>Running Backtest</CardTitle>
                  <CardDescription>Please wait while we analyze historical performance</CardDescription>
                </CardHeader>
                <CardContent className="space-y-8">
                  <Skeleton className="h-[300px] w-full" />
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                    <Skeleton className="h-24 w-full" />
                    <Skeleton className="h-24 w-full" />
                    <Skeleton className="h-24 w-full" />
                    <Skeleton className="h-24 w-full" />
                  </div>
                </CardContent>
              </Card>
            ) : backtestResults ? (
              <Card>
                <CardHeader>
                  <CardTitle>Comprehensive Backtest Results</CardTitle>
                  <CardDescription>
                    Portfolio: {backtestResults.portfolioId || 'Custom'} | 
                    Risk Profile: {backtestResults.riskProfile || 'Unknown'} |
                    ELIVATE Validated: {backtestResults.elivateScoreValidation ? 'Yes' : 'No'}
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                    <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
                      <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                        {(() => {
                          const totalReturn = backtestResults.performance?.totalReturn || backtestResults.totalReturn || 0;
                          return `${Number(totalReturn).toFixed(2)}%`;
                        })()}
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-300">Total Return</div>
                    </div>
                    
                    <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
                      <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                        {(() => {
                          const annualizedReturn = backtestResults.performance?.annualizedReturn || backtestResults.annualizedReturn || 0;
                          return `${Number(annualizedReturn).toFixed(2)}%`;
                        })()}
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-300">Annualized Return</div>
                    </div>
                    
                    <div className="bg-orange-50 dark:bg-orange-900/20 p-4 rounded-lg">
                      <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                        {(() => {
                          const volatility = backtestResults.riskMetrics?.volatility || backtestResults.volatility || 0;
                          return `${Number(volatility).toFixed(2)}%`;
                        })()}
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-300">Volatility</div>
                    </div>

                    <div className="bg-purple-50 dark:bg-purple-900/20 p-4 rounded-lg">
                      <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                        {(() => {
                          const sharpeRatio = backtestResults.riskMetrics?.sharpeRatio || backtestResults.sharpeRatio || 0;
                          return Number(sharpeRatio).toFixed(2);
                        })()}
                      </div>
                      <div className="text-sm text-gray-600 dark:text-gray-300">Sharpe Ratio</div>
                    </div>
                  </div>

                  {/* Performance Chart */}
                  {(backtestResults.returns || backtestResults.historicalData) && (backtestResults.returns?.length > 0 || backtestResults.historicalData?.length > 0) && (
                    <div className="h-64 mb-6">
                      <h3 className="text-lg font-semibold mb-2">Portfolio Performance</h3>
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={backtestResults.returns || backtestResults.historicalData}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis 
                            dataKey="date" 
                            tickFormatter={(value) => format(new Date(value), 'MMM yyyy')}
                          />
                          <YAxis tickFormatter={(value) => formatCurrency(value)} />
                          <Tooltip 
                            labelFormatter={(value) => format(new Date(value), 'MMM dd, yyyy')}
                            formatter={(value: number) => [formatCurrency(value), 'Portfolio Value']}
                          />
                          <Line 
                            type="monotone" 
                            dataKey={backtestResults.returns ? "value" : "portfolioValue"}
                            stroke="#3b82f6" 
                            strokeWidth={2}
                            dot={false}
                            name="Portfolio"
                          />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  )}

                  {/* Additional Details */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Initial Investment</div>
                      <div className="text-xl font-bold">{formatCurrency(backtestResults.initialAmount || 100000)}</div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Final Value</div>
                      <div className="text-xl font-bold">
                        {(() => {
                          const initialAmount = Number(backtestResults.initialAmount || 100000);
                          const totalReturn = backtestResults.performance?.totalReturn || backtestResults.totalReturn || 0;
                          const finalValue = initialAmount * (1 + totalReturn / 100);
                          return formatCurrency(finalValue);
                        })()}
                      </div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Max Drawdown</div>
                      <div className="text-xl font-bold text-red-500">
                        {(() => {
                          const maxDrawdown = backtestResults.riskMetrics?.maxDrawdown || backtestResults.maxDrawdown || 0;
                          return `${Number(maxDrawdown).toFixed(2)}%`;
                        })()}
                      </div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Period</div>
                      <div className="text-sm font-bold">
                        {backtestResults.startDate && backtestResults.endDate ? 
                          `${format(new Date(backtestResults.startDate), "MMM yyyy")} - ${format(new Date(backtestResults.endDate), "MMM yyyy")}` : 
                          'Analysis Period'
                        }
                      </div>
                    </div>
                  </div>

                  {/* Debug Information for Development */}
                  {process.env.NODE_ENV === 'development' && (
                    <details className="mt-4">
                      <summary className="cursor-pointer text-sm text-gray-500">Debug: Raw Results</summary>
                      <pre className="text-xs bg-gray-100 dark:bg-gray-800 p-2 rounded mt-2 overflow-auto">
                        {JSON.stringify(backtestResults, null, 2)}
                      </pre>
                    </details>
                  )}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardHeader>
                  <CardTitle>Portfolio Backtesting</CardTitle>
                  <CardDescription>
                    Test your investment strategy against historical market data to see how it would have performed
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="text-center py-12">
                    <p className="text-gray-500 dark:text-gray-400 mb-4">
                      Configure your backtest parameters and run analysis to see comprehensive results with ELIVATE scoring.
                    </p>
                    <p className="text-sm text-gray-400 dark:text-gray-500">
                      System Status: <span className={systemStatus === 'operational' ? 'text-green-500' : 'text-red-500'}>
                        {systemStatus === 'operational' ? 'Ready' : 'Checking...'}
                      </span>
                    </p>
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