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
  
  // Backtesting form
  const form = useForm<z.infer<typeof backtestFormSchema>>({
    resolver: zodResolver(backtestFormSchema),
    defaultValues: {
      startDate: new Date(Date.now() - 365 * 24 * 60 * 60 * 1000), // 1 year ago
      endDate: new Date(),
      initialAmount: "100000",
      rebalancePeriod: "quarterly"
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
      setBacktestResults(data);
      toast({
        title: "Backtest Complete",
        description: "Portfolio backtest results are ready.",
      });
    },
    onError: (error) => {
      toast({
        variant: "destructive",
        title: "Backtest Failed",
        description: error instanceof Error ? error.message : "An error occurred during backtesting.",
      });
    },
  });
  
  function onSubmit(data: z.infer<typeof backtestFormSchema>) {
    // Validate that we have either portfolioId or riskProfile
    if (!data.portfolioId && !data.riskProfile) {
      toast({
        variant: "destructive",
        title: "Validation Error",
        description: "Please select either a portfolio or a risk profile.",
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
  
  // Prepare chart data if we have results
  const chartData = backtestResults?.returns?.map((point: any) => ({
    date: formatDateForChart(point.date),
    portfolio: point.value,
    benchmark: backtestResults.benchmark.find((b: any) => 
      new Date(b.date).getTime() === new Date(point.date).getTime()
    )?.value || null,
  }));
  
  return (
    <div className="container mx-auto py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2">Portfolio Backtesting</h1>
        <p className="text-gray-500 dark:text-gray-400">
          Test your investment strategies against historical market data
        </p>
      </div>
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <div className="lg:col-span-1">
          <Card>
            <CardHeader>
              <CardTitle>Configuration</CardTitle>
              <CardDescription>Set up your backtest parameters</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
                <TabsList className="grid w-full grid-cols-2">
                  <TabsTrigger value="risk-profile">Risk Profile</TabsTrigger>
                  <TabsTrigger value="portfolio">Portfolio</TabsTrigger>
                </TabsList>
                <Form {...form}>
                  <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6 mt-4">
                    <TabsContent value="risk-profile">
                      <FormField
                        control={form.control}
                        name="riskProfile"
                        render={({ field }) => (
                          <FormItem className="space-y-3">
                            <FormLabel>Select Risk Profile</FormLabel>
                            <FormControl>
                              <RadioGroup
                                onValueChange={(value) => {
                                  field.onChange(value);
                                  form.setValue("portfolioId", undefined);
                                }}
                                defaultValue={field.value}
                                className="flex flex-col space-y-1"
                              >
                                <FormItem className="flex items-center space-x-3 space-y-0">
                                  <FormControl>
                                    <RadioGroupItem value="Conservative" />
                                  </FormControl>
                                  <FormLabel className="font-normal">
                                    Conservative
                                  </FormLabel>
                                </FormItem>
                                <FormItem className="flex items-center space-x-3 space-y-0">
                                  <FormControl>
                                    <RadioGroupItem value="Moderate" />
                                  </FormControl>
                                  <FormLabel className="font-normal">
                                    Moderate
                                  </FormLabel>
                                </FormItem>
                                <FormItem className="flex items-center space-x-3 space-y-0">
                                  <FormControl>
                                    <RadioGroupItem value="Balanced" />
                                  </FormControl>
                                  <FormLabel className="font-normal">
                                    Balanced
                                  </FormLabel>
                                </FormItem>
                                <FormItem className="flex items-center space-x-3 space-y-0">
                                  <FormControl>
                                    <RadioGroupItem value="Growth" />
                                  </FormControl>
                                  <FormLabel className="font-normal">
                                    Growth
                                  </FormLabel>
                                </FormItem>
                                <FormItem className="flex items-center space-x-3 space-y-0">
                                  <FormControl>
                                    <RadioGroupItem value="Aggressive" />
                                  </FormControl>
                                  <FormLabel className="font-normal">
                                    Aggressive
                                  </FormLabel>
                                </FormItem>
                              </RadioGroup>
                            </FormControl>
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                    </TabsContent>
                    
                    <TabsContent value="portfolio">
                      <FormField
                        control={form.control}
                        name="portfolioId"
                        render={({ field }) => (
                          <FormItem>
                            <FormLabel>Select Portfolio</FormLabel>
                            <Select
                              onValueChange={(value) => {
                                field.onChange(value);
                                form.setValue("riskProfile", undefined);
                              }}
                              defaultValue={field.value}
                            >
                              <FormControl>
                                <SelectTrigger>
                                  <SelectValue placeholder="Select a portfolio" />
                                </SelectTrigger>
                              </FormControl>
                              <SelectContent>
                                {isLoadingPortfolios ? (
                                  <SelectItem value="loading" disabled>
                                    Loading portfolios...
                                  </SelectItem>
                                ) : portfolios?.length > 0 ? (
                                  portfolios.map((portfolio: any) => (
                                    <SelectItem key={portfolio.id} value={portfolio.id.toString()}>
                                      {portfolio.name}
                                    </SelectItem>
                                  ))
                                ) : (
                                  <SelectItem value="none" disabled>
                                    No portfolios available
                                  </SelectItem>
                                )}
                              </SelectContent>
                            </Select>
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                    </TabsContent>
                    
                    <Separator />
                    
                    <div className="grid grid-cols-2 gap-4">
                      <FormField
                        control={form.control}
                        name="startDate"
                        render={({ field }) => (
                          <FormItem className="flex flex-col">
                            <FormLabel>Start Date</FormLabel>
                            <DatePicker date={field.value} setDate={field.onChange} />
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                      
                      <FormField
                        control={form.control}
                        name="endDate"
                        render={({ field }) => (
                          <FormItem className="flex flex-col">
                            <FormLabel>End Date</FormLabel>
                            <DatePicker date={field.value} setDate={field.onChange} />
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                    </div>
                    
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
                          <FormLabel>Rebalancing Frequency</FormLabel>
                          <Select onValueChange={field.onChange} defaultValue={field.value}>
                            <FormControl>
                              <SelectTrigger>
                                <SelectValue placeholder="Select rebalancing frequency" />
                              </SelectTrigger>
                            </FormControl>
                            <SelectContent>
                              <SelectItem value="monthly">Monthly</SelectItem>
                              <SelectItem value="quarterly">Quarterly</SelectItem>
                              <SelectItem value="annually">Annually</SelectItem>
                            </SelectContent>
                          </Select>
                          <FormMessage />
                        </FormItem>
                      )}
                    />
                    
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
                  <Skeleton className="h-24 w-full" />
                  <Skeleton className="h-24 w-full" />
                </div>
              </CardContent>
            </Card>
          ) : backtestResults ? (
            <>
              <Card className="mb-6">
                <CardHeader>
                  <CardTitle>Backtest Results</CardTitle>
                  <CardDescription>
                    {backtestResults.riskProfile ? 
                      `${backtestResults.riskProfile} Risk Profile` : 
                      "Custom Portfolio"} Performance: {format(new Date(backtestResults.startDate), "dd MMM yyyy")} to {format(new Date(backtestResults.endDate), "dd MMM yyyy")}
                  </CardDescription>
                </CardHeader>
              <CardContent>
                <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
                  <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">
                      {backtestResults.performance?.totalReturn?.toFixed(2) || '0.00'}%
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">Total Return</div>
                  </div>
                  
                  <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                      {backtestResults.performance?.annualizedReturn?.toFixed(2) || '0.00'}%
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">Annualized Return</div>
                  </div>
                  
                  <div className="bg-orange-50 dark:bg-orange-900/20 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                      {backtestResults.riskMetrics?.volatility?.toFixed(2) || '0.00'}%
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">Volatility</div>
                  </div>

                  <div className="bg-purple-50 dark:bg-purple-900/20 p-4 rounded-lg">
                    <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">
                      {backtestResults.riskMetrics?.sharpeRatio?.toFixed(2) || '0.00'}
                    </div>
                    <div className="text-sm text-gray-600 dark:text-gray-300">Sharpe Ratio</div>
                  </div>
                </div>

                {/* ELIVATE Score Validation */}
                {backtestResults.elivateScoreValidation && (
                  <div className="bg-gradient-to-r from-indigo-50 to-purple-50 dark:from-indigo-900/20 dark:to-purple-900/20 p-4 rounded-lg mb-6">
                    <h3 className="text-lg font-semibold mb-2">ELIVATE Score Analysis</h3>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <div>
                        <div className="text-xl font-bold text-indigo-600 dark:text-indigo-400">
                          {backtestResults.elivateScoreValidation.averagePortfolioScore?.toFixed(1) || 'N/A'}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-300">Average Portfolio Score</div>
                      </div>
                      <div>
                        <div className="text-xl font-bold text-purple-600 dark:text-purple-400">
                          {backtestResults.elivateScoreValidation.scorePredictionAccuracy?.toFixed(1) || 'N/A'}%
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-300">Prediction Accuracy</div>
                      </div>
                      <div>
                        <div className="text-xl font-bold text-green-600 dark:text-green-400">
                          {backtestResults.elivateScoreValidation.correlationAnalysis?.scoreToReturn?.toFixed(2) || 'N/A'}
                        </div>
                        <div className="text-sm text-gray-600 dark:text-gray-300">Score-Return Correlation</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Performance Chart */}
                {backtestResults.historicalData && backtestResults.historicalData.length > 0 && (
                  <div className="h-64 mb-6">
                    <h3 className="text-lg font-semibold mb-2">Portfolio Performance</h3>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={backtestResults.historicalData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="date" 
                          tickFormatter={(value) => format(new Date(value), 'MMM yyyy')}
                        />
                        <YAxis tickFormatter={(value) => `$${value.toLocaleString()}`} />
                        <Tooltip 
                          labelFormatter={(value) => format(new Date(value), 'MMM dd, yyyy')}
                          formatter={(value: number) => [`$${value.toLocaleString()}`, 'Portfolio Value']}
                        />
                        <Line 
                          type="monotone" 
                          dataKey="portfolioValue" 
                          stroke="#3b82f6" 
                          strokeWidth={2}
                          dot={false}
                          name="Portfolio"
                        />
                        {backtestResults.historicalData[0]?.benchmarkValue && (
                          <Line 
                            type="monotone" 
                            dataKey="benchmarkValue" 
                            stroke="#ef4444" 
                            strokeWidth={2}
                            strokeDasharray="5 5"
                            dot={false}
                            name="Benchmark"
                          />
                        )}
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}

                {/* Fund Attribution */}
                {backtestResults.attribution?.fundContributions && (
                  <div>
                    <h3 className="text-lg font-semibold mb-3">Fund Contributions</h3>
                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200 dark:divide-gray-700">
                        <thead className="bg-gray-50 dark:bg-gray-800">
                          <tr>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                              Fund
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                              ELIVATE Score
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                              Allocation
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                              Return
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-300 uppercase tracking-wider">
                              Contribution
                            </th>
                          </tr>
                        </thead>
                        <tbody className="bg-white dark:bg-gray-900 divide-y divide-gray-200 dark:divide-gray-700">
                          {backtestResults.attribution.fundContributions.map((fund: any, index: number) => (
                            <tr key={index}>
                              <td className="px-4 py-2 text-sm text-gray-900 dark:text-white">
                                {fund.fundName || `Fund ${fund.fundId}`}
                              </td>
                              <td className="px-4 py-2 text-sm text-gray-900 dark:text-white">
                                {fund.elivateScore?.toFixed(1) || 'N/A'}
                              </td>
                              <td className="px-4 py-2 text-sm text-gray-900 dark:text-white">
                                {(fund.allocation * 100)?.toFixed(1) || '0.0'}%
                              </td>
                              <td className="px-4 py-2 text-sm text-gray-900 dark:text-white">
                                {fund.absoluteReturn?.toFixed(2) || '0.00'}%
                              </td>
                              <td className="px-4 py-2 text-sm text-gray-900 dark:text-white">
                                {fund.contribution?.toFixed(2) || '0.00'}%
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
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
  );
}
                    <ResponsiveContainer width="100%" height="100%">
                      <AreaChart
                        data={chartData}
                        margin={{
                          top: 5,
                          right: 30,
                          left: 20,
                          bottom: 5,
                        }}
                      >
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" />
                        <YAxis />
                        <Tooltip 
                          formatter={(value) => formatCurrency(Number(value))}
                          labelFormatter={(label) => `Date: ${label}`}
                        />
                        <Legend />
                        <Area 
                          type="monotone" 
                          dataKey="portfolio" 
                          name="Portfolio Value" 
                          stroke="#4338ca" 
                          fill="#4338ca" 
                          fillOpacity={0.3}
                        />
                        <Area 
                          type="monotone" 
                          dataKey="benchmark" 
                          name="NIFTY 50" 
                          stroke="#f59e0b" 
                          fill="#f59e0b" 
                          fillOpacity={0.3}
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </div>
                  
                  <div className="mt-8">
                    <h3 className="text-lg font-medium mb-4">Performance Comparison</h3>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Portfolio Return</div>
                          <div className={`text-xl font-bold ${backtestResults.totalReturn >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                            {formatPercentage(backtestResults.totalReturn)}
                          </div>
                        </div>
                        <div>
                          <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Benchmark Return (NIFTY 50)</div>
                          <div className={`text-xl font-bold ${backtestResults.benchmarkReturn >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                            {formatPercentage(backtestResults.benchmarkReturn)}
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </>
          ) : (
            <Card>
              <CardHeader>
                <CardTitle>Portfolio Backtesting</CardTitle>
                <CardDescription>
                  Test your investment strategy against historical market data to see how it would have performed
                </CardDescription>
              </CardHeader>
              <CardContent className="flex flex-col items-center justify-center py-12">
                <div className="text-center space-y-4">
                  <div className="bg-slate-100 dark:bg-slate-800 rounded-full w-16 h-16 mx-auto flex items-center justify-center">
                    <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8 text-slate-500 dark:text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                    </svg>
                  </div>
                  <h3 className="text-xl font-medium">Configure Your Backtest</h3>
                  <p className="text-slate-500 dark:text-slate-400 max-w-md">
                    Select a portfolio or risk profile and set your parameters to analyze historical performance and compare against market benchmarks.
                  </p>
                </div>
              </CardContent>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
}