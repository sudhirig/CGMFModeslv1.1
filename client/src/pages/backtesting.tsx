import { useQuery, useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { queryClient, apiRequest } from "@/lib/queryClient";
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
    mutationFn: (data: z.infer<typeof backtestFormSchema>) => {
      // Format dates to ISO strings
      const formattedData = {
        ...data,
        startDate: data.startDate.toISOString(),
        endDate: data.endDate.toISOString(),
      };
      
      return apiRequest('/api/backtest', {
        method: 'POST',
        body: JSON.stringify(formattedData),
        headers: {
          'Content-Type': 'application/json',
        },
      });
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
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-8">
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Initial Investment</div>
                      <div className="text-2xl font-bold">{formatCurrency(backtestResults.initialAmount)}</div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Final Value</div>
                      <div className="text-2xl font-bold">{formatCurrency(backtestResults.finalAmount)}</div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Total Return</div>
                      <div className={`text-2xl font-bold ${backtestResults.totalReturn >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {formatPercentage(backtestResults.totalReturn)}
                      </div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Annualized Return</div>
                      <div className={`text-2xl font-bold ${backtestResults.annualizedReturn >= 0 ? 'text-green-500' : 'text-red-500'}`}>
                        {formatPercentage(backtestResults.annualizedReturn)}
                      </div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Maximum Drawdown</div>
                      <div className="text-2xl font-bold text-red-500">
                        {formatPercentage(backtestResults.maxDrawdown)}
                      </div>
                    </div>
                    <div className="bg-slate-50 dark:bg-slate-800 p-4 rounded-lg">
                      <div className="text-sm font-medium text-slate-500 dark:text-slate-400">Sharpe Ratio</div>
                      <div className="text-2xl font-bold">
                        {backtestResults.sharpeRatio.toFixed(2)}
                      </div>
                    </div>
                  </div>
                  
                  <div className="h-[400px] w-full">
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