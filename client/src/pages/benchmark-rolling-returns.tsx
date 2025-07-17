import { useState, useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { 
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, 
  ResponsiveContainer, Area, AreaChart, ComposedChart
} from "recharts";
import { 
  TrendingUp, BarChart3, Calendar, Info, Download, ChevronRight,
  ArrowUpRight, ArrowDownRight, Activity
} from "lucide-react";

interface BenchmarkData {
  index_name: string;
  index_date: string;
  close_value: number;
}

interface RollingReturn {
  date: string;
  return1Y: number;
  return3Y: number;
  return5Y: number;
  return7Y: number;
  return10Y: number;
}

export default function BenchmarkRollingReturns() {
  const [selectedBenchmark, setSelectedBenchmark] = useState<string>("Nifty 50 TRI");
  const [selectedBenchmark2, setSelectedBenchmark2] = useState<string>("");
  const [rollingPeriod, setRollingPeriod] = useState<string>("1Y");
  const [chartType, setChartType] = useState<string>("returns");

  // Fetch available benchmarks
  const { data: benchmarkList, isLoading: isLoadingBenchmarks } = useQuery({
    queryKey: ['/api/benchmarks/list'],
  });

  // Fetch historical data for selected benchmark
  const { data: benchmarkData, isLoading: isLoadingData } = useQuery({
    queryKey: ['/api/benchmarks/historical-data', selectedBenchmark],
    enabled: !!selectedBenchmark,
  });

  // Fetch historical data for comparison benchmark
  const { data: benchmarkData2, isLoading: isLoadingData2 } = useQuery({
    queryKey: ['/api/benchmarks/historical-data', selectedBenchmark2],
    enabled: !!selectedBenchmark2,
  });

  // Calculate rolling returns
  const rollingReturns = useMemo(() => {
    if (!benchmarkData?.data) return [];

    const data = benchmarkData.data as BenchmarkData[];
    const sorted = [...data].sort((a, b) => 
      new Date(a.index_date).getTime() - new Date(b.index_date).getTime()
    );

    const returns: RollingReturn[] = [];
    const periods = {
      '1Y': 252,
      '3Y': 756,
      '5Y': 1260,
      '7Y': 1764,
      '10Y': 2520
    };

    // Calculate rolling returns for each date
    for (let i = periods['10Y']; i < sorted.length; i++) {
      const currentDate = sorted[i].index_date;
      const currentValue = sorted[i].close_value;
      
      const returnData: RollingReturn = {
        date: currentDate,
        return1Y: 0,
        return3Y: 0,
        return5Y: 0,
        return7Y: 0,
        return10Y: 0
      };

      // Calculate returns for each period
      Object.entries(periods).forEach(([period, days]) => {
        if (i >= days) {
          const pastValue = sorted[i - days].close_value;
          const years = days / 252;
          const totalReturn = ((currentValue - pastValue) / pastValue) * 100;
          const annualizedReturn = (Math.pow(currentValue / pastValue, 1 / years) - 1) * 100;
          returnData[`return${period}` as keyof RollingReturn] = annualizedReturn;
        }
      });

      returns.push(returnData);
    }

    return returns;
  }, [benchmarkData]);

  // Calculate statistics
  const statistics = useMemo(() => {
    if (rollingReturns.length === 0) return null;

    const periodKey = `return${rollingPeriod}` as keyof RollingReturn;
    const values = rollingReturns
      .map(r => r[periodKey] as number)
      .filter(v => v !== 0);

    if (values.length === 0) return null;

    const avg = values.reduce((a, b) => a + b, 0) / values.length;
    const max = Math.max(...values);
    const min = Math.min(...values);
    const latest = values[values.length - 1];

    // Calculate standard deviation
    const variance = values.reduce((acc, val) => acc + Math.pow(val - avg, 2), 0) / values.length;
    const stdDev = Math.sqrt(variance);

    // Calculate percentiles
    const sorted = [...values].sort((a, b) => a - b);
    const p10 = sorted[Math.floor(sorted.length * 0.1)];
    const p25 = sorted[Math.floor(sorted.length * 0.25)];
    const p50 = sorted[Math.floor(sorted.length * 0.5)];
    const p75 = sorted[Math.floor(sorted.length * 0.75)];
    const p90 = sorted[Math.floor(sorted.length * 0.9)];

    // Calculate positive/negative periods
    const positivePeriods = values.filter(v => v > 0).length;
    const negativePeriods = values.filter(v => v < 0).length;

    return {
      average: avg,
      maximum: max,
      minimum: min,
      latest: latest,
      stdDev: stdDev,
      sharpeRatio: avg / stdDev,
      percentiles: { p10, p25, p50, p75, p90 },
      positivePeriods,
      negativePeriods,
      totalPeriods: values.length,
      positivePercentage: (positivePeriods / values.length) * 100
    };
  }, [rollingReturns, rollingPeriod]);

  // Prepare chart data
  const chartData = useMemo(() => {
    if (!rollingReturns.length) return [];

    const periodKey = `return${rollingPeriod}` as keyof RollingReturn;
    
    // Get last 5 years of data for cleaner visualization
    const recentData = rollingReturns.slice(-1260);
    
    return recentData.map(item => ({
      date: new Date(item.date).toLocaleDateString('en-US', { 
        month: 'short', 
        year: '2-digit' 
      }),
      value: parseFloat((item[periodKey] as number).toFixed(2)),
      benchmark: selectedBenchmark
    }));
  }, [rollingReturns, rollingPeriod, selectedBenchmark]);

  // Distribution data for histogram
  const distributionData = useMemo(() => {
    if (!statistics) return [];

    const periodKey = `return${rollingPeriod}` as keyof RollingReturn;
    const values = rollingReturns
      .map(r => r[periodKey] as number)
      .filter(v => v !== 0);

    // Create bins
    const binCount = 20;
    const min = Math.floor(statistics.minimum);
    const max = Math.ceil(statistics.maximum);
    const binSize = (max - min) / binCount;

    const bins = Array(binCount).fill(0);
    values.forEach(value => {
      const binIndex = Math.min(
        Math.floor((value - min) / binSize),
        binCount - 1
      );
      bins[binIndex]++;
    });

    return bins.map((count, i) => ({
      range: `${(min + i * binSize).toFixed(1)}-${(min + (i + 1) * binSize).toFixed(1)}`,
      count: count,
      percentage: (count / values.length * 100).toFixed(1)
    }));
  }, [statistics, rollingReturns, rollingPeriod]);

  return (
    <div className="container mx-auto p-6 space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold">Benchmark Rolling Returns</h1>
          <p className="text-muted-foreground">
            Analyze historical rolling returns and performance statistics of market benchmarks
          </p>
        </div>
        <Button variant="outline" size="sm">
          <Download className="mr-2 h-4 w-4" />
          Export Data
        </Button>
      </div>

      {/* Benchmark Selection */}
      <Card>
        <CardHeader>
          <CardTitle>Select Benchmark</CardTitle>
          <CardDescription>
            Choose benchmark indices to analyze their rolling returns
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <label className="text-sm font-medium">Primary Benchmark</label>
              <Select value={selectedBenchmark} onValueChange={setSelectedBenchmark}>
                <SelectTrigger>
                  <SelectValue placeholder="Select a benchmark" />
                </SelectTrigger>
                <SelectContent>
                  {benchmarkList?.benchmarks?.map((benchmark: any) => (
                    <SelectItem key={benchmark.benchmark_name} value={benchmark.benchmark_name}>
                      {benchmark.benchmark_name} ({benchmark.fund_count} funds)
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <label className="text-sm font-medium">Compare With (Optional)</label>
              <Select value={selectedBenchmark2} onValueChange={setSelectedBenchmark2}>
                <SelectTrigger>
                  <SelectValue placeholder="Select benchmark to compare" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">None</SelectItem>
                  {benchmarkList?.benchmarks
                    ?.filter((b: any) => b.benchmark_name !== selectedBenchmark)
                    ?.map((benchmark: any) => (
                      <SelectItem key={benchmark.benchmark_name} value={benchmark.benchmark_name}>
                        {benchmark.benchmark_name}
                      </SelectItem>
                    ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Rolling Period Selection */}
          <div className="mt-6">
            <label className="text-sm font-medium mb-2 block">Rolling Period</label>
            <div className="flex gap-2">
              {['1Y', '3Y', '5Y', '7Y', '10Y'].map(period => (
                <Button
                  key={period}
                  variant={rollingPeriod === period ? "default" : "outline"}
                  size="sm"
                  onClick={() => setRollingPeriod(period)}
                >
                  {period}
                </Button>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Main Content Tabs */}
      <Tabs defaultValue="returns" className="space-y-4">
        <TabsList>
          <TabsTrigger value="returns">Rolling Returns</TabsTrigger>
          <TabsTrigger value="statistics">Statistics</TabsTrigger>
          <TabsTrigger value="distribution">Distribution</TabsTrigger>
          <TabsTrigger value="comparison">Comparison</TabsTrigger>
        </TabsList>

        {/* Rolling Returns Chart */}
        <TabsContent value="returns" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <TrendingUp className="h-5 w-5" />
                {rollingPeriod} Rolling Returns - {selectedBenchmark}
              </CardTitle>
              <CardDescription>
                Annualized returns calculated over {rollingPeriod} rolling periods
              </CardDescription>
            </CardHeader>
            <CardContent>
              {isLoadingData ? (
                <Skeleton className="h-96 w-full" />
              ) : chartData.length > 0 ? (
                <div className="h-96">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={chartData}>
                      <defs>
                        <linearGradient id="colorReturns" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#10B981" stopOpacity={0.8}/>
                          <stop offset="95%" stopColor="#10B981" stopOpacity={0}/>
                        </linearGradient>
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="date" 
                        interval="preserveStartEnd"
                        tick={{ fontSize: 12 }}
                      />
                      <YAxis 
                        label={{ value: 'Returns (%)', angle: -90, position: 'insideLeft' }}
                        tick={{ fontSize: 12 }}
                      />
                      <Tooltip 
                        formatter={(value: number) => `${value.toFixed(2)}%`}
                        labelFormatter={(label) => `Date: ${label}`}
                      />
                      <Area
                        type="monotone"
                        dataKey="value"
                        stroke="#10B981"
                        fillOpacity={1}
                        fill="url(#colorReturns)"
                        strokeWidth={2}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <Alert>
                  <Info className="h-4 w-4" />
                  <AlertDescription>
                    No data available for the selected benchmark. Historical data may need to be imported.
                  </AlertDescription>
                </Alert>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Statistics */}
        <TabsContent value="statistics" className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Summary Statistics */}
            <Card>
              <CardHeader>
                <CardTitle>Summary Statistics</CardTitle>
                <CardDescription>
                  Key metrics for {rollingPeriod} rolling returns
                </CardDescription>
              </CardHeader>
              <CardContent>
                {statistics ? (
                  <div className="space-y-4">
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Average Return</span>
                      <span className="font-semibold">{statistics.average.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Latest Return</span>
                      <span className="font-semibold flex items-center gap-1">
                        {statistics.latest.toFixed(2)}%
                        {statistics.latest > 0 ? (
                          <ArrowUpRight className="h-4 w-4 text-green-600" />
                        ) : (
                          <ArrowDownRight className="h-4 w-4 text-red-600" />
                        )}
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Maximum Return</span>
                      <span className="font-semibold text-green-600">{statistics.maximum.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Minimum Return</span>
                      <span className="font-semibold text-red-600">{statistics.minimum.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Standard Deviation</span>
                      <span className="font-semibold">{statistics.stdDev.toFixed(2)}%</span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm text-muted-foreground">Sharpe Ratio</span>
                      <span className="font-semibold">{statistics.sharpeRatio.toFixed(2)}</span>
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    No statistics available
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Period Analysis */}
            <Card>
              <CardHeader>
                <CardTitle>Period Analysis</CardTitle>
                <CardDescription>
                  Performance consistency over time
                </CardDescription>
              </CardHeader>
              <CardContent>
                {statistics ? (
                  <div className="space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Positive Periods</span>
                        <span className="font-semibold text-green-600">
                          {statistics.positivePeriods} ({statistics.positivePercentage.toFixed(1)}%)
                        </span>
                      </div>
                      <Progress 
                        value={statistics.positivePercentage} 
                        className="h-2"
                      />
                    </div>
                    
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Negative Periods</span>
                        <span className="font-semibold text-red-600">
                          {statistics.negativePeriods} ({(100 - statistics.positivePercentage).toFixed(1)}%)
                        </span>
                      </div>
                      <Progress 
                        value={100 - statistics.positivePercentage} 
                        className="h-2 bg-red-100"
                      />
                    </div>

                    <div className="pt-4 border-t">
                      <h4 className="font-medium mb-2">Percentile Distribution</h4>
                      <div className="space-y-2 text-sm">
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">90th Percentile</span>
                          <span>{statistics.percentiles.p90.toFixed(2)}%</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">75th Percentile</span>
                          <span>{statistics.percentiles.p75.toFixed(2)}%</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">Median (50th)</span>
                          <span className="font-semibold">{statistics.percentiles.p50.toFixed(2)}%</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">25th Percentile</span>
                          <span>{statistics.percentiles.p25.toFixed(2)}%</span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-muted-foreground">10th Percentile</span>
                          <span>{statistics.percentiles.p10.toFixed(2)}%</span>
                        </div>
                      </div>
                    </div>
                  </div>
                ) : (
                  <div className="text-center py-8 text-muted-foreground">
                    No statistics available
                  </div>
                )}
              </CardContent>
            </Card>
          </div>

          {/* Historical Performance Table */}
          <Card>
            <CardHeader>
              <CardTitle>Historical Performance Summary</CardTitle>
              <CardDescription>
                Rolling returns across different time periods
              </CardDescription>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Period</TableHead>
                    <TableHead className="text-right">Average</TableHead>
                    <TableHead className="text-right">Maximum</TableHead>
                    <TableHead className="text-right">Minimum</TableHead>
                    <TableHead className="text-right">Std Dev</TableHead>
                    <TableHead className="text-right">Latest</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {['1Y', '3Y', '5Y', '7Y', '10Y'].map(period => {
                    const periodKey = `return${period}` as keyof RollingReturn;
                    const values = rollingReturns
                      .map(r => r[periodKey] as number)
                      .filter(v => v !== 0);
                    
                    if (values.length === 0) return null;
                    
                    const avg = values.reduce((a, b) => a + b, 0) / values.length;
                    const max = Math.max(...values);
                    const min = Math.min(...values);
                    const latest = values[values.length - 1];
                    const variance = values.reduce((acc, val) => acc + Math.pow(val - avg, 2), 0) / values.length;
                    const stdDev = Math.sqrt(variance);
                    
                    return (
                      <TableRow key={period}>
                        <TableCell className="font-medium">{period}</TableCell>
                        <TableCell className="text-right">{avg.toFixed(2)}%</TableCell>
                        <TableCell className="text-right text-green-600">{max.toFixed(2)}%</TableCell>
                        <TableCell className="text-right text-red-600">{min.toFixed(2)}%</TableCell>
                        <TableCell className="text-right">{stdDev.toFixed(2)}%</TableCell>
                        <TableCell className="text-right font-semibold">
                          {latest.toFixed(2)}%
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Distribution */}
        <TabsContent value="distribution" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <BarChart3 className="h-5 w-5" />
                Return Distribution - {rollingPeriod} Rolling Returns
              </CardTitle>
              <CardDescription>
                Frequency distribution of rolling returns
              </CardDescription>
            </CardHeader>
            <CardContent>
              {distributionData.length > 0 ? (
                <div className="h-96">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={distributionData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis 
                        dataKey="range" 
                        angle={-45}
                        textAnchor="end"
                        height={80}
                        tick={{ fontSize: 11 }}
                      />
                      <YAxis 
                        label={{ value: 'Frequency', angle: -90, position: 'insideLeft' }}
                      />
                      <Tooltip 
                        formatter={(value: any, name: string) => [
                          name === 'count' ? `${value} periods` : `${value}%`,
                          name === 'count' ? 'Count' : 'Percentage'
                        ]}
                      />
                      <Bar dataKey="count" fill="#10B981" radius={[4, 4, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              ) : (
                <div className="text-center py-16 text-muted-foreground">
                  No distribution data available
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>

        {/* Comparison */}
        <TabsContent value="comparison" className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle>Benchmark Comparison</CardTitle>
              <CardDescription>
                Compare rolling returns between selected benchmarks
              </CardDescription>
            </CardHeader>
            <CardContent>
              {selectedBenchmark2 ? (
                <div className="space-y-4">
                  <Alert>
                    <Info className="h-4 w-4" />
                    <AlertDescription>
                      Comparison feature coming soon. Select a second benchmark to compare performance.
                    </AlertDescription>
                  </Alert>
                </div>
              ) : (
                <div className="text-center py-16 text-muted-foreground">
                  Select a second benchmark to compare performance
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Info Section */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Info className="h-5 w-5" />
            About Rolling Returns
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2 text-sm text-muted-foreground">
            <p>
              Rolling returns are calculated by taking the annualized returns for a specific period 
              (e.g., 1 year, 3 years) and calculating it for each day in the historical data.
            </p>
            <p>
              This analysis helps understand the consistency of returns over time and identifies 
              the best and worst periods for investment in the benchmark.
            </p>
            <p>
              Data shown is based on Total Return Index (TRI) values which include dividend reinvestment.
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}