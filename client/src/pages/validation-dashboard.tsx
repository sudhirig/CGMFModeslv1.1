import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { useToast } from "@/hooks/use-toast";
import { TrendingUp, TrendingDown, Target, BarChart3, Play, Calendar } from "lucide-react";
import { useState } from "react";

interface ValidationSummary {
  validation_run_id: string;
  run_date: string;
  total_funds_tested: number;
  validation_period_months: number;
  overall_prediction_accuracy_3m: number;
  overall_prediction_accuracy_6m: number;
  overall_prediction_accuracy_1y: number;
  overall_score_correlation_3m: number;
  overall_score_correlation_6m: number;
  overall_score_correlation_1y: number;
  quartile_stability_3m: number;
  quartile_stability_6m: number;
  quartile_stability_1y: number;
  strong_buy_accuracy: number;
  buy_accuracy: number;
  hold_accuracy: number;
  sell_accuracy: number;
  strong_sell_accuracy: number;
  validation_status: string;
}

interface ValidationDetail {
  fund_id: number;
  fund_name: string;
  category: string;
  historical_total_score: number;
  historical_recommendation: string;
  historical_quartile: number;
  actual_return_3m: number;
  actual_return_6m: number;
  actual_return_1y: number;
  prediction_accuracy_3m: boolean;
  prediction_accuracy_6m: boolean;
  prediction_accuracy_1y: boolean;
  score_correlation_3m: number;
  score_correlation_6m: number;
  score_correlation_1y: number;
  quartile_maintained_3m: boolean;
  quartile_maintained_6m: boolean;
  quartile_maintained_1y: boolean;
}

export default function ValidationDashboard() {
  const [startDate, setStartDate] = useState("2023-01-01");
  const [endDate, setEndDate] = useState("2024-01-01");
  const [validationPeriodMonths, setValidationPeriodMonths] = useState(12);
  const [minimumDataPoints, setMinimumDataPoints] = useState(252);

  const { toast } = useToast();
  const queryClient = useQueryClient();

  const { data: validationResults, isLoading: resultsLoading } = useQuery<ValidationSummary[]>({
    queryKey: ["/api/validation/results"],
  });

  const { data: validationDetails, isLoading: detailsLoading } = useQuery<ValidationDetail[]>({
    queryKey: ["/api/validation/details/VALIDATION_RUN_2025_06_05"],
  });

  const runHistoricalValidation = useMutation({
    mutationFn: async (validationConfig: any) => {
      const response = await fetch("/api/validation/run-historical", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(validationConfig),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.message || "Failed to run historical validation");
      }

      return response.json();
    },
    onSuccess: (data) => {
      toast({
        title: "Historical Validation Started",
        description: `Validation run ${data.validationRunId} completed successfully with ${data.summary.totalFundsTested} funds tested.`,
      });
      queryClient.invalidateQueries({ queryKey: ["/api/validation/results"] });
    },
    onError: (error: any) => {
      toast({
        title: "Validation Failed",
        description: error.message || "Failed to run historical validation",
        variant: "destructive",
      });
    },
  });

  const handleRunHistoricalValidation = () => {
    runHistoricalValidation.mutate({
      startDate,
      endDate,
      validationPeriodMonths,
      minimumDataPoints,
    });
  };

  const latestResult = validationResults?.[0];

  if (resultsLoading) {
    return (
      <div className="container mx-auto p-6">
        <div className="flex items-center justify-center h-64">
          <div className="text-lg">Loading validation results...</div>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Backtesting Validation Dashboard</h1>
          <p className="text-muted-foreground mt-2">
            Historical validation of scoring accuracy and prediction performance
          </p>
        </div>
        <Badge variant="outline" className="text-sm">
          {latestResult?.validation_status || "No Data"}
        </Badge>
      </div>

      {latestResult && (
        <>
          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Funds Tested</CardTitle>
                <BarChart3 className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">{latestResult.total_funds_tested}</div>
                <p className="text-xs text-muted-foreground">
                  {latestResult.validation_period_months} month validation
                </p>
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">3M Accuracy</CardTitle>
                <Target className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {(latestResult.overall_prediction_accuracy_3m !== null && latestResult.overall_prediction_accuracy_3m !== undefined) ? Number(latestResult.overall_prediction_accuracy_3m).toFixed(1) : '0.0'}%
                </div>
                <Progress value={latestResult.overall_prediction_accuracy_3m || 0} className="mt-2" />
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">6M Accuracy</CardTitle>
                <TrendingUp className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {latestResult.overall_prediction_accuracy_6m ? Number(latestResult.overall_prediction_accuracy_6m).toFixed(1) : '0.0'}%
                </div>
                <Progress value={latestResult.overall_prediction_accuracy_6m || 0} className="mt-2" />
              </CardContent>
            </Card>

            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">1Y Accuracy</CardTitle>
                <TrendingDown className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-2xl font-bold">
                  {latestResult.overall_prediction_accuracy_1y ? Number(latestResult.overall_prediction_accuracy_1y).toFixed(1) : '0.0'}%
                </div>
                <Progress value={latestResult.overall_prediction_accuracy_1y || 0} className="mt-2" />
              </CardContent>
            </Card>
          </div>

          {/* Detailed Analysis */}
          <Tabs defaultValue="overview" className="space-y-4">
            <TabsList>
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="historical">Historical Validation</TabsTrigger>
              <TabsTrigger value="recommendations">Recommendation Accuracy</TabsTrigger>
              <TabsTrigger value="details">Fund Details</TabsTrigger>
            </TabsList>

            <TabsContent value="overview" className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle>Score Correlation</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between items-center">
                      <span className="text-sm">3 Month</span>
                      <span className="font-medium">
                        {latestResult.overall_score_correlation_3m ? Number(latestResult.overall_score_correlation_3m).toFixed(3) : '0.000'}
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">6 Month</span>
                      <span className="font-medium">
                        {latestResult.overall_score_correlation_6m ? Number(latestResult.overall_score_correlation_6m).toFixed(3) : '0.000'}
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">1 Year</span>
                      <span className="font-medium">
                        {latestResult.overall_score_correlation_1y ? Number(latestResult.overall_score_correlation_1y).toFixed(3) : '0.000'}
                      </span>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Quartile Stability</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <div className="flex justify-between items-center">
                      <span className="text-sm">3 Month</span>
                      <span className="font-medium">
                        {latestResult.quartile_stability_3m ? Number(latestResult.quartile_stability_3m).toFixed(1) : '0.0'}%
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">6 Month</span>
                      <span className="font-medium">
                        {latestResult.quartile_stability_6m ? Number(latestResult.quartile_stability_6m).toFixed(1) : '0.0'}%
                      </span>
                    </div>
                    <div className="flex justify-between items-center">
                      <span className="text-sm">1 Year</span>
                      <span className="font-medium">
                        {latestResult.quartile_stability_1y ? Number(latestResult.quartile_stability_1y).toFixed(1) : '0.0'}%
                      </span>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="historical" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Calendar className="h-5 w-5" />
                    Run Historical Validation
                  </CardTitle>
                  <p className="text-sm text-muted-foreground">
                    Execute point-in-time historical validation using the original documentation methodology
                  </p>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label htmlFor="start-date">Start Date</Label>
                      <Input
                        id="start-date"
                        type="date"
                        value={startDate}
                        onChange={(e) => setStartDate(e.target.value)}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="end-date">End Date</Label>
                      <Input
                        id="end-date"
                        type="date"
                        value={endDate}
                        onChange={(e) => setEndDate(e.target.value)}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="validation-period">Validation Period (months)</Label>
                      <Input
                        id="validation-period"
                        type="number"
                        min="1"
                        max="60"
                        value={validationPeriodMonths}
                        onChange={(e) => setValidationPeriodMonths(Number(e.target.value))}
                      />
                    </div>
                    <div className="space-y-2">
                      <Label htmlFor="minimum-data">Minimum Data Points</Label>
                      <Input
                        id="minimum-data"
                        type="number"
                        min="90"
                        max="1000"
                        value={minimumDataPoints}
                        onChange={(e) => setMinimumDataPoints(Number(e.target.value))}
                      />
                    </div>
                  </div>
                  
                  <div className="flex items-center justify-between pt-4">
                    <div className="text-sm text-muted-foreground">
                      This will run point-in-time scoring validation using only authentic historical data
                    </div>
                    <Button 
                      onClick={handleRunHistoricalValidation}
                      disabled={runHistoricalValidation.isPending}
                      className="flex items-center gap-2"
                    >
                      <Play className="h-4 w-4" />
                      {runHistoricalValidation.isPending ? "Running..." : "Start Validation"}
                    </Button>
                  </div>
                </CardContent>
              </Card>

              {/* Validation Progress/Results */}
              {runHistoricalValidation.isPending && (
                <Card>
                  <CardContent className="pt-6">
                    <div className="flex items-center space-x-4">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
                      <div>
                        <p className="text-sm font-medium">Running Historical Validation</p>
                        <p className="text-xs text-muted-foreground">
                          Processing point-in-time scoring and validation metrics...
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Enhanced Validation Features */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Point-in-Time Scoring</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-xs text-muted-foreground">
                      Calculates fund scores using only data available up to historical scoring dates, preventing look-ahead bias
                    </p>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Prediction Accuracy</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-xs text-muted-foreground">
                      Validates scoring accuracy across 3M, 6M, and 1Y periods with comprehensive performance tracking
                    </p>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardHeader>
                    <CardTitle className="text-sm">Quartile Stability</CardTitle>
                  </CardHeader>
                  <CardContent>
                    <p className="text-xs text-muted-foreground">
                      Tracks quartile maintenance over time and validates recommendation accuracy by type
                    </p>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="recommendations" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle>Recommendation Accuracy by Type</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-600">
                        {latestResult.strong_buy_accuracy ? Number(latestResult.strong_buy_accuracy).toFixed(1) : '0.0'}%
                      </div>
                      <div className="text-sm text-muted-foreground">Strong Buy</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-green-500">
                        {latestResult.buy_accuracy ? Number(latestResult.buy_accuracy).toFixed(1) : '0.0'}%
                      </div>
                      <div className="text-sm text-muted-foreground">Buy</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-yellow-500">
                        {latestResult.hold_accuracy ? Number(latestResult.hold_accuracy).toFixed(1) : '0.0'}%
                      </div>
                      <div className="text-sm text-muted-foreground">Hold</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-orange-500">
                        {latestResult.sell_accuracy ? Number(latestResult.sell_accuracy).toFixed(1) : '0.0'}%
                      </div>
                      <div className="text-sm text-muted-foreground">Sell</div>
                    </div>
                    <div className="text-center">
                      <div className="text-2xl font-bold text-red-500">
                        {latestResult.strong_sell_accuracy ? Number(latestResult.strong_sell_accuracy).toFixed(1) : '0.0'}%
                      </div>
                      <div className="text-sm text-muted-foreground">Strong Sell</div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardHeader>
                  <CardTitle>Individual Fund Validation Results</CardTitle>
                </CardHeader>
                <CardContent>
                  {detailsLoading ? (
                    <div className="text-center py-4">Loading fund details...</div>
                  ) : (
                    <Table>
                      <TableHeader>
                        <TableRow>
                          <TableHead>Fund Name</TableHead>
                          <TableHead>Category</TableHead>
                          <TableHead>Historical Score</TableHead>
                          <TableHead>Recommendation</TableHead>
                          <TableHead>3M Accuracy</TableHead>
                          <TableHead>6M Accuracy</TableHead>
                          <TableHead>1Y Accuracy</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {validationDetails?.slice(0, 10).map((detail) => (
                          <TableRow key={detail.fund_id}>
                            <TableCell className="font-medium max-w-[200px] truncate">
                              {detail.fund_name}
                            </TableCell>
                            <TableCell>{detail.category}</TableCell>
                            <TableCell>{detail.historical_total_score ? Number(detail.historical_total_score).toFixed(1) : '0.0'}</TableCell>
                            <TableCell>
                              <Badge
                                variant={
                                  detail.historical_recommendation === "STRONG_BUY" ||
                                  detail.historical_recommendation === "BUY"
                                    ? "default"
                                    : detail.historical_recommendation === "HOLD"
                                    ? "secondary"
                                    : "destructive"
                                }
                              >
                                {detail.historical_recommendation}
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <Badge variant={detail.prediction_accuracy_3m ? "default" : "secondary"}>
                                {detail.prediction_accuracy_3m ? "✓" : "✗"}
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <Badge variant={detail.prediction_accuracy_6m ? "default" : "secondary"}>
                                {detail.prediction_accuracy_6m ? "✓" : "✗"}
                              </Badge>
                            </TableCell>
                            <TableCell>
                              <Badge variant={detail.prediction_accuracy_1y ? "default" : "secondary"}>
                                {detail.prediction_accuracy_1y ? "✓" : "✗"}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
}