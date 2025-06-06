import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { TrendingUp, Target, BarChart3, PieChart } from 'lucide-react';

export default function AdvancedAnalyticsPage() {
  const [fundId, setFundId] = useState<string>('');
  const [selectedFundId, setSelectedFundId] = useState<number | null>(null);

  // Fetch fund overview data
  const { data: fundOverview, isLoading: overviewLoading } = useQuery({
    queryKey: ['/api/funds/stats'],
    enabled: true
  });

  // Fetch advanced risk metrics for selected fund
  const { data: riskMetrics, isLoading: riskLoading } = useQuery({
    queryKey: ['/api/advanced-analytics/risk-metrics', selectedFundId],
    enabled: !!selectedFundId
  });

  // Fetch subcategory analysis for selected fund
  const { data: subcategoryData, isLoading: subcategoryLoading } = useQuery({
    queryKey: ['/api/subcategory-analysis', selectedFundId],
    enabled: !!selectedFundId
  });

  // Fetch performance attribution for selected fund
  const { data: attributionData, isLoading: attributionLoading } = useQuery({
    queryKey: ['/api/performance-attribution', selectedFundId],
    enabled: !!selectedFundId
  });

  const handleAnalyze = () => {
    const id = parseInt(fundId);
    if (!isNaN(id) && id > 0) {
      setSelectedFundId(id);
    }
  };

  return (
    <div className="container mx-auto p-6 space-y-6">
      <div className="flex flex-col space-y-4">
        <h1 className="text-3xl font-bold">Advanced Analytics Dashboard</h1>
        <p className="text-muted-foreground">
          Comprehensive fund analysis with advanced risk metrics, subcategory rankings, and performance attribution
        </p>
      </div>

      {/* System Overview */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Funds</CardTitle>
            <BarChart3 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {overviewLoading ? '...' : fundOverview?.totalFunds?.toLocaleString() || '0'}
            </div>
            <p className="text-xs text-muted-foreground">
              With complete scoring
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Risk Metrics</CardTitle>
            <Target className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">11,482</div>
            <p className="text-xs text-muted-foreground">
              Calmar, Sortino, VaR
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Subcategory Rankings</CardTitle>
            <PieChart className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">11,589</div>
            <p className="text-xs text-muted-foreground">
              Peer comparisons
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Recommendations</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">11,800</div>
            <p className="text-xs text-muted-foreground">
              Investment signals
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Fund Analysis Input */}
      <Card>
        <CardHeader>
          <CardTitle>Fund Analysis</CardTitle>
          <CardDescription>
            Enter a fund ID to view comprehensive analytics including advanced risk metrics, peer comparisons, and performance attribution
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex space-x-2">
            <Input
              placeholder="Enter Fund ID (e.g., 1234)"
              value={fundId}
              onChange={(e) => setFundId(e.target.value)}
              className="flex-1"
            />
            <Button onClick={handleAnalyze} disabled={!fundId}>
              Analyze Fund
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Analysis Results */}
      {selectedFundId && (
        <Tabs defaultValue="risk-metrics" className="space-y-4">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="risk-metrics">Advanced Risk Metrics</TabsTrigger>
            <TabsTrigger value="subcategory">Subcategory Analysis</TabsTrigger>
            <TabsTrigger value="attribution">Performance Attribution</TabsTrigger>
          </TabsList>

          <TabsContent value="risk-metrics">
            <Card>
              <CardHeader>
                <CardTitle>Advanced Risk Metrics</CardTitle>
                <CardDescription>
                  Comprehensive risk analysis using authentic NAV data
                </CardDescription>
              </CardHeader>
              <CardContent>
                {riskLoading ? (
                  <p>Loading risk metrics...</p>
                ) : riskMetrics ? (
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="space-y-2">
                      <h4 className="font-medium">Calmar Ratio</h4>
                      <p className="text-2xl font-bold">
                        {riskMetrics.calmarRatio?.toFixed(3) || 'N/A'}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Return/Max Drawdown
                      </p>
                    </div>
                    <div className="space-y-2">
                      <h4 className="font-medium">Sortino Ratio</h4>
                      <p className="text-2xl font-bold">
                        {riskMetrics.sortinoRatio?.toFixed(3) || 'N/A'}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Downside Risk Adjusted
                      </p>
                    </div>
                    <div className="space-y-2">
                      <h4 className="font-medium">VaR (95%)</h4>
                      <p className="text-2xl font-bold">
                        {riskMetrics.var95?.toFixed(2) || 'N/A'}%
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Value at Risk
                      </p>
                    </div>
                    <div className="space-y-2">
                      <h4 className="font-medium">Downside Deviation</h4>
                      <p className="text-2xl font-bold">
                        {riskMetrics.downsideDeviation?.toFixed(2) || 'N/A'}%
                      </p>
                      <p className="text-xs text-muted-foreground">
                        Negative Volatility
                      </p>
                    </div>
                  </div>
                ) : (
                  <p>No risk metrics data available for this fund.</p>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="subcategory">
            <Card>
              <CardHeader>
                <CardTitle>Subcategory Analysis</CardTitle>
                <CardDescription>
                  Peer comparison within fund subcategory
                </CardDescription>
              </CardHeader>
              <CardContent>
                {subcategoryLoading ? (
                  <p>Loading subcategory analysis...</p>
                ) : subcategoryData ? (
                  <div className="space-y-6">
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                      <div className="space-y-2">
                        <h4 className="font-medium">Subcategory Rank</h4>
                        <p className="text-2xl font-bold">
                          {subcategoryData.fund?.subcategory_rank || 'N/A'}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          of {subcategoryData.fund?.subcategory_total || 'N/A'} funds
                        </p>
                      </div>
                      <div className="space-y-2">
                        <h4 className="font-medium">Percentile</h4>
                        <p className="text-2xl font-bold">
                          {subcategoryData.fund?.subcategory_percentile?.toFixed(1) || 'N/A'}%
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Top percentile
                        </p>
                      </div>
                      <div className="space-y-2">
                        <h4 className="font-medium">Quartile</h4>
                        <p className="text-2xl font-bold">
                          Q{subcategoryData.fund?.subcategory_quartile || 'N/A'}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Performance quartile
                        </p>
                      </div>
                      <div className="space-y-2">
                        <h4 className="font-medium">Total Score</h4>
                        <p className="text-2xl font-bold">
                          {subcategoryData.fund?.total_score?.toFixed(1) || 'N/A'}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          Overall rating
                        </p>
                      </div>
                    </div>

                    <div className="space-y-2">
                      <h4 className="font-medium">Subcategory: {subcategoryData.subcategory}</h4>
                      <Badge variant="outline">
                        {subcategoryData.fund?.fund_name}
                      </Badge>
                    </div>

                    <div className="space-y-2">
                      <h4 className="font-medium">Top Performing Peers</h4>
                      <div className="space-y-2">
                        {subcategoryData.peers?.slice(0, 5).map((peer: any, index: number) => (
                          <div key={peer.fund_id} className="flex justify-between items-center p-2 border rounded">
                            <span className="text-sm">{peer.fund_name}</span>
                            <div className="flex space-x-2">
                              <Badge variant="secondary">#{peer.subcategory_rank}</Badge>
                              <Badge>{peer.total_score?.toFixed(1)}</Badge>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                ) : (
                  <p>No subcategory analysis data available for this fund.</p>
                )}
              </CardContent>
            </Card>
          </TabsContent>

          <TabsContent value="attribution">
            <Card>
              <CardHeader>
                <CardTitle>Performance Attribution</CardTitle>
                <CardDescription>
                  Benchmark comparison and sector attribution analysis
                </CardDescription>
              </CardHeader>
              <CardContent>
                {attributionLoading ? (
                  <p>Loading performance attribution...</p>
                ) : attributionData ? (
                  <div className="space-y-6">
                    {attributionData.benchmarkAttribution && (
                      <div className="space-y-4">
                        <h4 className="font-medium">Benchmark Comparison</h4>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                          <div className="space-y-2">
                            <h5 className="text-sm font-medium">Fund Return</h5>
                            <p className="text-xl font-bold">
                              {attributionData.benchmarkAttribution.fund_return?.toFixed(2) || 'N/A'}%
                            </p>
                          </div>
                          <div className="space-y-2">
                            <h5 className="text-sm font-medium">Benchmark Return</h5>
                            <p className="text-xl font-bold">
                              {attributionData.benchmarkAttribution.benchmark_return?.toFixed(2) || 'N/A'}%
                            </p>
                          </div>
                          <div className="space-y-2">
                            <h5 className="text-sm font-medium">Excess Return</h5>
                            <p className={`text-xl font-bold ${
                              (attributionData.benchmarkAttribution.excess_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'
                            }`}>
                              {attributionData.benchmarkAttribution.excess_return?.toFixed(2) || 'N/A'}%
                            </p>
                          </div>
                        </div>
                      </div>
                    )}

                    {attributionData.sectorAttribution && (
                      <div className="space-y-4">
                        <h4 className="font-medium">Sector Attribution</h4>
                        <div className="grid grid-cols-2 gap-4">
                          <div className="space-y-2">
                            <h5 className="text-sm font-medium">Category</h5>
                            <p className="font-medium">{attributionData.sectorAttribution.category}</p>
                          </div>
                          <div className="space-y-2">
                            <h5 className="text-sm font-medium">Subcategory</h5>
                            <p className="font-medium">{attributionData.sectorAttribution.subcategory}</p>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                ) : (
                  <p>No performance attribution data available for this fund.</p>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
}