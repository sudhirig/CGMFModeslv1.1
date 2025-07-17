import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { TrendingUp, Target, BarChart3, PieChart, Zap, Search, Activity, DollarSign, Star, Shield, Award } from 'lucide-react';
import { Progress } from '@/components/ui/progress';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend, BarChart, Bar, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar } from 'recharts';

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

  const getScoreColor = (score: number) => {
    if (score >= 80) return "text-green-600";
    if (score >= 60) return "text-blue-600";
    if (score >= 40) return "text-yellow-600";
    return "text-red-600";
  };

  const getScoreGradient = (score: number) => {
    if (score >= 80) return "from-green-500 to-green-600";
    if (score >= 60) return "from-blue-500 to-blue-600";
    if (score >= 40) return "from-yellow-500 to-yellow-600";
    return "from-red-500 to-red-600";
  };

  const getRadarData = () => {
    if (!riskMetrics) return [];
    
    return [
      { metric: 'Calmar', value: Math.min(5, Math.max(0, riskMetrics.calmarRatio || 0)), fullMark: 5 },
      { metric: 'Sortino', value: Math.min(3, Math.max(0, riskMetrics.sortinoRatio || 0)), fullMark: 3 },
      { metric: 'VaR', value: Math.min(10, Math.max(0, Math.abs(riskMetrics.var95 || 0))), fullMark: 10 },
      { metric: 'Volatility', value: Math.min(30, Math.max(0, riskMetrics.downsideDeviation || 0)), fullMark: 30 },
    ];
  };

  return (
    <div className="py-6 bg-gradient-to-br from-gray-50 to-white min-h-screen">
      <div className="px-4 sm:px-6 lg:px-8">
        {/* Enhanced Header */}
        <div className="mb-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-gradient-to-r from-purple-500 to-pink-600 rounded-2xl shadow-lg">
                <BarChart3 className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className="text-4xl font-bold bg-gradient-to-r from-gray-900 to-gray-600 bg-clip-text text-transparent">
                  Advanced Analytics Dashboard
                </h1>
                <p className="mt-2 text-lg text-gray-600">
                  Comprehensive fund analysis with advanced risk metrics and performance attribution
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* Enhanced System Overview */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          <Card className="bg-gradient-to-br from-blue-50 to-blue-100 border-blue-200">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-blue-800">Total Funds</CardTitle>
              <BarChart3 className="h-8 w-8 text-blue-600" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-blue-900">
                {overviewLoading ? '...' : fundOverview?.totalFunds?.toLocaleString() || '0'}
              </div>
              <p className="text-xs text-blue-700 mt-1">
                With complete scoring
              </p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-green-50 to-green-100 border-green-200">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-green-800">Risk Metrics</CardTitle>
              <Target className="h-8 w-8 text-green-600" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-green-900">11,482</div>
              <p className="text-xs text-green-700 mt-1">
                Calmar, Sortino, VaR
              </p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-purple-50 to-purple-100 border-purple-200">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-purple-800">Subcategory Rankings</CardTitle>
              <PieChart className="h-8 w-8 text-purple-600" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-purple-900">11,589</div>
              <p className="text-xs text-purple-700 mt-1">
                Peer comparisons
              </p>
            </CardContent>
          </Card>

          <Card className="bg-gradient-to-br from-amber-50 to-amber-100 border-amber-200">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium text-amber-800">Recommendations</CardTitle>
              <TrendingUp className="h-8 w-8 text-amber-600" />
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold text-amber-900">11,800</div>
              <p className="text-xs text-amber-700 mt-1">
                Investment signals
              </p>
            </CardContent>
          </Card>
        </div>

        {/* Enhanced Fund Analysis Input */}
        <Card className="mb-8 shadow-lg border-0">
          <CardHeader>
            <CardTitle className="flex items-center space-x-2">
              <Search className="w-5 h-5 text-blue-600" />
              <span>Fund Analysis</span>
            </CardTitle>
            <CardDescription>
              Enter a fund ID to view comprehensive analytics including advanced risk metrics, peer comparisons, and performance attribution
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex space-x-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-3 h-4 w-4 text-gray-400" />
                <Input
                  placeholder="Enter Fund ID (e.g., 1234)"
                  value={fundId}
                  onChange={(e) => setFundId(e.target.value)}
                  className="pl-10 h-12 text-lg"
                />
              </div>
              <Button 
                onClick={handleAnalyze} 
                disabled={!fundId}
                className="h-12 px-8 bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700"
              >
                <Zap className="w-4 h-4 mr-2" />
                Analyze Fund
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Enhanced Analysis Results */}
        {selectedFundId && (
          <Tabs defaultValue="risk-metrics" className="space-y-6">
            <TabsList className="grid w-full grid-cols-3 bg-white rounded-lg shadow-sm border">
              <TabsTrigger value="risk-metrics" className="flex items-center space-x-2">
                <Shield className="w-4 h-4" />
                <span>Risk Metrics</span>
              </TabsTrigger>
              <TabsTrigger value="subcategory" className="flex items-center space-x-2">
                <Target className="w-4 h-4" />
                <span>Peer Analysis</span>
              </TabsTrigger>
              <TabsTrigger value="attribution" className="flex items-center space-x-2">
                <Activity className="w-4 h-4" />
                <span>Performance</span>
              </TabsTrigger>
            </TabsList>

            <TabsContent value="risk-metrics">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <Card className="bg-gradient-to-br from-blue-50 to-indigo-50 border-blue-200">
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Shield className="w-5 h-5 text-blue-600" />
                      <span>Advanced Risk Metrics</span>
                    </CardTitle>
                    <CardDescription>
                      Comprehensive risk analysis using authentic NAV data
                    </CardDescription>
                  </CardHeader>
                  <CardContent>
                    {riskLoading ? (
                      <div className="flex justify-center items-center h-48">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
                      </div>
                    ) : riskMetrics ? (
                      <div className="grid grid-cols-2 gap-6">
                        <div className="space-y-3">
                          <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                            <h4 className="font-medium text-gray-700">Calmar Ratio</h4>
                            <p className="text-2xl font-bold text-blue-600">
                              {riskMetrics.calmarRatio?.toFixed(3) || 'N/A'}
                            </p>
                            <p className="text-xs text-gray-500">
                              Return/Max Drawdown
                            </p>
                          </div>
                          <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                            <h4 className="font-medium text-gray-700">Sortino Ratio</h4>
                            <p className="text-2xl font-bold text-green-600">
                              {riskMetrics.sortinoRatio?.toFixed(3) || 'N/A'}
                            </p>
                            <p className="text-xs text-gray-500">
                              Downside Risk Adjusted
                            </p>
                          </div>
                        </div>
                        <div className="space-y-3">
                          <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                            <h4 className="font-medium text-gray-700">VaR (95%)</h4>
                            <p className="text-2xl font-bold text-red-600">
                              {riskMetrics.var95?.toFixed(2) || 'N/A'}%
                            </p>
                            <p className="text-xs text-gray-500">
                              Value at Risk
                            </p>
                          </div>
                          <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                            <h4 className="font-medium text-gray-700">Downside Deviation</h4>
                            <p className="text-2xl font-bold text-orange-600">
                              {riskMetrics.downsideDeviation?.toFixed(2) || 'N/A'}%
                            </p>
                            <p className="text-xs text-gray-500">
                              Negative Volatility
                            </p>
                          </div>
                        </div>
                      </div>
                    ) : (
                      <div className="text-center py-8">
                        <Shield className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                        <p className="text-gray-500">No risk metrics data available for this fund.</p>
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Target className="w-5 h-5 text-purple-600" />
                      <span>Risk Analysis Radar</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    {riskMetrics ? (
                      <div className="h-80">
                        <ResponsiveContainer width="100%" height="100%">
                          <RadarChart data={getRadarData()}>
                            <PolarGrid />
                            <PolarAngleAxis dataKey="metric" />
                            <PolarRadiusAxis angle={90} domain={[0, 'dataMax']} />
                            <Radar
                              name="Risk Metrics"
                              dataKey="value"
                              stroke="#8B5CF6"
                              fill="#8B5CF6"
                              fillOpacity={0.3}
                              strokeWidth={2}
                            />
                            <Tooltip />
                          </RadarChart>
                        </ResponsiveContainer>
                      </div>
                    ) : (
                      <div className="text-center py-12">
                        <BarChart3 className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                        <p className="text-gray-500">Risk analysis will appear here</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="subcategory">
              <Card className="bg-gradient-to-br from-green-50 to-emerald-50 border-green-200">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Target className="w-5 h-5 text-green-600" />
                    <span>Subcategory Analysis</span>
                  </CardTitle>
                  <CardDescription>
                    Peer comparison within fund subcategory
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {subcategoryLoading ? (
                    <div className="flex justify-center items-center h-48">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-green-600"></div>
                    </div>
                  ) : subcategoryData ? (
                    <div className="space-y-6">
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                          <h4 className="font-medium text-gray-700">Subcategory Rank</h4>
                          <p className="text-3xl font-bold text-green-600">
                            {subcategoryData.fund?.subcategory_rank || 'N/A'}
                          </p>
                          <p className="text-xs text-gray-500">
                            of {subcategoryData.fund?.subcategory_total || 'N/A'} funds
                          </p>
                        </div>
                        <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                          <h4 className="font-medium text-gray-700">Percentile</h4>
                          <p className="text-3xl font-bold text-blue-600">
                            {subcategoryData.fund?.subcategory_percentile?.toFixed(1) || 'N/A'}%
                          </p>
                          <p className="text-xs text-gray-500">
                            Top percentile
                          </p>
                        </div>
                        <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                          <h4 className="font-medium text-gray-700">Quartile</h4>
                          <p className="text-3xl font-bold text-purple-600">
                            Q{subcategoryData.fund?.subcategory_quartile || 'N/A'}
                          </p>
                          <p className="text-xs text-gray-500">
                            Performance quartile
                          </p>
                        </div>
                        <div className="text-center p-4 bg-white rounded-lg shadow-sm">
                          <h4 className="font-medium text-gray-700">Total Score</h4>
                          <p className="text-3xl font-bold text-orange-600">
                            {subcategoryData.fund?.total_score?.toFixed(1) || 'N/A'}
                          </p>
                          <p className="text-xs text-gray-500">
                            Overall rating
                          </p>
                        </div>
                      </div>

                      <div className="bg-white rounded-lg p-4 shadow-sm">
                        <h4 className="font-medium text-gray-700 mb-2">Subcategory: {subcategoryData.subcategory}</h4>
                        <Badge variant="outline" className="mb-4">
                          {subcategoryData.fund?.fund_name}
                        </Badge>
                      </div>

                      <div className="bg-white rounded-lg p-4 shadow-sm">
                        <h4 className="font-medium text-gray-700 mb-4">Top Performing Peers</h4>
                        <div className="space-y-3">
                          {subcategoryData.peers?.slice(0, 5).map((peer: any, index: number) => (
                            <div key={peer.fund_id} className="flex justify-between items-center p-3 bg-gray-50 rounded-lg">
                              <span className="text-sm font-medium">{peer.fund_name}</span>
                              <div className="flex space-x-2">
                                <Badge variant="secondary">#{peer.subcategory_rank}</Badge>
                                <Badge className="bg-green-100 text-green-800">{peer.total_score?.toFixed(1)}</Badge>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="text-center py-12">
                      <Target className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                      <p className="text-gray-500">No subcategory analysis data available for this fund.</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>

            <TabsContent value="attribution">
              <Card className="bg-gradient-to-br from-purple-50 to-indigo-50 border-purple-200">
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <Activity className="w-5 h-5 text-purple-600" />
                    <span>Performance Attribution</span>
                  </CardTitle>
                  <CardDescription>
                    Benchmark comparison and sector attribution analysis
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  {attributionLoading ? (
                    <div className="flex justify-center items-center h-48">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-600"></div>
                    </div>
                  ) : attributionData ? (
                    <div className="space-y-6">
                      {attributionData.benchmarkAttribution && (
                        <div className="bg-white rounded-lg p-6 shadow-sm">
                          <h4 className="font-medium text-gray-700 mb-4">Benchmark Comparison</h4>
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div className="text-center p-4 bg-gray-50 rounded-lg">
                              <h5 className="text-sm font-medium text-gray-600">Fund Return</h5>
                              <p className="text-2xl font-bold text-blue-600">
                                {attributionData.benchmarkAttribution.fund_return?.toFixed(2) || 'N/A'}%
                              </p>
                            </div>
                            <div className="text-center p-4 bg-gray-50 rounded-lg">
                              <h5 className="text-sm font-medium text-gray-600">Benchmark Return</h5>
                              <p className="text-2xl font-bold text-gray-600">
                                {attributionData.benchmarkAttribution.benchmark_return?.toFixed(2) || 'N/A'}%
                              </p>
                            </div>
                            <div className="text-center p-4 bg-gray-50 rounded-lg">
                              <h5 className="text-sm font-medium text-gray-600">Excess Return</h5>
                              <p className={`text-2xl font-bold ${
                                (attributionData.benchmarkAttribution.excess_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'
                              }`}>
                                {attributionData.benchmarkAttribution.excess_return?.toFixed(2) || 'N/A'}%
                              </p>
                            </div>
                          </div>
                        </div>
                      )}

                      {attributionData.sectorAttribution && (
                        <div className="bg-white rounded-lg p-6 shadow-sm">
                          <h4 className="font-medium text-gray-700 mb-4">Sector Attribution</h4>
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            <div className="space-y-2">
                              <h5 className="text-sm font-medium text-gray-600">Category</h5>
                              <p className="text-lg font-semibold text-gray-900">{attributionData.sectorAttribution.category}</p>
                            </div>
                            <div className="space-y-2">
                              <h5 className="text-sm font-medium text-gray-600">Subcategory</h5>
                              <p className="text-lg font-semibold text-gray-900">{attributionData.sectorAttribution.subcategory}</p>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="text-center py-12">
                      <Activity className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                      <p className="text-gray-500">No performance attribution data available for this fund.</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        )}
      </div>
    </div>
  );
}