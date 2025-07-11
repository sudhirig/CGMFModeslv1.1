import React, { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { TrendingUp, TrendingDown, BarChart3, Activity, Target, DollarSign } from "lucide-react";
import MarketPerformanceChart from "@/components/charts/market-performance-chart";
import { useQuery } from "@tanstack/react-query";

export default function MarketOverview() {
  const [timeframe, setTimeframe] = useState<string>("weekly");
  
  const { data: marketIndices, isLoading, error } = useQuery({
    queryKey: ["/api/market/indices"],
    staleTime: 5 * 60 * 1000, // 5 minutes
  });
  
  // Find specific indices with proper typing
  const nifty50 = Array.isArray(marketIndices) ? marketIndices.find((index: any) => index.indexName === "NIFTY 50") : null;
  const niftyMidCap = Array.isArray(marketIndices) ? marketIndices.find((index: any) => index.indexName === "NIFTY MIDCAP 100") : null;
  const indiaVix = Array.isArray(marketIndices) ? marketIndices.find((index: any) => index.indexName === "INDIA VIX") : null;
  
  return (
    <div className="px-4 sm:px-6 lg:px-8 mb-6">
      <Card className="bg-gradient-to-r from-green-50 to-emerald-50 border-0 shadow-lg">
        <CardContent className="p-6">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6">
            <div className="flex items-center space-x-3">
              <div className="p-2 bg-green-100 rounded-lg">
                <BarChart3 className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-gray-900">Market Overview</h2>
                <p className="text-sm text-gray-600">Real-time market indices and performance tracking</p>
              </div>
            </div>
            <div className="flex items-center space-x-2 mt-4 sm:mt-0">
              <Button 
                variant={timeframe === "daily" ? "default" : "outline"}
                size="sm"
                onClick={() => setTimeframe("daily")}
                className={timeframe === "daily" ? "bg-green-600 hover:bg-green-700" : ""}
              >
                Daily
              </Button>
              <Button 
                variant={timeframe === "weekly" ? "default" : "outline"}
                size="sm"
                onClick={() => setTimeframe("weekly")}
                className={timeframe === "weekly" ? "bg-green-600 hover:bg-green-700" : ""}
              >
                Weekly
              </Button>
              <Button 
                variant={timeframe === "monthly" ? "default" : "outline"}
                size="sm"
                onClick={() => setTimeframe("monthly")}
                className={timeframe === "monthly" ? "bg-green-600 hover:bg-green-700" : ""}
              >
                Monthly
              </Button>
              <Button 
                variant={timeframe === "yearly" ? "default" : "outline"}
                size="sm"
                onClick={() => setTimeframe("yearly")}
                className={timeframe === "yearly" ? "bg-green-600 hover:bg-green-700" : ""}
              >
                Yearly
              </Button>
            </div>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            {isLoading ? (
              <>
                <Skeleton className="h-28 w-full" />
                <Skeleton className="h-28 w-full" />
                <Skeleton className="h-28 w-full" />
              </>
            ) : error ? (
              <div className="col-span-3 text-red-500 text-center py-8">Error loading market data</div>
            ) : (
              <>
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-all duration-300">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-2">
                        <div className="p-2 bg-blue-100 rounded-lg">
                          <Activity className="w-5 h-5 text-blue-600" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-gray-600">NIFTY 50</p>
                          <Badge variant="outline" className="text-xs mt-1">NSE</Badge>
                        </div>
                      </div>
                      {nifty50?.changePercent && (
                        <div className={`flex items-center space-x-1 ${nifty50.changePercent >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {nifty50.changePercent >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                          <span className="text-sm font-medium">
                            {nifty50.changePercent >= 0 ? '+' : ''}{nifty50.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                    <div className="text-2xl font-bold text-gray-900">
                      {nifty50?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                    </div>
                  </CardContent>
                </Card>
                
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-all duration-300">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-2">
                        <div className="p-2 bg-purple-100 rounded-lg">
                          <Target className="w-5 h-5 text-purple-600" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-gray-600">NIFTY MID CAP 100</p>
                          <Badge variant="outline" className="text-xs mt-1">NSE</Badge>
                        </div>
                      </div>
                      {niftyMidCap?.changePercent && (
                        <div className={`flex items-center space-x-1 ${niftyMidCap.changePercent >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {niftyMidCap.changePercent >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                          <span className="text-sm font-medium">
                            {niftyMidCap.changePercent >= 0 ? '+' : ''}{niftyMidCap.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                    <div className="text-2xl font-bold text-gray-900">
                      {niftyMidCap?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                    </div>
                  </CardContent>
                </Card>
                
                <Card className="bg-white/80 backdrop-blur-sm border-0 shadow-sm hover:shadow-md transition-all duration-300">
                  <CardContent className="p-4">
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center space-x-2">
                        <div className="p-2 bg-orange-100 rounded-lg">
                          <DollarSign className="w-5 h-5 text-orange-600" />
                        </div>
                        <div>
                          <p className="text-sm font-medium text-gray-600">INDIA VIX</p>
                          <Badge variant="outline" className="text-xs mt-1">Volatility</Badge>
                        </div>
                      </div>
                      {indiaVix?.changePercent && (
                        <div className={`flex items-center space-x-1 ${indiaVix.changePercent >= 0 ? 'text-red-600' : 'text-green-600'}`}>
                          {indiaVix.changePercent >= 0 ? <TrendingUp className="w-4 h-4" /> : <TrendingDown className="w-4 h-4" />}
                          <span className="text-sm font-medium">
                            {indiaVix.changePercent >= 0 ? '+' : ''}{indiaVix.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                    <div className="text-2xl font-bold text-gray-900">
                      {indiaVix?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                    </div>
                  </CardContent>
                </Card>
              </>
            )}
          </div>
          
          <Card className="bg-white/60 backdrop-blur-sm border-0 shadow-sm">
            <CardContent className="p-6">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between mb-6">
                <h3 className="text-lg font-semibold text-gray-900">Market Performance</h3>
                <div className="flex items-center space-x-4 text-sm mt-2 sm:mt-0">
                  <div className="flex items-center">
                    <div className="h-3 w-3 rounded-full bg-blue-500 mr-2"></div>
                    <span className="text-gray-600 font-medium">NIFTY 50</span>
                  </div>
                  <div className="flex items-center">
                    <div className="h-3 w-3 rounded-full bg-purple-500 mr-2"></div>
                    <span className="text-gray-600 font-medium">NIFTY MID CAP 100</span>
                  </div>
                  <div className="flex items-center">
                    <div className="h-3 w-3 rounded-full bg-green-500 mr-2"></div>
                    <span className="text-gray-600 font-medium">NIFTY SMALL CAP 100</span>
                  </div>
                </div>
              </div>
              
              {isLoading ? (
                <Skeleton className="h-72 w-full rounded-lg" />
              ) : error ? (
                <div className="h-72 w-full bg-gray-50 rounded-lg flex items-center justify-center text-red-500">
                  <div className="text-center">
                    <BarChart3 className="w-12 h-12 mx-auto mb-2 text-gray-400" />
                    <p>Error loading market performance data</p>
                  </div>
                </div>
              ) : (
                <div className="h-72 w-full bg-gradient-to-b from-transparent to-green-50/20 rounded-lg">
                  <MarketPerformanceChart timeframe={timeframe} />
                </div>
              )}
            </CardContent>
          </Card>
        </CardContent>
      </Card>
    </div>
  );
}
