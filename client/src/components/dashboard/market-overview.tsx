import React, { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
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
      <Card>
        <CardContent className="p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-neutral-900">Market Overview</h2>
            <div className="flex items-center space-x-2">
              <button 
                className={`px-3 py-1 text-sm rounded-md border ${
                  timeframe === "daily" 
                    ? "bg-primary-100 text-primary-700 border-primary-200 hover:bg-primary-200" 
                    : "bg-white text-neutral-600 border-neutral-300 hover:bg-neutral-50"
                }`}
                onClick={() => setTimeframe("daily")}
              >
                Daily
              </button>
              <button 
                className={`px-3 py-1 text-sm rounded-md border ${
                  timeframe === "weekly" 
                    ? "bg-primary-100 text-primary-700 border-primary-200 hover:bg-primary-200" 
                    : "bg-white text-neutral-600 border-neutral-300 hover:bg-neutral-50"
                }`}
                onClick={() => setTimeframe("weekly")}
              >
                Weekly
              </button>
              <button 
                className={`px-3 py-1 text-sm rounded-md border ${
                  timeframe === "monthly" 
                    ? "bg-primary-100 text-primary-700 border-primary-200 hover:bg-primary-200" 
                    : "bg-white text-neutral-600 border-neutral-300 hover:bg-neutral-50"
                }`}
                onClick={() => setTimeframe("monthly")}
              >
                Monthly
              </button>
              <button 
                className={`px-3 py-1 text-sm rounded-md border ${
                  timeframe === "yearly" 
                    ? "bg-primary-100 text-primary-700 border-primary-200 hover:bg-primary-200" 
                    : "bg-white text-neutral-600 border-neutral-300 hover:bg-neutral-50"
                }`}
                onClick={() => setTimeframe("yearly")}
              >
                Yearly
              </button>
            </div>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            {isLoading ? (
              <>
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-24 w-full" />
              </>
            ) : error ? (
              <div className="col-span-3 text-red-500">Error loading market data</div>
            ) : (
              <>
                <div className="flex items-center bg-neutral-50 p-4 rounded-lg">
                  <div className="flex-shrink-0 h-12 w-12 bg-primary-100 rounded-lg flex items-center justify-center text-primary-600">
                    <span className="material-icons">equalizer</span>
                  </div>
                  <div className="ml-4">
                    <div className="text-sm font-medium text-neutral-500">NIFTY 50</div>
                    <div className="flex items-center">
                      <div className="text-xl font-semibold text-neutral-900">
                        {nifty50?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                      </div>
                      {nifty50?.changePercent && (
                        <div className={`ml-2 flex items-center ${nifty50.changePercent >= 0 ? 'text-success' : 'text-danger'}`}>
                          <span className="material-icons text-sm">
                            {nifty50.changePercent >= 0 ? 'arrow_upward' : 'arrow_downward'}
                          </span>
                          <span className="text-xs">
                            {nifty50.changePercent >= 0 ? '+' : ''}{nifty50.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
                
                <div className="flex items-center bg-neutral-50 p-4 rounded-lg">
                  <div className="flex-shrink-0 h-12 w-12 bg-primary-100 rounded-lg flex items-center justify-center text-primary-600">
                    <span className="material-icons">ssid_chart</span>
                  </div>
                  <div className="ml-4">
                    <div className="text-sm font-medium text-neutral-500">NIFTY MID CAP 100</div>
                    <div className="flex items-center">
                      <div className="text-xl font-semibold text-neutral-900">
                        {niftyMidCap?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                      </div>
                      {niftyMidCap?.changePercent && (
                        <div className={`ml-2 flex items-center ${niftyMidCap.changePercent >= 0 ? 'text-success' : 'text-danger'}`}>
                          <span className="material-icons text-sm">
                            {niftyMidCap.changePercent >= 0 ? 'arrow_upward' : 'arrow_downward'}
                          </span>
                          <span className="text-xs">
                            {niftyMidCap.changePercent >= 0 ? '+' : ''}{niftyMidCap.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
                
                <div className="flex items-center bg-neutral-50 p-4 rounded-lg">
                  <div className="flex-shrink-0 h-12 w-12 bg-primary-100 rounded-lg flex items-center justify-center text-primary-600">
                    <span className="material-icons">currency_rupee</span>
                  </div>
                  <div className="ml-4">
                    <div className="text-sm font-medium text-neutral-500">INDIA VIX</div>
                    <div className="flex items-center">
                      <div className="text-xl font-semibold text-neutral-900">
                        {indiaVix?.closeValue?.toLocaleString('en-IN', { maximumFractionDigits: 2 }) || 'N/A'}
                      </div>
                      {indiaVix?.changePercent && (
                        <div className={`ml-2 flex items-center ${indiaVix.changePercent >= 0 ? 'text-success' : 'text-danger'}`}>
                          <span className="material-icons text-sm">
                            {indiaVix.changePercent >= 0 ? 'arrow_upward' : 'arrow_downward'}
                          </span>
                          <span className="text-xs">
                            {indiaVix.changePercent >= 0 ? '+' : ''}{indiaVix.changePercent.toFixed(2)}%
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
          
          <div className="chart-container">
            <div className="flex justify-between items-center mb-4">
              <h3 className="text-base font-medium text-neutral-900">Market Performance</h3>
              <div className="flex space-x-4 text-sm">
                <div className="flex items-center">
                  <span className="h-3 w-3 rounded-full bg-primary-500 mr-1"></span>
                  <span className="text-neutral-600">NIFTY 50</span>
                </div>
                <div className="flex items-center">
                  <span className="h-3 w-3 rounded-full bg-info mr-1"></span>
                  <span className="text-neutral-600">NIFTY MID CAP 100</span>
                </div>
                <div className="flex items-center">
                  <span className="h-3 w-3 rounded-full bg-success mr-1"></span>
                  <span className="text-neutral-600">NIFTY SMALL CAP 100</span>
                </div>
              </div>
            </div>
            
            {isLoading ? (
              <Skeleton className="h-64 w-full" />
            ) : error ? (
              <div className="h-64 w-full bg-neutral-50 rounded-lg flex items-center justify-center text-red-500">
                Error loading market performance data
              </div>
            ) : (
              <div className="h-64 w-full">
                <MarketPerformanceChart timeframe={timeframe} />
              </div>
            )}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
