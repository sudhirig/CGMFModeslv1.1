import React, { useState } from 'react';
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { AlertCircle, Info, RefreshCw } from "lucide-react";
import { format } from 'date-fns';
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useElivate } from "@/hooks/use-elivate";
import ElivateGauge from "../components/dashboard/elivate-gauge";

export default function ElivateFramework() {
  const { elivateScore, isLoading, error, calculateElivateScore, isCalculating } = useElivate();
  const [activeTab, setActiveTab] = useState("overview");

  const handleRecalculate = () => {
    calculateElivateScore();
  };

  // Format score as percentage
  const formatScore = (score: number, maxScore: number) => {
    return `${Math.round((score / maxScore) * 100)}%`;
  };

  // Render the score bars
  const ScoreBar = ({ score, maxScore, label, color = "bg-primary-500" }: { score: number, maxScore: number, label: string, color?: string }) => {
    return (
      <div className="bg-neutral-50 rounded-lg p-3">
        <div className="flex justify-between items-center">
          <div className="text-xs font-medium text-neutral-500 uppercase">{label}</div>
          <div className="text-xs text-neutral-500">{score} / {maxScore}</div>
        </div>
        <div className="mt-2 w-full bg-neutral-200 rounded-full h-2">
          <div 
            className={`${color} h-2 rounded-full`} 
            style={{ width: `${(score / maxScore) * 100}%` }}
          ></div>
        </div>
      </div>
    );
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-neutral-900">ELIVATE Framework</h1>
        <p className="text-neutral-500 mt-1">
          Capital market analysis using the ELIVATE methodology (External Influence, Local Story, Inflation & Rates, Valuation & Earnings, Allocation of Capital, Trends & Sentiments)
        </p>
      </div>

      {isLoading ? (
        <Card>
          <CardContent className="p-8 flex justify-center items-center">
            <div className="flex flex-col items-center">
              <RefreshCw className="h-8 w-8 text-primary-500 animate-spin mb-4" />
              <p className="text-neutral-500">Loading ELIVATE data...</p>
            </div>
          </CardContent>
        </Card>
      ) : error ? (
        <Alert variant="destructive" className="mb-6">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>
            Failed to load ELIVATE framework data. {error}
            <div className="mt-2">
              <Button variant="outline" size="sm" onClick={() => window.location.reload()}>
                Retry
              </Button>
            </div>
          </AlertDescription>
        </Alert>
      ) : (
        <>
          <Card className="mb-6">
            <CardContent className="p-6">
              <div className="flex justify-between items-center mb-6">
                <h2 className="text-xl font-semibold text-neutral-900">Market Analysis</h2>
                <div className="flex items-center">
                  <span className="text-sm font-medium text-neutral-500 mr-4">
                    Last Updated: {elivateScore?.scoreDate ? format(new Date(elivateScore.scoreDate), "MMMM d, yyyy") : "N/A"}
                  </span>
                  <Button 
                    variant="outline" 
                    size="sm" 
                    onClick={handleRecalculate}
                    disabled={isCalculating}
                  >
                    {isCalculating ? (
                      <>
                        <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                        Recalculating...
                      </>
                    ) : (
                      <>
                        <RefreshCw className="h-4 w-4 mr-2" />
                        Recalculate
                      </>
                    )}
                  </Button>
                </div>
              </div>
              
              <div className="flex flex-col md:flex-row">
                <div className="md:w-1/3 mb-6 md:mb-0 md:pr-6 flex flex-col items-center">
                  <ElivateGauge score={elivateScore?.totalElivateScore || 0} />
                  <div className="mt-4 text-center">
                    <span className={`inline-flex items-center px-3 py-1 rounded-full text-sm font-medium ${
                      elivateScore?.marketStance === "BULLISH" 
                        ? "bg-green-100 text-green-800" 
                        : elivateScore?.marketStance === "BEARISH" 
                        ? "bg-red-100 text-red-800" 
                        : "bg-yellow-100 text-yellow-800"
                    }`}>
                      {elivateScore?.marketStance === "BULLISH" 
                        ? "Bullish" 
                        : elivateScore?.marketStance === "BEARISH" 
                        ? "Bearish" 
                        : "Neutral"} Market Stance
                    </span>
                    
                    <div className="mt-4 text-sm text-neutral-600">
                      <p>
                        Based on our proprietary ELIVATE framework analysis of domestic and global economic factors,
                        market signals indicate a {elivateScore?.marketStance?.toLowerCase() || "neutral"} outlook for Indian equities.
                      </p>
                    </div>
                  </div>
                </div>
                
                <div className="md:w-2/3">
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
                    {/* ELIVATE Component Scores */}
                    <ScoreBar 
                      label="External Influence" 
                      score={elivateScore?.externalInfluenceScore || 0} 
                      maxScore={20}
                      color="bg-blue-500"
                    />
                    <ScoreBar 
                      label="Local Story" 
                      score={elivateScore?.localStoryScore || 0} 
                      maxScore={20}
                      color="bg-green-500"
                    />
                    <ScoreBar 
                      label="Inflation & Rates" 
                      score={elivateScore?.inflationRatesScore || 0} 
                      maxScore={20}
                      color="bg-yellow-500"
                    />
                    <ScoreBar 
                      label="Valuation & Earnings" 
                      score={elivateScore?.valuationEarningsScore || 0} 
                      maxScore={20}
                      color="bg-purple-500"
                    />
                    <ScoreBar 
                      label="Allocation of Capital" 
                      score={elivateScore?.allocationCapitalScore || 0} 
                      maxScore={10}
                      color="bg-indigo-500"
                    />
                    <ScoreBar 
                      label="Trends & Sentiments" 
                      score={elivateScore?.trendsSentimentsScore || 0} 
                      maxScore={10}
                      color="bg-red-500"
                    />
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
          
          <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
            <TabsList className="grid grid-cols-3 mb-6">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="details">Factor Details</TabsTrigger>
              <TabsTrigger value="history">Historical Analysis</TabsTrigger>
            </TabsList>
            
            <TabsContent value="overview" className="space-y-4">
              <Card>
                <CardContent className="p-6">
                  <h3 className="text-lg font-semibold text-neutral-900 mb-4">ELIVATE Framework Explained</h3>
                  
                  <div className="space-y-4">
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-blue-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-blue-600 font-semibold">E</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">External Influence (20 points)</h4>
                        <p className="text-sm text-neutral-600">
                          Global factors affecting Indian markets, including US GDP growth, Fed rates policy, dollar index (DXY), and China's manufacturing PMI.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: US GDP Growth {elivateScore?.usGdpGrowth}%, Fed Rate {elivateScore?.fedFundsRate}%, DXY {elivateScore?.dxyIndex}, China PMI {elivateScore?.chinaPmi}
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-green-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-green-600 font-semibold">L</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">Local Story (20 points)</h4>
                        <p className="text-sm text-neutral-600">
                          India's economic indicators including GDP growth, GST collections, Industrial Production (IIP), and manufacturing PMI.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: India GDP Growth {elivateScore?.indiaGdpGrowth}%, GST ₹{elivateScore?.gstCollectionCr} Cr, IIP Growth {elivateScore?.iipGrowth}%, India PMI {elivateScore?.indiaPmi}
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-yellow-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-yellow-600 font-semibold">I</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">Inflation & Rates (20 points)</h4>
                        <p className="text-sm text-neutral-600">
                          Inflation measures (CPI, WPI), RBI repo rate, and 10-year bond yield as indicators of monetary policy stance.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: CPI {elivateScore?.cpiInflation}%, WPI {elivateScore?.wpiInflation}%, Repo Rate {elivateScore?.repoRate}%, 10Y Yield {elivateScore?.tenYearYield}%
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-purple-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-purple-600 font-semibold">V</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">Valuation & Earnings (20 points)</h4>
                        <p className="text-sm text-neutral-600">
                          Market valuation metrics like Nifty P/E and P/B ratios, and corporate earnings growth projections.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: Nifty P/E {elivateScore?.niftyPe}, Nifty P/B {elivateScore?.niftyPb}, Earnings Growth {elivateScore?.earningsGrowth}%
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-indigo-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-indigo-600 font-semibold">A</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">Allocation of Capital (10 points)</h4>
                        <p className="text-sm text-neutral-600">
                          Fund flows including FII/DII investments and retail SIP contribution trends.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: FII/DII Flow ₹{elivateScore?.fiiFlowsCr} Cr, Monthly SIP ₹{elivateScore?.sipInflowsCr} Cr
                        </div>
                      </div>
                    </div>
                    
                    <div className="flex items-start">
                      <div className="flex-shrink-0 w-10 h-10 bg-red-100 rounded-full flex items-center justify-center mr-4">
                        <span className="text-red-600 font-semibold">TE</span>
                      </div>
                      <div>
                        <h4 className="text-base font-medium text-neutral-900">Trends & Sentiments (10 points)</h4>
                        <p className="text-sm text-neutral-600">
                          Technical indicators including price relative to moving averages, volatility measures (VIX), and market breadth.
                        </p>
                        <div className="mt-1 text-xs text-neutral-500">
                          Current: Price vs. MA {elivateScore?.dmaRatio}%, India VIX {elivateScore?.indiaVix}, Adv/Dec Ratio {elivateScore?.advanceDeclineRatio}
                        </div>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            
            <TabsContent value="details" className="space-y-4">
              <Card>
                <CardContent className="p-6">
                  <h3 className="text-lg font-semibold text-neutral-900 mb-4">Factor Details</h3>
                  
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-neutral-200">
                      <thead>
                        <tr>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Factor</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Current Value</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Rating</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Impact</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-neutral-200 bg-white">
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">US GDP Growth</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.usGdpGrowth}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.usGdpGrowth) || 0) / 5) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Global risk appetite</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Fed Funds Rate</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.fedFundsRate}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.fedFundsRate) || 0) / 6) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">FII flows</td>
                        </tr>
                        {/* Add more rows for other factors */}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            
            <TabsContent value="history" className="space-y-4">
              <Card>
                <CardContent className="p-6">
                  <h3 className="text-lg font-semibold text-neutral-900 mb-4">Historical ELIVATE Scores</h3>
                  
                  <div className="flex items-center justify-center h-64">
                    <div className="text-center text-neutral-500">
                      <Info className="h-12 w-12 text-neutral-300 mx-auto mb-2" />
                      <p>Historical data will be available as more ELIVATE scores are calculated.</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
}