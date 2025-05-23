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
                          Current: Stocks Above 200-DMA {elivateScore?.stocksAbove200dmaPct}%, India VIX {elivateScore?.indiaVix}, Adv/Dec Ratio {elivateScore?.advanceDeclineRatio}
                        </div>
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            
            <TabsContent value="details" className="space-y-4">
              {/* Factor Details & Data Sources Card */}
              <Card className="mb-6">
                <CardContent className="pt-6">
                  <div className="flex items-center justify-between mb-4">
                    <h3 className="text-lg font-semibold">Factor Details & Data Sources</h3>
                    <div className="flex items-center space-x-2">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                        Live Data
                      </span>
                      <span className="text-sm text-neutral-500">Last Updated: {format(new Date(elivateScore?.scoreDate || new Date()), 'MMM dd, yyyy')}</span>
                    </div>
                  </div>
                  
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
                    <div className="bg-slate-50 p-4 rounded-lg">
                      <h4 className="text-sm font-medium mb-2">AMFI Data</h4>
                      <p className="text-sm text-muted-foreground mb-2">Source: Association of Mutual Funds in India (AMFI)</p>
                      <ul className="text-xs text-muted-foreground space-y-1">
                        <li className="flex justify-between">
                          <span>Data Type:</span>
                          <span className="font-medium text-black">Live API</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Update Frequency:</span>
                          <span className="font-medium text-black">Daily</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Covers:</span>
                          <span className="font-medium text-black">2,985 Mutual Funds</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Data Points:</span>
                          <span className="font-medium text-black">Fund NAVs, Categories</span>
                        </li>
                      </ul>
                    </div>
                    
                    <div className="bg-slate-50 p-4 rounded-lg">
                      <h4 className="text-sm font-medium mb-2">NSE Data</h4>
                      <p className="text-sm text-muted-foreground mb-2">Source: National Stock Exchange of India</p>
                      <ul className="text-xs text-muted-foreground space-y-1">
                        <li className="flex justify-between">
                          <span>Data Type:</span>
                          <span className="font-medium text-black">Live API</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Update Frequency:</span>
                          <span className="font-medium text-black">Daily (EOD)</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Covers:</span>
                          <span className="font-medium text-black">Market Indices, Metrics</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Data Points:</span>
                          <span className="font-medium text-black">P/E, P/B, VIX, A/D Ratio</span>
                        </li>
                      </ul>
                    </div>
                    
                    <div className="bg-slate-50 p-4 rounded-lg">
                      <h4 className="text-sm font-medium mb-2">RBI & Economic Data</h4>
                      <p className="text-sm text-muted-foreground mb-2">Source: Reserve Bank of India & Government</p>
                      <ul className="text-xs text-muted-foreground space-y-1">
                        <li className="flex justify-between">
                          <span>Data Type:</span>
                          <span className="font-medium text-black">Live API</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Update Frequency:</span>
                          <span className="font-medium text-black">Monthly</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Covers:</span>
                          <span className="font-medium text-black">Economic Indicators</span>
                        </li>
                        <li className="flex justify-between">
                          <span>Data Points:</span>
                          <span className="font-medium text-black">GDP, Inflation, Repo Rate</span>
                        </li>
                      </ul>
                    </div>
                  </div>
                  
                  <h3 className="text-lg font-semibold text-neutral-900 mb-4">Factor Details</h3>
                  
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-neutral-200">
                      <thead>
                        <tr>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Factor</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Current Value</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Source</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Update Frequency</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Rating</th>
                          <th className="px-4 py-3 bg-neutral-50 text-left text-xs font-medium text-neutral-500 uppercase">Impact</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-neutral-200 bg-white">
                        {/* External Influence Factors */}
                        <tr className="bg-blue-50">
                          <td colSpan={6} className="px-4 py-2 text-sm font-semibold text-blue-700">External Influence</td>
                        </tr>
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
                          <td className="px-4 py-3 text-sm text-neutral-500">FII flows & liquidity</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Dollar Index (DXY)</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.dxyIndex}</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.dxyIndex) || 0) / 110) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Currency impact</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">China PMI</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.chinaPmi}</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-blue-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.chinaPmi) || 0) / 55) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Regional growth</td>
                        </tr>
                        
                        {/* Local Story Factors */}
                        <tr className="bg-green-50">
                          <td colSpan={4} className="px-4 py-2 text-sm font-semibold text-green-700">Local Story</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">India GDP Growth</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.indiaGdpGrowth}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-green-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.indiaGdpGrowth) || 0) / 8) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Economic strength</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">GST Collections</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">₹{elivateScore?.gstCollectionCr.toLocaleString()} Cr</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-green-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.gstCollectionCr) || 0) / 180000) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Tax revenue health</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">IIP Growth</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.iipGrowth}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-green-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.iipGrowth) || 0) / 6) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Industrial production</td>
                        </tr>
                        
                        {/* Inflation & Rates Factors */}
                        <tr className="bg-yellow-50">
                          <td colSpan={4} className="px-4 py-2 text-sm font-semibold text-yellow-700">Inflation & Rates</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">CPI Inflation</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.cpiInflation}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-yellow-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.cpiInflation) || 0) / 8) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Consumer inflation</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Repo Rate</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.repoRate}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-yellow-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.repoRate) || 0) / 8) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Monetary policy</td>
                        </tr>
                        
                        {/* Valuation & Earnings */}
                        <tr className="bg-purple-50">
                          <td colSpan={4} className="px-4 py-2 text-sm font-semibold text-purple-700">Valuation & Earnings</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Nifty P/E Ratio</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.niftyPe}</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-purple-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.niftyPe) || 0) / 25) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Valuation</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Earnings Growth</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.earningsGrowth}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-purple-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.earningsGrowth) || 0) / 20) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Corporate profitability</td>
                        </tr>
                        
                        {/* Allocation & Trends sections (condensed for space) */}
                        <tr className="bg-indigo-50">
                          <td colSpan={4} className="px-4 py-2 text-sm font-semibold text-indigo-700">Capital Allocation</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">FII Flows</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">₹{elivateScore?.fiiFlowsCr.toLocaleString()} Cr</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-indigo-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.fiiFlowsCr) || 0) / 15000) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Foreign investment</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">SIP Inflows</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">₹{elivateScore?.sipInflowsCr.toLocaleString()} Cr</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-indigo-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.sipInflowsCr) || 0) / 18000) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Retail participation</td>
                        </tr>
                        
                        <tr className="bg-red-50">
                          <td colSpan={4} className="px-4 py-2 text-sm font-semibold text-red-700">Trends & Sentiments</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">India VIX</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.indiaVix}</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-red-500 h-2 rounded-full" style={{ width: `${Math.min(100 - ((Number(elivateScore?.indiaVix) || 0) / 25) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Market volatility</td>
                        </tr>
                        <tr>
                          <td className="px-4 py-3 text-sm font-medium text-neutral-900">Stocks Above 200 DMA</td>
                          <td className="px-4 py-3 text-sm text-neutral-500">{elivateScore?.stocksAbove200dmaPct}%</td>
                          <td className="px-4 py-3">
                            <div className="flex items-center">
                              <div className="w-16 bg-neutral-200 rounded-full h-2">
                                <div className="bg-red-500 h-2 rounded-full" style={{ width: `${Math.min(((Number(elivateScore?.stocksAbove200dmaPct) || 0) / 80) * 100, 100)}%` }}></div>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-sm text-neutral-500">Market breadth</td>
                        </tr>
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
                  
                  <div className="mb-6">
                    <h4 className="text-base font-medium text-neutral-700 mb-2">Current Market Outlook</h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                        <div className="text-sm font-medium text-gray-500 mb-1">Current ELIVATE Score</div>
                        <div className="flex items-center">
                          <div className="text-3xl font-bold text-gray-900">{elivateScore?.totalElivateScore}</div>
                          <div className={`ml-2 px-2 py-1 text-xs font-semibold rounded-full ${
                            elivateScore?.marketStance === "BULLISH" 
                              ? "bg-green-100 text-green-800" 
                              : elivateScore?.marketStance === "BEARISH" 
                              ? "bg-red-100 text-red-800" 
                              : "bg-yellow-100 text-yellow-800"
                          }`}>
                            {elivateScore?.marketStance}
                          </div>
                        </div>
                        <div className="text-sm text-gray-600 mt-1">Last updated: {elivateScore?.scoreDate ? new Date(elivateScore.scoreDate).toLocaleDateString() : 'N/A'}</div>
                      </div>
                      
                      <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                        <div className="text-sm font-medium text-gray-500 mb-1">Investment Implication</div>
                        <div className="text-base font-medium text-gray-900">
                          {elivateScore?.marketStance === "BULLISH" 
                            ? "Consider increased equity allocation" 
                            : elivateScore?.marketStance === "BEARISH" 
                            ? "Consider defensive positioning" 
                            : "Maintain balanced allocation"
                          }
                        </div>
                        <div className="text-sm text-gray-600 mt-1">
                          {elivateScore?.marketStance === "BULLISH" 
                            ? "Favorable risk-reward environment" 
                            : elivateScore?.marketStance === "BEARISH" 
                            ? "Higher market uncertainty" 
                            : "Mixed signals present"
                          }
                        </div>
                      </div>
                      
                      <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                        <div className="text-sm font-medium text-gray-500 mb-1">Risk Assessment</div>
                        <div className="text-base font-medium text-gray-900">
                          {elivateScore?.totalElivateScore >= 75 
                            ? "Lower than average risk" 
                            : elivateScore?.totalElivateScore >= 50 
                            ? "Average risk level" 
                            : "Higher than average risk"
                          }
                        </div>
                        <div className="text-sm text-gray-600 mt-1">
                          Based on market volatility and trends
                        </div>
                      </div>
                    </div>
                  </div>
                  
                  <div className="mb-6">
                    <h4 className="text-base font-medium text-neutral-700 mb-2">Historical Trend Analysis</h4>
                    <div className="bg-white border border-gray-200 rounded-lg p-4 shadow-sm">
                      <div className="flex items-center justify-between mb-4">
                        <div>
                          <span className="text-sm font-medium text-gray-500">Annual Projections</span>
                          <div className="text-base font-medium mt-1">May 2025 - Apr 2026</div>
                        </div>
                        <div className="flex space-x-2">
                          <div className="text-xs px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Q1</div>
                          <div className="text-xs px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Q2</div>
                          <div className="text-xs px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Q3</div>
                          <div className="text-xs px-2 py-1 bg-blue-100 text-blue-800 rounded-full">Q4</div>
                        </div>
                      </div>
                      
                      <div className="space-y-4">
                        <div>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">GDP Growth</span>
                            <span className="text-gray-500">Quarterly projection</span>
                          </div>
                          <div className="relative pt-1">
                            <div className="flex h-6 items-center">
                              <div className="flex items-center justify-center w-1/4 h-full bg-green-100 text-xs text-green-800 first:rounded-l-md">
                                6.9%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-green-100 text-xs text-green-800">
                                7.1%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-green-100 text-xs text-green-800">
                                7.2%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-green-100 text-xs text-green-800 last:rounded-r-md">
                                7.0%
                              </div>
                            </div>
                          </div>
                        </div>
                        
                        <div>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">Inflation</span>
                            <span className="text-gray-500">CPI, quarterly avg</span>
                          </div>
                          <div className="relative pt-1">
                            <div className="flex h-6 items-center">
                              <div className="flex items-center justify-center w-1/4 h-full bg-yellow-100 text-xs text-yellow-800 first:rounded-l-md">
                                4.8%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-yellow-100 text-xs text-yellow-800">
                                4.5%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-yellow-100 text-xs text-yellow-800">
                                4.3%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-yellow-100 text-xs text-yellow-800 last:rounded-r-md">
                                4.1%
                              </div>
                            </div>
                          </div>
                        </div>
                        
                        <div>
                          <div className="flex justify-between text-sm mb-1">
                            <span className="font-medium">Corporate Earnings</span>
                            <span className="text-gray-500">YoY growth projection</span>
                          </div>
                          <div className="relative pt-1">
                            <div className="flex h-6 items-center">
                              <div className="flex items-center justify-center w-1/4 h-full bg-purple-100 text-xs text-purple-800 first:rounded-l-md">
                                15.4%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-purple-100 text-xs text-purple-800">
                                16.2%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-purple-100 text-xs text-purple-800">
                                17.0%
                              </div>
                              <div className="flex items-center justify-center w-1/4 h-full bg-purple-100 text-xs text-purple-800 last:rounded-r-md">
                                16.5%
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  
                  <div>
                    <h4 className="text-base font-medium text-neutral-700 mb-2">Key Events & Milestones</h4>
                    <div className="relative border-l-2 border-gray-200 pl-8 pb-2 ml-4">
                      <div className="absolute -left-2 -top-1 mt-0.5">
                        <div className="w-4 h-4 rounded-full bg-green-500"></div>
                      </div>
                      <div className="mb-6">
                        <div className="text-sm text-gray-500">May 2025</div>
                        <div className="text-base font-medium">RBI Policy Decision</div>
                        <div className="text-sm text-gray-600 mt-1">Expected pause in rate cycle with stable stance</div>
                      </div>
                      
                      <div className="absolute -left-2 top-24">
                        <div className="w-4 h-4 rounded-full bg-blue-500"></div>
                      </div>
                      <div className="mb-6">
                        <div className="text-sm text-gray-500">July 2025</div>
                        <div className="text-base font-medium">Union Budget</div>
                        <div className="text-sm text-gray-600 mt-1">Focus on fiscal consolidation and growth</div>
                      </div>
                      
                      <div className="absolute -left-2 top-48">
                        <div className="w-4 h-4 rounded-full bg-purple-500"></div>
                      </div>
                      <div>
                        <div className="text-sm text-gray-500">October 2025</div>
                        <div className="text-base font-medium">Q2 Earnings Season</div>
                        <div className="text-sm text-gray-600 mt-1">Expected to show continued momentum in growth</div>
                      </div>
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