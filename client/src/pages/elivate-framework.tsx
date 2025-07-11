import React, { useState } from 'react';
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Info, TrendingUp, Globe, Activity, DollarSign, Target, BarChart3, Award, Star, Zap, Shield, Eye, RefreshCw, AlertCircle } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend, BarChart, Bar, PieChart, Pie, Cell, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Area, AreaChart } from "recharts";
import { Skeleton } from "@/components/ui/skeleton";
import { useElivate } from "@/hooks/use-elivate";
import ElivateGauge from "../components/dashboard/elivate-gauge";

export default function ElivateFramework() {
  const [selectedComponent, setSelectedComponent] = useState<string>("overview");
  const { elivateScore, isLoading, error, calculateElivateScore, isCalculating } = useElivate();
  
  const { data: componentData, isLoading: isComponentLoading } = useQuery({
    queryKey: ["/api/elivate/components"],
    staleTime: 5 * 60 * 1000,
  });

  const { data: historicalData, isLoading: isHistoricalLoading } = useQuery({
    queryKey: ["/api/elivate/historical"],
    staleTime: 5 * 60 * 1000,
  });

  const components = [
    {
      id: "external-influence",
      name: "External Influence",
      points: 20,
      icon: <Globe className="w-6 h-6" />,
      color: "#3B82F6",
      gradient: "from-blue-500 to-blue-700",
      description: "Global economic factors affecting Indian markets",
      factors: ["US GDP Growth", "Federal Reserve Rates", "DXY (Dollar Index)", "China PMI"],
      currentScore: componentData?.externalInfluence || 0,
      impact: "High",
      trend: "Stable"
    },
    {
      id: "local-story",
      name: "Local Story",
      points: 20,
      icon: <Activity className="w-6 h-6" />,
      color: "#10B981",
      gradient: "from-green-500 to-green-700",
      description: "Indian domestic economic indicators",
      factors: ["India GDP Growth", "GST Collections", "IIP Growth", "India PMI"],
      currentScore: componentData?.localStory || 0,
      impact: "High",
      trend: "Positive"
    },
    {
      id: "inflation-rates",
      name: "Inflation & Rates",
      points: 20,
      icon: <TrendingUp className="w-6 h-6" />,
      color: "#F59E0B",
      gradient: "from-amber-500 to-amber-700",
      description: "Price levels and interest rate environment",
      factors: ["CPI Inflation", "WPI Inflation", "Repo Rate", "10Y Bond Yield"],
      currentScore: componentData?.inflationRates || 0,
      impact: "High",
      trend: "Neutral"
    },
    {
      id: "valuation-earnings",
      name: "Valuation & Earnings",
      points: 20,
      icon: <BarChart3 className="w-6 h-6" />,
      color: "#8B5CF6",
      gradient: "from-purple-500 to-purple-700",
      description: "Market valuations and earnings growth",
      factors: ["Nifty P/E Ratio", "Nifty P/B Ratio", "Earnings Growth", "Revenue Growth"],
      currentScore: componentData?.valuationEarnings || 0,
      impact: "High",
      trend: "Positive"
    },
    {
      id: "capital-allocation",
      name: "Capital Allocation",
      points: 10,
      icon: <DollarSign className="w-6 h-6" />,
      color: "#EF4444",
      gradient: "from-red-500 to-red-700",
      description: "Flow of institutional and retail capital",
      factors: ["FII Flows", "DII Flows", "SIP Inflows", "IPO Activity"],
      currentScore: componentData?.capitalAllocation || 0,
      impact: "Medium",
      trend: "Volatile"
    },
    {
      id: "trends-sentiments",
      name: "Trends & Sentiments",
      points: 10,
      icon: <Target className="w-6 h-6" />,
      color: "#EC4899",
      gradient: "from-pink-500 to-pink-700",
      description: "Market momentum and sentiment indicators",
      factors: ["200-Day MA", "VIX Levels", "Advance/Decline", "Market Breadth"],
      currentScore: componentData?.trendsSentiments || 0,
      impact: "Medium",
      trend: "Bullish"
    }
  ];

  const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#8B5CF6', '#EF4444', '#EC4899'];

  const getComponentData = () => {
    return components.map(comp => ({
      name: comp.name.replace(' &', '\n&'),
      score: comp.currentScore,
      maxScore: comp.points,
      percentage: (comp.currentScore / comp.points) * 100,
      efficiency: Math.min(100, (comp.currentScore / comp.points) * 120)
    }));
  };

  const getRadarData = () => {
    return components.map(comp => ({
      component: comp.name.split(' ')[0],
      score: comp.currentScore,
      fullMark: comp.points
    }));
  };

  const totalScore = elivateScore?.totalScore || 0;
  const maxScore = 100;
  const scorePercentage = (totalScore / maxScore) * 100;

  const getScoreColor = (score: number) => {
    if (score >= 80) return "text-green-600";
    if (score >= 60) return "text-amber-600";
    if (score >= 40) return "text-orange-600";
    return "text-red-600";
  };

  const getScoreGradient = (score: number) => {
    if (score >= 80) return "from-green-500 to-green-600";
    if (score >= 60) return "from-amber-500 to-amber-600";
    if (score >= 40) return "from-orange-500 to-orange-600";
    return "from-red-500 to-red-600";
  };

  const ScoreBar = ({ score, maxScore, label, color = "bg-primary-500" }: { score: number, maxScore: number, label: string, color?: string }) => {
    return (
      <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-100">
        <div className="flex justify-between items-center mb-2">
          <div className="text-sm font-medium text-gray-700">{label}</div>
          <div className="text-sm font-semibold text-gray-900">{score.toFixed(1)} / {maxScore} pts</div>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-3">
          <div 
            className={`${color} h-3 rounded-full transition-all duration-1000 ease-out`} 
            style={{ width: `${(score / maxScore) * 100}%` }}
          ></div>
        </div>
        <div className="text-xs text-gray-500 mt-1">
          {((score / maxScore) * 100).toFixed(1)}% of maximum
        </div>
      </div>
    );
  };

  const handleRecalculate = () => {
    calculateElivateScore();
  };

  return (
    <div className="py-6 bg-gradient-to-br from-gray-50 to-white min-h-screen">
      <div className="px-4 sm:px-6 lg:px-8">
        {/* Enhanced Header */}
        <div className="mb-8">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between">
            <div className="flex items-center space-x-4">
              <div className="p-3 bg-gradient-to-r from-blue-500 to-purple-600 rounded-2xl shadow-lg">
                <Zap className="w-8 h-8 text-white" />
              </div>
              <div>
                <h1 className="text-4xl font-bold bg-gradient-to-r from-gray-900 to-gray-600 bg-clip-text text-transparent">
                  ELIVATE Framework
                </h1>
                <p className="mt-2 text-lg text-gray-600 max-w-2xl">
                  Enhanced 6-component scoring methodology for comprehensive market intelligence and investment decisions
                </p>
              </div>
            </div>
            <div className="mt-6 lg:mt-0 flex items-center space-x-4">
              <Button
                onClick={handleRecalculate}
                disabled={isCalculating}
                variant="outline"
                className="flex items-center space-x-2"
              >
                <RefreshCw className={`w-4 h-4 ${isCalculating ? 'animate-spin' : ''}`} />
                <span>Refresh Score</span>
              </Button>
              <div className="text-right">
                <div className="text-sm text-gray-500">Current Score</div>
                <div className={`text-3xl font-bold ${getScoreColor(scorePercentage)}`}>
                  {totalScore.toFixed(1)}
                </div>
                <div className="text-sm text-gray-500">/ {maxScore}</div>
              </div>
              <div className="w-24 h-24 relative">
                <svg className="w-24 h-24 transform -rotate-90" viewBox="0 0 100 100">
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    stroke="#E5E7EB"
                    strokeWidth="8"
                    fill="none"
                  />
                  <circle
                    cx="50"
                    cy="50"
                    r="40"
                    stroke={scorePercentage >= 80 ? "#10B981" : scorePercentage >= 60 ? "#F59E0B" : "#EF4444"}
                    strokeWidth="8"
                    fill="none"
                    strokeDasharray={`${scorePercentage * 2.51}, 251`}
                    strokeLinecap="round"
                    className="transition-all duration-1000 ease-out"
                  />
                </svg>
                <div className="absolute inset-0 flex items-center justify-center">
                  <span className="text-sm font-bold text-gray-700">
                    {scorePercentage.toFixed(0)}%
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        {isLoading ? (
          <Card>
            <CardContent className="p-8 flex justify-center items-center">
              <div className="flex flex-col items-center">
                <RefreshCw className="h-8 w-8 text-blue-500 animate-spin mb-4" />
                <p className="text-gray-500">Loading ELIVATE data...</p>
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
          <Tabs value={selectedComponent} onValueChange={setSelectedComponent} className="space-y-6">
            <TabsList className="grid w-full grid-cols-4 bg-white rounded-lg shadow-sm border">
              <TabsTrigger value="overview" className="flex items-center space-x-2">
                <Eye className="w-4 h-4" />
                <span>Overview</span>
              </TabsTrigger>
              <TabsTrigger value="components" className="flex items-center space-x-2">
                <Target className="w-4 h-4" />
                <span>Components</span>
              </TabsTrigger>
              <TabsTrigger value="analysis" className="flex items-center space-x-2">
                <BarChart3 className="w-4 h-4" />
                <span>Analysis</span>
              </TabsTrigger>
              <TabsTrigger value="historical" className="flex items-center space-x-2">
                <TrendingUp className="w-4 h-4" />
                <span>Historical</span>
              </TabsTrigger>
            </TabsList>

            <TabsContent value="overview">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Score Overview */}
                <Card className="bg-gradient-to-br from-blue-50 to-indigo-50 border-blue-200">
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Award className="w-5 h-5 text-blue-600" />
                      <span>Current ELIVATE Score</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="text-center">
                      <div className="text-6xl font-bold text-blue-600 mb-2">
                        {totalScore.toFixed(1)}
                      </div>
                      <div className="text-lg text-gray-600 mb-4">out of 100 points</div>
                      <div className="w-full bg-gray-200 rounded-full h-4 mb-4">
                        <div 
                          className={`bg-gradient-to-r ${getScoreGradient(scorePercentage)} h-4 rounded-full transition-all duration-1000 ease-out`}
                          style={{ width: `${scorePercentage}%` }}
                        ></div>
                      </div>
                      <Badge variant="outline" className="text-sm">
                        {scorePercentage >= 80 ? "Excellent" : scorePercentage >= 60 ? "Good" : scorePercentage >= 40 ? "Fair" : "Poor"} Market Conditions
                      </Badge>
                    </div>
                  </CardContent>
                </Card>

                {/* Component Breakdown */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <BarChart3 className="w-5 h-5 text-purple-600" />
                      <span>Component Scores</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {components.map((comp) => (
                        <ScoreBar
                          key={comp.id}
                          score={comp.currentScore}
                          maxScore={comp.points}
                          label={comp.name}
                          color={`bg-gradient-to-r ${comp.gradient}`}
                        />
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="components">
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {components.map((comp) => (
                  <Card key={comp.id} className="hover:shadow-lg transition-shadow duration-300">
                    <CardHeader className="pb-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                          <div className={`p-3 rounded-lg bg-gradient-to-r ${comp.gradient}`}>
                            {React.cloneElement(comp.icon, { className: "w-6 h-6 text-white" })}
                          </div>
                          <div>
                            <CardTitle className="text-lg">{comp.name}</CardTitle>
                            <div className="flex items-center space-x-2 mt-1">
                              <Badge variant="secondary" className="text-xs">
                                {comp.impact} Impact
                              </Badge>
                              <Badge variant="outline" className="text-xs">
                                {comp.trend}
                              </Badge>
                            </div>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className="text-2xl font-bold text-gray-900">
                            {comp.currentScore.toFixed(1)}
                          </div>
                          <div className="text-sm text-gray-500">
                            / {comp.points} pts
                          </div>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent>
                      <p className="text-sm text-gray-600 mb-4">{comp.description}</p>
                      <div className="space-y-2">
                        <div className="text-sm font-medium text-gray-700">Key Factors:</div>
                        <div className="flex flex-wrap gap-2">
                          {comp.factors.map((factor, idx) => (
                            <Badge key={idx} variant="outline" className="text-xs">
                              {factor}
                            </Badge>
                          ))}
                        </div>
                      </div>
                      <div className="mt-4">
                        <Progress 
                          value={(comp.currentScore / comp.points) * 100} 
                          className="h-2"
                        />
                        <div className="text-xs text-gray-500 mt-1">
                          {((comp.currentScore / comp.points) * 100).toFixed(1)}% of maximum
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </TabsContent>

            <TabsContent value="analysis">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Radar Chart */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <Target className="w-5 h-5 text-blue-600" />
                      <span>Component Analysis</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <RadarChart data={getRadarData()}>
                          <PolarGrid />
                          <PolarAngleAxis dataKey="component" />
                          <PolarRadiusAxis angle={90} domain={[0, 20]} />
                          <Radar
                            name="Current Score"
                            dataKey="score"
                            stroke="#3B82F6"
                            fill="#3B82F6"
                            fillOpacity={0.2}
                            strokeWidth={2}
                          />
                          <Radar
                            name="Maximum Score"
                            dataKey="fullMark"
                            stroke="#E5E7EB"
                            fill="none"
                            strokeWidth={1}
                            strokeDasharray="5,5"
                          />
                          <Legend />
                          <Tooltip />
                        </RadarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Bar Chart */}
                <Card>
                  <CardHeader>
                    <CardTitle className="flex items-center space-x-2">
                      <BarChart3 className="w-5 h-5 text-green-600" />
                      <span>Score Distribution</span>
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={getComponentData()}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis 
                            dataKey="name" 
                            angle={-45}
                            textAnchor="end"
                            height={60}
                          />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="score" fill="#3B82F6" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="historical">
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    <TrendingUp className="w-5 h-5 text-purple-600" />
                    <span>Historical Performance</span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  {isHistoricalLoading ? (
                    <div className="flex justify-center items-center h-64">
                      <RefreshCw className="w-8 h-8 animate-spin text-blue-500" />
                    </div>
                  ) : historicalData ? (
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={historicalData}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis dataKey="date" />
                          <YAxis />
                          <Tooltip />
                          <Legend />
                          <Line type="monotone" dataKey="score" stroke="#3B82F6" strokeWidth={2} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  ) : (
                    <div className="text-center py-12">
                      <BarChart3 className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                      <p className="text-gray-500">Historical data will be available soon</p>
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