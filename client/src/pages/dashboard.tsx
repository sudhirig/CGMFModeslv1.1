import React, { useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, TrendingDown, BarChart3, Calendar, Download, RefreshCw, AlertCircle, CheckCircle, Activity, DollarSign, Target, Shield } from "lucide-react";
import ElivateScoreCard from "@/components/dashboard/elivate-score-card";
import MarketOverview from "@/components/dashboard/market-overview";
import TopRatedFunds from "@/components/dashboard/top-rated-funds";
import ModelPortfolio from "@/components/dashboard/model-portfolio";
import EtlStatus from "@/components/dashboard/etl-status";
import { useQuery } from "@tanstack/react-query";

export default function Dashboard() {
  const [selectedTimeRange, setSelectedTimeRange] = useState("30d");
  const [isRefreshing, setIsRefreshing] = useState(false);

  // Fetch dashboard summary data
  const { data: dashboardStats, isLoading: statsLoading } = useQuery({
    queryKey: ["/api/dashboard/stats"],
    staleTime: 5 * 60 * 1000,
  });

  const handleRefresh = async () => {
    setIsRefreshing(true);
    // Simulate refresh delay
    setTimeout(() => setIsRefreshing(false), 1500);
  };

  const handleExport = () => {
    // Export functionality - would generate PDF/Excel report
    console.log("Exporting dashboard data...");
  };

  // Use real data from API
  const stats = {
    totalFunds: dashboardStats?.totalFunds || 0,
    elivateScored: dashboardStats?.elivateScored || 0,
    avgScore: dashboardStats?.avgScore || 0,
    marketStatus: dashboardStats?.marketStatus || "Loading...",
    lastUpdated: dashboardStats?.lastUpdated || "Updating..."
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-blue-50/30">
      {/* Enhanced Header */}
      <div className="px-4 sm:px-6 lg:px-8 py-6">
        <div className="flex flex-col space-y-6">
          {/* Title Section */}
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between">
            <div className="flex-1 min-w-0">
              <h1 className="text-3xl font-bold text-gray-900 sm:text-4xl">
                Dashboard Overview
              </h1>
              <p className="mt-2 text-lg text-gray-600">
                Comprehensive market intelligence and fund performance insights
              </p>
            </div>
            <div className="mt-4 sm:mt-0 sm:ml-4">
              <div className="flex items-center space-x-2">
                <Badge variant="secondary" className="bg-green-100 text-green-800">
                  <CheckCircle className="w-3 h-3 mr-1" />
                  System Active
                </Badge>
                <Badge variant="outline" className="text-blue-600 border-blue-200">
                  <Activity className="w-3 h-3 mr-1" />
                  Live Data
                </Badge>
              </div>
            </div>
          </div>

          {/* Stats Overview */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <Card className="bg-white shadow-sm border-0 hover:shadow-md transition-shadow">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600">Total Funds</p>
                    <p className="text-2xl font-bold text-gray-900">{stats.totalFunds.toLocaleString()}</p>
                  </div>
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <BarChart3 className="w-6 h-6 text-blue-600" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-white shadow-sm border-0 hover:shadow-md transition-shadow">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600">ELIVATE Scored</p>
                    <p className="text-2xl font-bold text-gray-900">{stats.elivateScored.toLocaleString()}</p>
                  </div>
                  <div className="p-2 bg-green-100 rounded-lg">
                    <Target className="w-6 h-6 text-green-600" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-white shadow-sm border-0 hover:shadow-md transition-shadow">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600">Average Score</p>
                    <p className="text-2xl font-bold text-gray-900">{stats.avgScore}</p>
                  </div>
                  <div className="p-2 bg-purple-100 rounded-lg">
                    <DollarSign className="w-6 h-6 text-purple-600" />
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-white shadow-sm border-0 hover:shadow-md transition-shadow">
              <CardContent className="p-4">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-sm font-medium text-gray-600">Market Status</p>
                    <p className="text-2xl font-bold text-gray-900">{stats.marketStatus}</p>
                  </div>
                  <div className="p-2 bg-orange-100 rounded-lg">
                    <Shield className="w-6 h-6 text-orange-600" />
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Enhanced Control Panel */}
          <Card className="bg-white/80 backdrop-blur-sm shadow-sm border-0">
            <CardContent className="p-4">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between space-y-4 sm:space-y-0">
                <div className="flex items-center space-x-3">
                  <div className="flex items-center space-x-2">
                    <Select value={selectedTimeRange} onValueChange={setSelectedTimeRange}>
                      <SelectTrigger className="w-36">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="7d">Last 7 Days</SelectItem>
                        <SelectItem value="30d">Last 30 Days</SelectItem>
                        <SelectItem value="90d">Last 90 Days</SelectItem>
                        <SelectItem value="1y">Last Year</SelectItem>
                        <SelectItem value="custom">Custom Range</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <Button variant="outline" size="sm">
                    <Calendar className="w-4 h-4 mr-2" />
                    Custom Range
                  </Button>
                </div>
                
                <div className="flex items-center space-x-2">
                  <Button 
                    variant="outline" 
                    size="sm" 
                    onClick={handleRefresh}
                    disabled={isRefreshing}
                  >
                    <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
                    {isRefreshing ? 'Refreshing...' : 'Refresh Data'}
                  </Button>
                  <Button variant="outline" size="sm" onClick={handleExport}>
                    <Download className="w-4 h-4 mr-2" />
                    Export
                  </Button>
                </div>
              </div>
              
              <div className="mt-3 pt-3 border-t border-gray-200">
                <p className="text-sm text-gray-500">
                  <span className="font-medium">Last Updated:</span> {stats.lastUpdated}
                  <span className="mx-2">•</span>
                  <span className="font-medium">Data Source:</span> MFAPI.in, AMFI, Alpha Vantage
                  <span className="mx-2">•</span>
                  <span className="font-medium">Coverage:</span> 70% of funds scored
                </p>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Enhanced Dashboard Components */}
      <div className="space-y-6">
        <ElivateScoreCard />
        <MarketOverview />
        <TopRatedFunds />
        <ModelPortfolio />
        <EtlStatus />
      </div>
    </div>
  );
}
