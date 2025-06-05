import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { usePortfolio } from "@/hooks/use-portfolio";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";

type RiskProfile = 'Conservative' | 'Moderately Conservative' | 'Balanced' | 'Moderately Aggressive' | 'Aggressive';

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d'];

export default function PortfolioBuilder() {
  const [selectedTab, setSelectedTab] = useState<string>("builder");
  const [selectedRiskProfile, setSelectedRiskProfile] = useState<RiskProfile>("Balanced");
  
  const { 
    portfolios,
    selectedPortfolio, 
    generatePortfolio,
    isGenerating,
    isLoadingPortfolios,
    error 
  } = usePortfolio();
  
  const handleGeneratePortfolio = () => {
    generatePortfolio({ riskProfile: selectedRiskProfile });
  };

  const riskProfiles = [
    {
      name: 'Conservative',
      description: 'Low risk, stable returns with minimal volatility',
      allocation: 'Debt: 80-90%, Equity: 10-20%'
    },
    {
      name: 'Moderately Conservative',
      description: 'Low to moderate risk with steady growth potential',
      allocation: 'Debt: 60-70%, Equity: 30-40%'
    },
    {
      name: 'Balanced',
      description: 'Moderate risk with balanced growth and stability',
      allocation: 'Debt: 40-50%, Equity: 50-60%'
    },
    {
      name: 'Moderately Aggressive',
      description: 'Moderate to high risk with strong growth potential',
      allocation: 'Debt: 20-30%, Equity: 70-80%'
    },
    {
      name: 'Aggressive',
      description: 'High risk, high growth potential for long-term wealth creation',
      allocation: 'Debt: 10-20%, Equity: 80-90%'
    }
  ];

  const currentRiskProfile = riskProfiles.find(profile => profile.name === selectedRiskProfile);

  return (
    <div className="container mx-auto p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Portfolio Builder</h1>
        <p className="text-gray-600 dark:text-gray-300 mt-2">
          Create and analyze diversified investment portfolios based on your risk profile
        </p>
      </div>

      <Tabs value={selectedTab} onValueChange={setSelectedTab} className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="builder">Portfolio Builder</TabsTrigger>
          <TabsTrigger value="analysis">Portfolio Analysis</TabsTrigger>
        </TabsList>

        <TabsContent value="builder" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Risk Profile Selection</CardTitle>
              <CardDescription>
                Choose your investment risk tolerance to generate an appropriate portfolio
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-4">
                <label className="text-sm font-medium">Select Risk Profile</label>
                <Select value={selectedRiskProfile} onValueChange={(value: RiskProfile) => setSelectedRiskProfile(value)}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {riskProfiles.map((profile) => (
                      <SelectItem key={profile.name} value={profile.name}>
                        {profile.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              {currentRiskProfile && (
                <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                  <h3 className="font-semibold text-blue-900 dark:text-blue-100">
                    {currentRiskProfile.name}
                  </h3>
                  <p className="text-blue-700 dark:text-blue-200 text-sm mt-1">
                    {currentRiskProfile.description}
                  </p>
                  <p className="text-blue-600 dark:text-blue-300 text-sm mt-2 font-medium">
                    Typical Allocation: {currentRiskProfile.allocation}
                  </p>
                </div>
              )}

              <Separator />

              <div className="flex gap-4">
                <Button 
                  onClick={handleGeneratePortfolio} 
                  disabled={isGenerating}
                  className="flex-1"
                >
                  {isGenerating ? 'Generating Portfolio...' : 'Generate Portfolio'}
                </Button>
              </div>

              {error && (
                <div className="p-4 bg-red-50 dark:bg-red-900/20 rounded-lg">
                  <p className="text-red-700 dark:text-red-200 text-sm">
                    Error: {error instanceof Error ? error.message : 'Failed to generate portfolio'}
                  </p>
                </div>
              )}
            </CardContent>
          </Card>

          {selectedPortfolio && (
            <Card>
              <CardHeader>
                <CardTitle>Generated Portfolio: {selectedPortfolio.name}</CardTitle>
                <CardDescription>
                  Risk Profile: {selectedPortfolio.riskProfile}
                </CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid md:grid-cols-2 gap-6">
                  <div>
                    <h3 className="font-semibold mb-4">Asset Allocation</h3>
                    <div className="space-y-3">
                      {selectedPortfolio.allocations?.map((allocation, index) => (
                        <div key={index} className="flex justify-between items-center p-3 bg-gray-50 dark:bg-gray-800 rounded-lg">
                          <div>
                            <p className="font-medium">{allocation.fund.fund_name}</p>
                            <p className="text-sm text-gray-600 dark:text-gray-400">{allocation.fund.category}</p>
                          </div>
                          <div className="text-right">
                            <p className="font-semibold">{allocation.allocation_percent}%</p>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div>
                    <h3 className="font-semibold mb-4">Portfolio Visualization</h3>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={selectedPortfolio.allocations?.map((allocation, index) => ({
                              name: allocation.fund.fund_name,
                              value: allocation.allocation_percent,
                              fill: COLORS[index % COLORS.length]
                            }))}
                            cx="50%"
                            cy="50%"
                            outerRadius={80}
                            dataKey="value"
                            label={({value}) => `${value}%`}
                          />
                          <Tooltip />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="analysis" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle>Portfolio Analysis</CardTitle>
              <CardDescription>
                Comprehensive analysis of your selected portfolio
              </CardDescription>
            </CardHeader>
            <CardContent>
              {selectedPortfolio ? (
                <div className="space-y-6">
                  <div className="grid md:grid-cols-3 gap-4">
                    <div className="p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
                      <h3 className="font-semibold text-green-900 dark:text-green-100">Total Funds</h3>
                      <p className="text-2xl font-bold text-green-700 dark:text-green-300">
                        {selectedPortfolio.allocations?.length || 0}
                      </p>
                    </div>
                    <div className="p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
                      <h3 className="font-semibold text-blue-900 dark:text-blue-100">Risk Profile</h3>
                      <p className="text-lg font-bold text-blue-700 dark:text-blue-300">
                        {selectedPortfolio.riskProfile}
                      </p>
                    </div>
                    <div className="p-4 bg-purple-50 dark:bg-purple-900/20 rounded-lg">
                      <h3 className="font-semibold text-purple-900 dark:text-purple-100">Diversification</h3>
                      <p className="text-lg font-bold text-purple-700 dark:text-purple-300">
                        {new Set(selectedPortfolio.allocations?.map(a => a.fund.category)).size} Categories
                      </p>
                    </div>
                  </div>

                  <div>
                    <h3 className="font-semibold mb-4">Fund Details</h3>
                    <div className="space-y-2">
                      {selectedPortfolio.allocations?.map((allocation, index) => (
                        <div key={index} className="flex justify-between items-center p-3 border rounded-lg">
                          <div>
                            <p className="font-medium">{allocation.fund.fund_name}</p>
                            <p className="text-sm text-gray-600 dark:text-gray-400">
                              Category: {allocation.fund.category}
                            </p>
                          </div>
                          <div className="text-right">
                            <p className="font-semibold">{allocation.allocation_percent}%</p>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="text-center py-8">
                  <p className="text-gray-600 dark:text-gray-400">
                    Generate a portfolio first to see the analysis
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {isLoadingPortfolios && (
        <div className="space-y-4">
          <Skeleton className="h-4 w-full" />
          <Skeleton className="h-4 w-3/4" />
          <Skeleton className="h-4 w-1/2" />
        </div>
      )}
    </div>
  );
}