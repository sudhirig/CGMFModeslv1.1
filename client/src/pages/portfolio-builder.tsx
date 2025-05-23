import React, { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { usePortfolio } from "@/hooks/use-portfolio";
import { Skeleton } from "@/components/ui/skeleton";
import { PieChart, Pie, Cell, ResponsiveContainer, Legend, Tooltip } from "recharts";

type RiskProfile = 'Conservative' | 'Moderately Conservative' | 'Balanced' | 'Moderately Aggressive' | 'Aggressive';

export default function PortfolioBuilder() {
  const [selectedRiskProfile, setSelectedRiskProfile] = useState<RiskProfile>("Balanced");
  const { generatePortfolio, portfolio, isLoading, error } = usePortfolio();
  
  const handleGeneratePortfolio = () => {
    generatePortfolio(selectedRiskProfile);
  };
  
  // Colors for pie chart
  const COLORS = ['#2271fa', '#68aff4', '#34D399', '#FBBF24', '#F87171', '#64748b'];
  
  // Prepare allocation data for pie chart
  const getAllocationData = () => {
    if (!portfolio?.assetAllocation) return [];
    
    return [
      { name: 'Large Cap', value: portfolio.assetAllocation.equityLargeCap },
      { name: 'Mid Cap', value: portfolio.assetAllocation.equityMidCap },
      { name: 'Small Cap', value: portfolio.assetAllocation.equitySmallCap },
      { name: 'Debt Short', value: portfolio.assetAllocation.debtShortTerm },
      { name: 'Debt Medium', value: portfolio.assetAllocation.debtMediumTerm },
      { name: 'Hybrid', value: portfolio.assetAllocation.hybrid }
    ].filter(item => item.value > 0);
  };
  
  return (
    <div className="py-6">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-2xl font-bold text-neutral-900">Portfolio Builder</h1>
          <p className="mt-1 text-sm text-neutral-500">
            Create optimized portfolios based on risk profile and market conditions
          </p>
        </div>
        
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="md:col-span-1">
            <Card>
              <CardHeader>
                <CardTitle>Portfolio Parameters</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div>
                    <label className="text-sm font-medium text-neutral-700">Risk Profile</label>
                    <Select value={selectedRiskProfile} onValueChange={(value) => setSelectedRiskProfile(value as RiskProfile)}>
                      <SelectTrigger className="w-full mt-1">
                        <SelectValue placeholder="Select risk profile" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="Conservative">Conservative</SelectItem>
                        <SelectItem value="Moderately Conservative">Moderately Conservative</SelectItem>
                        <SelectItem value="Balanced">Balanced</SelectItem>
                        <SelectItem value="Moderately Aggressive">Moderately Aggressive</SelectItem>
                        <SelectItem value="Aggressive">Aggressive</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  
                  <div className="pt-4">
                    <Button className="w-full" onClick={handleGeneratePortfolio} disabled={isLoading}>
                      {isLoading ? "Generating..." : "Generate Portfolio"}
                    </Button>
                  </div>
                  
                  {error && (
                    <div className="text-sm text-red-500 mt-2">
                      Error: {error}
                    </div>
                  )}
                </div>
                
                {portfolio && (
                  <div className="mt-6 bg-neutral-50 rounded-lg p-4">
                    <h3 className="text-base font-medium text-neutral-900 mb-3">Recommended Allocation</h3>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={getAllocationData()}
                            cx="50%"
                            cy="50%"
                            labelLine={false}
                            label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
                            outerRadius={80}
                            fill="#8884d8"
                            dataKey="value"
                          >
                            {getAllocationData().map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                            ))}
                          </Pie>
                          <Tooltip formatter={(value) => `${value}%`} />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                    
                    <div className="mt-4">
                      <div className="text-sm font-medium text-neutral-700 mb-2">Risk Profile</div>
                      <div className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning bg-opacity-10 text-warning">
                        {portfolio.riskProfile}
                      </div>
                    </div>
                    
                    <div className="mt-4">
                      <div className="text-sm font-medium text-neutral-700 mb-2">Expected Returns (Annualized)</div>
                      <div className="text-lg font-semibold text-neutral-900">
                        {portfolio.expectedReturns ? `${portfolio.expectedReturns.min}% - ${portfolio.expectedReturns.max}%` : "N/A"}
                      </div>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
          
          <div className="md:col-span-2">
            <Card>
              <CardHeader>
                <CardTitle>Model Portfolio</CardTitle>
                {portfolio && <div className="text-sm text-neutral-500">Generated on {new Date().toLocaleString()}</div>}
              </CardHeader>
              <CardContent>
                {isLoading ? (
                  <div className="space-y-4">
                    <Skeleton className="h-12 w-full" />
                    <Skeleton className="h-64 w-full" />
                    <Skeleton className="h-12 w-full" />
                  </div>
                ) : !portfolio ? (
                  <div className="flex items-center justify-center h-64 bg-neutral-50 rounded-lg">
                    <div className="text-center">
                      <div className="material-icons text-4xl text-neutral-400 mb-2">account_balance</div>
                      <h3 className="text-lg font-medium text-neutral-900">No Portfolio Generated</h3>
                      <p className="text-sm text-neutral-500 mt-1">
                        Select a risk profile and generate a portfolio to see recommendations
                      </p>
                    </div>
                  </div>
                ) : (
                  <div>
                    <h3 className="text-base font-medium text-neutral-900 mb-4">Recommended Funds</h3>
                    
                    <div className="bg-neutral-50 rounded-lg overflow-hidden">
                      <table className="min-w-full divide-y divide-neutral-200">
                        <thead>
                          <tr>
                            <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Category</th>
                            <th className="px-4 py-3 bg-neutral-100 text-left text-xs font-medium text-neutral-500 uppercase tracking-wider">Fund Name</th>
                            <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Allocation %</th>
                            <th className="px-4 py-3 bg-neutral-100 text-right text-xs font-medium text-neutral-500 uppercase tracking-wider">Score</th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-neutral-200">
                          {portfolio.allocations?.map((allocation, index) => (
                            <tr key={index}>
                              <td className="px-4 py-3 whitespace-nowrap text-sm text-neutral-900">
                                {allocation.fund.category.split(': ')[1] || allocation.fund.category}
                              </td>
                              <td className="px-4 py-3 whitespace-nowrap text-sm font-medium text-primary-600">
                                {allocation.fund.fundName}
                              </td>
                              <td className="px-4 py-3 whitespace-nowrap text-sm text-right text-neutral-900">
                                {allocation.allocationPercent}%
                              </td>
                              <td className="px-4 py-3 whitespace-nowrap text-sm text-right font-medium text-neutral-900">
                                {portfolio.score}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    
                    <div className="mt-4 flex justify-end">
                      <Button variant="default">
                        Generate Portfolio Report
                      </Button>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </div>
      </div>
    </div>
  );
}
