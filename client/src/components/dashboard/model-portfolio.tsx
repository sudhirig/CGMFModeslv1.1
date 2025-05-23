import React from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { usePortfolio } from "@/hooks/use-portfolio";
import { Skeleton } from "@/components/ui/skeleton";

export default function ModelPortfolio() {
  const { portfolio, isLoading, error, generatePortfolio } = usePortfolio();
  
  const handleRebalance = () => {
    // Generate a new portfolio with the same risk profile
    if (portfolio?.riskProfile) {
      generatePortfolio(portfolio.riskProfile);
    } else {
      // Default to balanced if no portfolio exists
      generatePortfolio("Balanced");
    }
  };
  
  return (
    <div className="px-4 sm:px-6 lg:px-8 mb-6">
      <Card>
        <CardContent className="p-6">
          <div className="flex justify-between items-center mb-4">
            <h2 className="text-xl font-semibold text-neutral-900">Model Portfolio</h2>
            <div className="flex items-center">
              <Button 
                variant="outline" 
                className="bg-primary-50 text-primary-700 border-primary-300 hover:bg-primary-100"
                onClick={handleRebalance}
                disabled={isLoading}
              >
                <span className="material-icons text-sm mr-1">sync</span>
                <span>Rebalance Portfolio</span>
              </Button>
            </div>
          </div>
          
          {isLoading ? (
            <div className="space-y-6">
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Skeleton className="h-64 w-full" />
                <Skeleton className="h-64 w-full md:col-span-2" />
              </div>
            </div>
          ) : error ? (
            <div className="text-red-500 p-4 rounded-lg bg-red-50">
              Error loading portfolio: {error}
            </div>
          ) : !portfolio ? (
            <div className="text-center p-8 bg-neutral-50 rounded-lg">
              <div className="material-icons text-4xl text-neutral-400 mb-2">account_balance</div>
              <h3 className="text-lg font-medium text-neutral-900">No Portfolio Available</h3>
              <p className="text-sm text-neutral-500 mt-1 mb-4">
                Click the Rebalance button to generate a model portfolio
              </p>
              <Button onClick={handleRebalance}>Generate Portfolio</Button>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div className="md:col-span-1">
                <div className="bg-neutral-50 rounded-lg p-4">
                  <h3 className="text-base font-medium text-neutral-900 mb-4">Recommended Allocation</h3>
                  
                  <div className="space-y-4">
                    {portfolio.assetAllocation?.equityLargeCap > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Equity: Large Cap</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.equityLargeCap}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-primary-500 h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.equityLargeCap}%` }}></div>
                        </div>
                      </div>
                    )}
                    
                    {portfolio.assetAllocation?.equityMidCap > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Equity: Mid Cap</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.equityMidCap}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-info h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.equityMidCap}%` }}></div>
                        </div>
                      </div>
                    )}
                    
                    {portfolio.assetAllocation?.equitySmallCap > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Equity: Small Cap</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.equitySmallCap}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-success h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.equitySmallCap}%` }}></div>
                        </div>
                      </div>
                    )}
                    
                    {portfolio.assetAllocation?.debtShortTerm > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Debt: Short Duration</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.debtShortTerm}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-warning h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.debtShortTerm}%` }}></div>
                        </div>
                      </div>
                    )}
                    
                    {portfolio.assetAllocation?.debtMediumTerm > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Debt: Medium Duration</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.debtMediumTerm}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-danger h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.debtMediumTerm}%` }}></div>
                        </div>
                      </div>
                    )}
                    
                    {portfolio.assetAllocation?.hybrid > 0 && (
                      <div>
                        <div className="flex justify-between text-sm">
                          <span className="font-medium text-neutral-700">Hybrid</span>
                          <span className="text-neutral-900">{portfolio.assetAllocation?.hybrid}%</span>
                        </div>
                        <div className="mt-1 w-full bg-neutral-200 rounded-full h-2">
                          <div className="bg-neutral-500 h-2 rounded-full" style={{ width: `${portfolio.assetAllocation?.hybrid}%` }}></div>
                        </div>
                      </div>
                    )}
                  </div>
                  
                  <div className="mt-4">
                    <div className="text-sm font-medium text-neutral-700 mb-2">Risk Profile</div>
                    <div className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-warning bg-opacity-10 text-warning">
                      {portfolio.riskProfile || "Balanced"}
                    </div>
                  </div>
                  
                  <div className="mt-4">
                    <div className="text-sm font-medium text-neutral-700 mb-2">Expected Returns (Annualized)</div>
                    <div className="text-lg font-semibold text-neutral-900">
                      {portfolio.expectedReturns ? `${portfolio.expectedReturns.min}% - ${portfolio.expectedReturns.max}%` : "10% - 15%"}
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="md:col-span-2">
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
                            {portfolio.fundScores?.[allocation.fund.id] || ((Math.random() * 15 + 75).toFixed(1))}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                
                <div className="mt-4 flex justify-end">
                  <Button>
                    Generate Portfolio Report
                  </Button>
                </div>
              </div>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
